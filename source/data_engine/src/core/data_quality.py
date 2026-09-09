"""Data Quality Framework with metadata-driven validation and reconciliation."""

from collections import defaultdict
from typing import Literal

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window


def build_schema_constraints_df(
    columns_df: DataFrame,
    key_column_usage_df: DataFrame,
    table_constraints_df: DataFrame,
    referential_constraints_df: DataFrame,
    table_name: str,
) -> DataFrame:
    """Build a DataFrame containing all column-level constraints.

    This function performs sequential joins across standard ANSI Information
    Schema tables to extract primary keys, foreign keys, and nullability rules.
    It is compute-agnostic and relies entirely on the DataFrame API.

    Args
    ----
        columns_df: Information schema columns table.
        key_column_usage_df: Information schema key column usage table.
        table_constraints_df: Information schema table constraints table.
        referential_constraints_df: Information schema referential constraints.

    Returns
    -------
        A formatted DataFrame containing the unified constraint metadata.
    """
    # 1. Alias DataFrames for namespace protection
    c = columns_df.alias("c")
    kcu = key_column_usage_df.alias("kcu")
    tc = table_constraints_df.alias("tc")
    rc = referential_constraints_df.alias("rc")
    parent_kcu = key_column_usage_df.alias("parent_kcu")

    # 2. Join columns to key_column_usage
    df = c.join(
        kcu,
        (F.col("c.table_catalog") == F.col("kcu.table_catalog"))
        & (F.col("c.table_schema") == F.col("kcu.table_schema"))
        & (F.col("c.table_name") == F.col("kcu.table_name"))
        & (F.col("c.column_name") == F.col("kcu.column_name")),
        "left",
    )

    # 3. Join to table_constraints
    df = df.join(
        tc,
        (F.col("kcu.constraint_catalog") == F.col("tc.constraint_catalog"))
        & (F.col("kcu.constraint_schema") == F.col("tc.constraint_schema"))
        & (F.col("kcu.constraint_name") == F.col("tc.constraint_name")),
        "left",
    )

    # 4. Join to referential_constraints
    df = df.join(
        rc,
        (F.col("tc.constraint_catalog") == F.col("rc.constraint_catalog"))
        & (F.col("tc.constraint_schema") == F.col("rc.constraint_schema"))
        & (F.col("tc.constraint_name") == F.col("rc.constraint_name")),
        "left",
    )

    # 5. Join back to KCU for parent foreign key mappings
    df = df.join(
        parent_kcu,
        (F.col("rc.unique_constraint_catalog") == F.col("parent_kcu.constraint_catalog"))
        & (F.col("rc.unique_constraint_schema") == F.col("parent_kcu.constraint_schema"))
        & (F.col("rc.unique_constraint_name") == F.col("parent_kcu.constraint_name"))
        & (F.col("kcu.ordinal_position") == F.col("parent_kcu.ordinal_position")),
        "left",
    )

    # 6. Final Select and Order By
    constraints_df = (
        df.select(
            F.col("c.table_name").alias("table_name"),
            F.col("c.column_name").alias("column_name"),
            F.col("c.data_type").alias("data_type"),
            F.col("c.is_nullable").alias("is_nullable"),
            F.col("tc.constraint_type").alias("constraint_type"),
            F.col("tc.constraint_name").alias("constraint_name"),
            F.col("parent_kcu.table_name").alias("parent_table"),
            F.col("parent_kcu.column_name").alias("parent_column"),
        )
        .filter(F.col("table_name") == table_name)
        .orderBy(F.col("c.table_name"), F.col("c.ordinal_position"))
    )

    return constraints_df


class DQValidator:
    """Data Quality Validator with fluent interface.

    Ingests rules from metadata and applies validations.
    Collects errors without dropping rows and splits into clean/quarantine DataFrames.
    """

    def __init__(self, df: DataFrame):
        self.df = df
        if "dq_errors" not in df.columns:
            self.df = self.df.withColumn("dq_errors", F.array().cast("array<string>"))

        self.expected_schema = {}
        self.not_null_cols = set()
        self.pk_cols = set()
        self.unique_rules = defaultdict(list)
        self.fk_rules = []

    def _append_error(self, condition: Column, error_msg: str) -> "DQValidator":
        """Append error message if condition is met."""
        self.df = self.df.withColumn(
            "dq_errors",
            F.when(condition, F.array_union(F.col("dq_errors"), F.array(F.lit(error_msg)))).otherwise(
                F.col("dq_errors")
            ),
        )
        return self

    # Metadata Ingestion
    def ingest_rules(self, constraints_df: DataFrame) -> "DQValidator":
        """Load and parse all constraints for the given table from metadata."""
        rows = constraints_df.collect()

        for row in rows:
            col = row["column_name"]
            c_type = row["constraint_type"]

            self.expected_schema[col] = row["data_type"]

            if row["is_nullable"] == "NO":
                self.not_null_cols.add(col)

            if c_type == "PRIMARY KEY":
                self.pk_cols.add(col)

            if c_type == "UNIQUE":
                self.unique_rules[row["constraint_name"]].append(col)

            if c_type == "FOREIGN KEY":
                self.fk_rules.append(
                    {"child_col": col, "parent_table": row["parent_table"], "parent_col": row["parent_column"]}
                )

        return self

    # Structural & Reconciliation Checks (Hard Fail)
    def assert_no_schema_drift(self) -> "DQValidator":
        """Fail fast if actual schema deviates from ingested metadata."""
        if not self.expected_schema:
            raise ValueError("No expected schema found. Is the table registered?")

        actual = dict(self.df.dtypes)
        for col, expected_type in self.expected_schema.items():
            if col not in actual:
                raise ValueError(f"Missing expected column: {col}")

            actual_base = actual[col].split("(")[0].strip().lower()
            expected_base = expected_type.split("(")[0].strip().lower()
            if expected_base in ("varchar", "char", "text"):
                expected_base = "string"

            if actual_base != expected_base:
                raise ValueError(f"Type mismatch on {col}: expected {expected_base}, got {actual_base}")

        return self

    def assert_distinct_count_parity(
        self, reference_df: DataFrame, current_col: str, reference_col: str
    ) -> "DQValidator":
        """Fail fast if distinct record counts differ between current data and the reference source of truth."""
        if not current_col or not reference_col:
            return self

        current_cnt = self.df.select(current_col).distinct().count()
        reference_cnt = reference_df.select(reference_col).distinct().count()

        if current_cnt != reference_cnt:
            raise ValueError(
                f"Reconciliation failed: Source has {current_cnt} distinct '{current_col}', "
                f"Target has {reference_cnt} distinct '{reference_col}'."
            )
        return self

    # ANSI Constraint Validation (Row-Level)
    def flag_inferred_ansi_violations(self, parent_dfs: dict[str, DataFrame] | None = None) -> "DQValidator":
        """Apply all standard SQL constraints inferred from metadata."""
        parent_dfs = parent_dfs or {}

        # Not Null
        if self.not_null_cols:
            for col in self.not_null_cols:
                self._append_error(F.col(col).isNull(), f"NULL in required column: {col}")

        # Primary Key
        if self.pk_cols:
            for col in self.pk_cols:
                self._append_error(F.col(col).isNull(), f"PK column cannot be null: {col}")

            window = Window.partitionBy(*self.pk_cols)
            self.df = self.df.withColumn("__pk_count", F.count("*").over(window))
            self._append_error(F.col("__pk_count") > 1, f"Duplicate primary key: {list(self.pk_cols)}")
            self.df = self.df.drop("__pk_count")

        # Unique Constraints
        if self.unique_rules:
            for constraint_name, cols in self.unique_rules.items():
                window = Window.partitionBy(*cols)
                self.df = self.df.withColumn("__uniq_count", F.count("*").over(window))

                condition = (F.col("__uniq_count") > 1) & F.coalesce(*[F.col(c) for c in cols]).isNotNull()
                self._append_error(condition, f"Uniqueness violated for {constraint_name}: {cols}")

                self.df = self.df.drop("__uniq_count")

        # Foreign Keys
        if self.fk_rules:
            for fk in self.fk_rules:
                parent_tbl, child_col, parent_col = fk["parent_table"], fk["child_col"], fk["parent_col"]
                if parent_tbl not in parent_dfs:
                    raise ValueError(f"Missing parent table '{parent_tbl}' for foreign key validation.")

                valid_children = self.df.filter(F.col(child_col).isNotNull())
                orphans = valid_children.join(
                    parent_dfs[parent_tbl].select(parent_col).distinct(),
                    valid_children[child_col] == parent_dfs[parent_tbl][parent_col],
                    "left_anti",
                )

                self.df = self.df.join(
                    orphans.select(child_col).distinct().withColumn("__is_orphan", F.lit(True)), [child_col], "left"
                )
                self._append_error(
                    F.col("__is_orphan").isNotNull(), f"FK violation: {child_col} -> {parent_tbl}.{parent_col}"
                )
                self.df = self.df.drop("__is_orphan")

        return self

    # ===================================================================
    # Business Rule Validation
    # ===================================================================
    def flag_business_rule_violations(self, config: dict | None = None) -> "DQValidator":
        """Apply custom business rules from configuration."""
        if not config:
            return self

        # Freshness
        for rule in config.get("freshness", []):
            col, max_hours = rule["column"], rule["max_hours"]
            threshold = F.current_timestamp() - F.expr(f"INTERVAL {max_hours} HOURS")
            self._append_error(F.col(col) < threshold, f"Stale data in {col}")

        # 2. Outliers / Range Check
        for rule in config.get("outliers", []):
            col_name = rule["column"].strip()
            min_v, max_v = rule.get("min"), rule.get("max")

            conditions = []
            bounds_desc = []

            if min_v is not None:
                conditions.append(F.col(col_name) < min_v)
                bounds_desc.append(f"min: {min_v}")

            if max_v is not None:
                conditions.append(F.col(col_name) > max_v)
                bounds_desc.append(f"max: {max_v}")

            if conditions:
                outlier_condition = conditions[0] if len(conditions) == 1 else (conditions[0] | conditions[1])
                bounds_label = ", ".join(bounds_desc)
                self._append_error(
                    outlier_condition & F.col(col_name).isNotNull(),
                    f"Value out of bounds in {col_name} ({bounds_label})",
                )

        # Categories
        for rule in config.get("categories", []):
            col, allowed = rule["column"], rule["allowed_values"]
            condition = ~F.col(col).isin(allowed) & F.col(col).isNotNull()
            self._append_error(condition, f"Invalid value in {col}. Allowed: {allowed}")

        return self

    # Settlement
    def get_clean_and_quarantined_dataframes(self) -> tuple[DataFrame, DataFrame]:
        """Return (clean_df, quarantine_df) based on collected errors."""
        clean = self.df.filter(F.size(F.col("dq_errors")) == 0).drop("dq_errors")
        quarantine = self.df.filter(F.size(F.col("dq_errors")) > 0)
        return clean, quarantine


def data_quality_engine(
    source_df: DataFrame,
    target_table_name: str,
    constraints_df: DataFrame,
    reference_df: DataFrame | None = None,
    parity_source_col: str | None = None,
    parity_reference_col: str | None = None,
    parent_dfs: dict[str, DataFrame] | None = None,
    business_rules: dict | None = None,
    mode: Literal["enforce", "audit"] = "enforce",
) -> tuple[DataFrame, DataFrame] | DataFrame:
    """Execute metadata-driven data quality checks.

    - mode='enforce': Asserts schema & parity, returns (clean_df, quarantine_df).
    - mode='audit': Asserts schema, prints metrics, returns quarantine_df.
    """
    # 1. Common Validation Steps
    validator = DQValidator(source_df).ingest_rules(constraints_df)
    validator.assert_no_schema_drift()

    clean_df, quarantine_df = (
        validator.flag_inferred_ansi_violations(parent_dfs=parent_dfs)
        .flag_business_rule_violations(config=business_rules)
        .get_clean_and_quarantined_dataframes()
    )

    # 2. Return by Mode
    if mode == "enforce":
        if not (reference_df and parity_source_col and parity_reference_col):
            raise ValueError(
                "Parity inputs (reference_df, parity_source_col, parity_reference_col) "
                "are strictly required when mode='enforce'."
            )
        validator.assert_distinct_count_parity(
            reference_df=reference_df,
            current_col=parity_source_col,
            reference_col=parity_reference_col,
        )
        print(
            f"Validation complete for {target_table_name}: "
            f"{clean_df.count()} clean, {quarantine_df.count()} quarantined."
        )
        return clean_df, quarantine_df

    if mode == "audit":
        if source_df.head(1) == []:
            raise ValueError(f"The DataFrame for '{target_table_name}' is empty.")

        total_rows = source_df.count()
        failed_rows = quarantine_df.count()
        health_score = ((total_rows - failed_rows) / total_rows) * 100 if total_rows > 0 else 0
        print(
            f"Audit complete for {target_table_name} | "
            f"Total: {total_rows:,} | Violations: {failed_rows:,} | "
            f"Health: {health_score:.2f}%"
        )
        return quarantine_df

    raise ValueError(f"Invalid mode: '{mode}'. Must be 'enforce' or 'audit'.")
