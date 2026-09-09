"""Functions for encryption metadata."""

import concurrent.futures
import datetime
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import requests
import yaml
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import (
    ChainedTokenCredential,
    ClientSecretCredential,
    DeviceCodeCredential,
    InteractiveBrowserCredential,
)
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


def get_purview_token(
    tenant_id: str | None = None, client_id: str | None = None, client_secret: str | None = None
) -> str:
    """
    Get an OAuth token for Azure Purview Data Plane.

    - Uses Service Principal if credentials are provided.
    - Falls back to interactive authentication (Browser → Device Code) otherwise.
    """
    scope = "https://purview.azure.net/.default"

    # Try Service Principal authentication (preferred for automation)
    if tenant_id and client_id and client_secret:
        try:
            credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
            return credential.get_token(scope).token
        except ClientAuthenticationError as e:
            print(f"[AUTH] Service Principal authentication failed: {e}. Falling back to interactive...")

    # Fallback to interactive authentication
    try:
        credential = ChainedTokenCredential(
            InteractiveBrowserCredential(),
            DeviceCodeCredential(client_id="04b07795-8ddb-461a-bbee-02f9e1bf7b46"),  # Microsoft public client
        )
        return credential.get_token(scope).token
    except ClientAuthenticationError as e:
        raise RuntimeError(f"Purview authentication failed: {e}") from e


def dict_to_namespace(d: dict[str, Any]) -> SimpleNamespace:
    """Recursively convert a dictionary into a SimpleNamespace for dot-notation access."""
    simple_name_space = SimpleNamespace(
        **{k: (dict_to_namespace(v) if isinstance(v, dict) else v) for k, v in d.items()}
    )
    return simple_name_space


def resolve_encryption_config(config_path: str, env: str) -> SimpleNamespace:
    """Load and merge environment-specific encryption configuration from YAML.

    Returns a flat SimpleNamespace with dot-notation access.
    """
    path_obj = Path(config_path).resolve()

    if not path_obj.exists():
        raise FileNotFoundError(f"Configuration file not found: {path_obj}")

    with path_obj.open("r", encoding="utf-8") as f:
        full_config = yaml.safe_load(f)

    default_config = full_config.get("default", {})
    env_config = full_config.get(env.lower(), {})

    # Environment-specific settings override defaults
    merged_config = {**default_config, **env_config}

    resolved_config = dict_to_namespace(merged_config)

    return resolved_config


class PurviewDataMapClient:
    """Client for querying classified metadata from Azure Purview Data Map."""

    class RateLimitError(Exception):
        """Raised when Purview returns HTTP 429 (rate limit)."""

        pass

    def __init__(self, spark: SparkSession, account_name: str, token: str):
        self.spark = spark
        self.base_url = f"https://{account_name}.purview.azure.com"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    @retry(
        retry=retry_if_exception_type(RateLimitError),
        wait=wait_exponential(multiplier=2, min=2, max=32),
        stop=stop_after_attempt(6),
    )
    def _execute_search_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute search query with rate-limit retry."""
        url = f"{self.base_url}/datamap/api/search/query?api-version=2023-09-01"
        response = self.session.post(url, json=payload)

        if response.status_code == 429:
            raise self.RateLimitError("Upstream Purview rate limit saturated. Retrying...")

        response.raise_for_status()
        return response.json()

    def fetch_classified_metadata(self, catalog_filter: str, last_update_time: int) -> DataFrame:
        """Fetch classified column metadata and return as Spark DataFrame."""
        classified_metadata_schema = StructType(
            [
                StructField("col_catalog", StringType(), True),
                StructField("col_schema", StringType(), True),
                StructField("col_table", StringType(), True),
                StructField("col_name", StringType(), True),
                StructField("guid", StringType(), True),
                StructField("dataType", StringType(), True),
                StructField("createTime", LongType(), True),
                StructField("updateTime", LongType(), True),
                StructField("classifications", ArrayType(StringType()), True),
            ]
        )

        payload = {
            "keywords": catalog_filter,
            "limit": 1000,
            "filter": {"and": [{"assetType": "Databricks UC Column"}]},
            "attributes": ["qualifiedName", "createTime", "updateTime", "classifications", "type"],
        }

        raw_assets = []
        while True:
            data = self._execute_search_query(payload)
            for item in data.get("value", []):
                if item.get("updateTime", 0) <= last_update_time:
                    continue
                q_name = str(item.get("qualifiedName", ""))
                if catalog_filter.lower() not in q_name.lower():
                    continue

                cat_match = re.search(r"catalogs/([^/]+)", q_name, re.IGNORECASE)
                sch_match = re.search(r"schemas/([^/]+)", q_name, re.IGNORECASE)
                tbl_match = re.search(r"tables/([^/]+)", q_name, re.IGNORECASE)
                col_match = re.search(r"columns/([^/]+)", q_name, re.IGNORECASE)
                classifications = [str(c.get("typeName")) for c in item.get("classifications", [])]

                raw_assets.append(
                    (
                        cat_match.group(1).strip().lower() if cat_match else "",
                        sch_match.group(1).strip().lower() if sch_match else "",
                        tbl_match.group(1).strip().lower() if tbl_match else "",
                        col_match.group(1).strip().lower() if col_match else "",
                        str(item.get("id", "")),
                        str(item.get("type", "")),
                        int(item.get("createTime", 0)),
                        int(item.get("updateTime", 0)),
                        classifications,
                    )
                )
            # Pagination
            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break
            payload["continuationToken"] = continuation_token

        if not raw_assets:
            classified_metadata_df = self.spark.createDataFrame([], schema=classified_metadata_schema)

        classified_metadata_df = self.spark.createDataFrame(raw_assets, schema=classified_metadata_schema)

        return classified_metadata_df


class DatabricksWarehouseClient:
    """Client for executing SQL statements and querying Unity Catalog metadata on a specific warehouse."""

    def __init__(self, spark: SparkSession, warehouse_name: str):
        self.spark = spark
        self.w = WorkspaceClient()

        warehouse_id = next(
            (wh.id for wh in self.w.warehouses.list() if wh.name == warehouse_name),
            None,
        )

        if not warehouse_id:
            raise ValueError(f"Could not resolve warehouse '{warehouse_name}' on the active workspace.")

        self.warehouse_id = warehouse_id

        # Extract workspace host for building history URLs
        raw_host = self.w.config.host
        self.workspace_host = raw_host.replace("https://", "").strip("/") if raw_host else ""

    def execute_statement(self, sql_statement: str, wait_timeout: str = "30s") -> DataFrame:
        """Execute a SQL statement on the target warehouse and return execution metadata."""
        statement_schema = StructType(
            [
                StructField("statement", StringType(), False),
                StructField("statement_id", StringType(), True),
                StructField("status", StringType(), False),
                StructField("error_message", StringType(), True),
                StructField("history_url", StringType(), True),
            ]
        )
        try:
            response = self.w.statement_execution.execute_statement(
                statement=sql_statement,
                warehouse_id=self.warehouse_id,
                wait_timeout=wait_timeout,
            )

            # Extract status information
            statement_id = response.statement_id
            status_block = response.status
            state_enum = status_block.state if status_block else None
            state_string = state_enum.value if state_enum else "UNKNOWN"

            error_block = status_block.error if status_block else None
            error_message = error_block.message if error_block else None

            # Build history URL
            history_url = None
            if self.workspace_host and statement_id:
                history_url = f"https://{self.workspace_host}/sql/history?queryIdFilter={statement_id}"

            statement_rows = [
                (
                    sql_statement,
                    statement_id,
                    state_string,
                    error_message,
                    history_url,
                )
            ]

        except Exception as e:
            statement_rows = [(sql_statement, None, "FAILED", str(e), None)]

        statement_df = self.spark.createDataFrame(statement_rows, schema=statement_schema)

        return statement_df

    def discover_applied_masks(self, full_table_names: list[str], num_parallel_api_threads: int) -> DataFrame:
        """Fetch column-level masks from Unity Catalog for the given tables."""
        masks_schema = StructType(
            [
                StructField("catalog", StringType(), False),
                StructField("schema", StringType(), False),
                StructField("table", StringType(), False),
                StructField("column", StringType(), False),
                StructField("actual_mask_name", StringType(), False),
            ]
        )

        def _fetch_column_masks_for_table(fqn: str) -> list[tuple[str, str, str, str, str]]:
            """Fetch masks for a single table."""
            table_tuples = []
            try:
                table_info = self.w.tables.get(full_name=fqn)
                if table_info.columns:
                    for c in table_info.columns:
                        if c.mask and c.mask.function_name:
                            parts = fqn.split(".")
                            mask_tuple = (
                                parts[0].lower(),
                                parts[1].lower(),
                                parts[2].lower(),
                                str(c.name).lower(),
                                str(c.mask.function_name),
                            )
                            table_tuples.append(mask_tuple)
                return table_tuples
            except Exception:
                return []

        masks_accumulator = []

        # Parallel fetch to reduce latency
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallel_api_threads) as executor:
            for result in executor.map(_fetch_column_masks_for_table, full_table_names):
                masks_accumulator.extend(result)

        masks_df = self.spark.createDataFrame(masks_accumulator, schema=masks_schema)

        return masks_df


def generate_masking_execution_ledger(
    spark: SparkSession,
    purview_client: PurviewDataMapClient,
    warehouse_client: DatabricksWarehouseClient,
    policy_exemptions_df: DataFrame,
    historical_ledger_df: DataFrame,
    catalog_filter: str,
    config: SimpleNamespace,
    secure_functions_schema: str,
) -> DataFrame:
    """Build masking execution ledger by reconciling physical schema, Purview classifications, and applied masks."""

    def norm(c: str):
        return F.lower(F.trim(F.col(c)))

    # Load configuration
    target_tags = [str(t).lower().strip() for t in config.target_classifications]
    table_exclusions = [str(p).lower().strip() for p in config.table_exclude_patterns]
    table_type_inclusions = [str(t).upper().strip() for t in config.tabletype_include_list]
    data_type_exclusions = [str(dt).lower().strip() for dt in config.datatype_exclude_list]
    super_groups_string = "||".join([str(g).lower().strip() for g in config.privileged_security_groups])
    num_parallel_api_threads = int(config.num_parallel_api_threads)

    # Physical schema baseline
    tables_staged = spark.table("system.information_schema.tables").filter(
        (norm("table_catalog").like(f"%{catalog_filter.lower()}%"))
        & (norm("table_type").isin(table_type_inclusions))
        & (norm("table_schema") != secure_functions_schema.lower())
        & (~norm("table_schema").like("%segment_classification_na%"))
    )

    for pattern in table_exclusions:
        tables_staged = tables_staged.filter(~norm("table_name").like(f"%{pattern}%"))

    columns_staged = spark.table("system.information_schema.columns").filter(
        (norm("table_catalog").like(f"%{catalog_filter.lower()}%")) & (~norm("data_type").isin(data_type_exclusions))
    )

    db_baseline = columns_staged.join(
        tables_staged, on=["table_catalog", "table_schema", "table_name"], how="inner"
    ).select(
        norm("table_catalog").alias("cat"),
        norm("table_schema").alias("sch"),
        norm("table_name").alias("tbl"),
        norm("column_name").alias("col"),
        F.col("data_type").alias("datatype"),
        F.col("table_type"),
    )

    # Live state from Purview and Unity Catalog
    distinct_tables = db_baseline.select("cat", "sch", "tbl").distinct().collect()
    fqns_list = [f"{r['cat']}.{r['sch']}.{r['tbl']}" for r in distinct_tables]

    last_update_time = 0
    if "reconciliation_status" in historical_ledger_df.columns:
        watermark_row = (
            historical_ledger_df.filter(F.col("reconciliation_status") == "SUCCESS")
            .agg(F.coalesce(F.max("source_updatetime"), F.lit(0)).alias("ts"))
            .collect()
        )
        if watermark_row and watermark_row[0]["ts"] is not None:
            last_update_time = int(watermark_row[0]["ts"])

    gov_intent = (
        purview_client.fetch_classified_metadata(catalog_filter, last_update_time)
        .withColumn(
            "active_tags",
            F.array_intersect(
                F.transform(F.col("classifications"), lambda x: F.lower(x)), F.array([F.lit(t) for t in target_tags])
            ),
        )
        .filter(F.size(F.col("active_tags")) > 0)
        .select(
            norm("col_catalog").alias("cat"),
            norm("col_schema").alias("sch"),
            norm("col_table").alias("tbl"),
            norm("col_name").alias("col"),
            F.col("id"),
            F.col("createTime").alias("source_createtime"),
            F.col("updateTime").alias("source_updatetime"),
            F.col("active_tags").alias("active_classifications"),
        )
    )

    uc_masks = warehouse_client.discover_applied_masks(fqns_list, num_parallel_api_threads).select(
        norm("catalog").alias("cat"),
        norm("schema").alias("sch"),
        norm("table").alias("tbl"),
        norm("column").alias("col"),
        F.col("actual_mask_name"),
    )

    # Exemptions & historical ledger
    exemptions_base = policy_exemptions_df.filter(
        F.col("catalog").isNotNull()
        & F.col("schema").isNotNull()
        & F.col("table").isNotNull()
        & F.col("column").isNotNull()
        & norm("catalog").like(f"%{catalog_filter.lower()}%")
    ).select(
        norm("catalog").alias("cat"),
        norm("schema").alias("sch"),
        norm("table").alias("tbl"),
        norm("column").alias("col"),
        norm("exempted_group").alias("raw_exempted_string"),
    )

    exemptions = (
        exemptions_base.groupBy("cat", "sch", "tbl", "col")
        .agg(
            F.concat_ws("||", F.collect_list("raw_exempted_string"), F.lit(super_groups_string)).alias(
                "combined_raw_pool"
            )
        )
        .select(
            "cat",
            "sch",
            "tbl",
            "col",
            F.concat_ws(
                "||", F.array_sort(F.array_remove(F.array_distinct(F.split(F.col("combined_raw_pool"), r"\|\|")), ""))
            ).alias("target_unmasked_groups"),
        )
    )

    ledger_window = Window.partitionBy("cat", "sch", "tbl", "col").orderBy(F.col("processed_time").desc())
    latest_ledger_state = (
        historical_ledger_df.select(
            norm("catalog").alias("cat"),
            norm("schema").alias("sch"),
            norm("table").alias("tbl"),
            norm("column").alias("col"),
            norm("target_unmasked_groups").alias("last_unmasked_groups"),
            F.col("processed_time"),
        )
        .withColumn("row_idx", F.row_number().over(ledger_window))
        .filter(F.col("row_idx") == 1)
        .select("cat", "sch", "tbl", "col", "last_unmasked_groups")
    )

    # Reconciliation
    join_keys = ["cat", "sch", "tbl", "col"]
    reconciled_grid = (
        db_baseline.join(uc_masks, on=join_keys, how="full_outer")
        .join(gov_intent, on=join_keys, how="full_outer")
        .join(exemptions, on=join_keys, how="full_outer")
        .join(latest_ledger_state, on=join_keys, how="full_outer")
    )

    processed_state = reconciled_grid.select(
        F.col("cat").alias("catalog"),
        F.col("sch").alias("schema"),
        F.col("tbl").alias("table"),
        F.col("col").alias("column"),
        F.col("datatype"),
        F.col("table_type"),
        F.col("id"),
        F.coalesce(F.col("source_createtime"), F.lit(0)).alias("source_createtime"),
        F.coalesce(F.col("source_updatetime"), F.lit(0)).alias("source_updatetime"),
        F.coalesce(F.col("active_classifications"), F.array()).alias("active_classifications"),
        F.col("actual_mask_name"),
        F.when(F.col("target_unmasked_groups").isNull(), F.lit(""))
        .otherwise(F.col("target_unmasked_groups"))
        .alias("target_unmasked_groups"),
        F.when(F.col("last_unmasked_groups").isNull(), F.lit(""))
        .otherwise(F.col("last_unmasked_groups"))
        .alias("last_unmasked_groups"),
        F.when(F.col("datatype").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_deleted"),
        F.when(F.col("id").isNull(), F.lit(False)).otherwise(F.lit(True)).alias("is_scanned"),
    )

    action_expr = (
        F.when(F.col("is_deleted"), F.lit("ORPHAN_CLEANUP"))
        .when(
            (~F.col("is_deleted"))
            & (F.size(F.col("active_classifications")) > 0)
            & (F.col("actual_mask_name").isNull() | (F.col("target_unmasked_groups") != F.col("last_unmasked_groups"))),
            F.lit("APPLY"),
        )
        .when(
            (~F.col("is_deleted"))
            & (~F.col("is_scanned"))
            & (F.col("actual_mask_name").isNull() | (F.col("target_unmasked_groups") != F.col("last_unmasked_groups"))),
            F.lit("APPLY"),
        )
        .when(
            (~F.col("is_deleted"))
            & F.col("is_scanned")
            & (F.size(F.col("active_classifications")) == 0)
            & F.col("actual_mask_name").isNotNull(),
            F.lit("REMOVE"),
        )
        .otherwise(F.lit("NONE"))
    )

    ledger_df = processed_state.select(
        "catalog",
        "schema",
        "table",
        "column",
        "datatype",
        "table_type",
        "id",
        "source_createtime",
        "source_updatetime",
        "active_classifications",
        "target_unmasked_groups",
        action_expr.alias("target_action"),
        F.when(action_expr == "APPLY", F.concat(F.lit("mask_"), F.lower(F.col("datatype"))))
        .otherwise(F.col("actual_mask_name"))
        .alias("target_mask_function"),
        F.when(F.col("is_scanned"), F.lit("SCANNED")).otherwise(F.lit("UNSCANNED")).alias("discovery_status"),
        F.lit("PENDING").alias("reconciliation_status"),
    )

    masking_execution_ledger = ledger_df.filter(F.col("target_action") != "NONE")
    return masking_execution_ledger


def register_masking_functions(
    spark: SparkSession,
    secure_functions_catalog: str,
    secure_functions_schema: str,
    vault_scope: str,
    encryption_key: str,
) -> DataFrame:
    """Register masking and encryption functions in Unity Catalog."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {secure_functions_catalog}.{secure_functions_schema}")

    # Type → masking expression mapping
    type_logic = {
        "STRING": (f"base64(aes_encrypt(val, unhex(secret('{vault_scope}', '{encryption_key}')), 'GCM'))"),
        "VARCHAR": "mask(val, 'X', 'x', '9', '0')",
        "CHAR": "mask(val, 'X', 'x', '9', '0')",
        "VARIANT": "mask(val, 'X', 'x', '9', '0')",
        "int": "CAST(-999999999 AS INT)",
        "bigint": "CAST(-999999999 AS BIGINT)",
        "long": "CAST(-999999999 AS LONG)",
        "float": "CAST(-999999999 AS FLOAT)",
        "double": "CAST(-999999999 AS DOUBLE)",
        "decimal": "CAST(-999999999 AS DECIMAL(38,18))",
        "short": "CAST(-99999 AS SHORT)",
        "byte": "CAST(-999 AS BYTE)",
        "date": "CAST('1111-11-11' AS DATE)",
        "timestamp": "CAST('1111-11-11 11:11:11' AS TIMESTAMP)",
        "timestamp_ntz": "CAST('1111-11-11 11:11:11' AS TIMESTAMP_NTZ)",
        "boolean": "CAST(NULL AS BOOLEAN)",
        "binary": "CAST(NULL AS BINARY)",
    }

    audit_rows = []

    for dt, mask_val in type_logic.items():
        func_name = f"mask_{dt.lower()}"
        fqn_func = f"{secure_functions_catalog}.{secure_functions_schema}.{func_name}"

        sql_statement = (
            f"CREATE OR REPLACE FUNCTION {fqn_func}(val {dt}, groups STRING) "
            f"RETURNS {dt} DETERMINISTIC "
            f"RETURN IF(exists(split(groups, '\\\\|\\\\|'), g -> is_account_group_member(g)), val, {mask_val})"
        )

        try:
            spark.sql(sql_statement)
            status = "REGISTERED"
            error_message = None
        except Exception as e:
            status = "FAILED"
            error_message = str(e)

        audit_rows.append((dt, fqn_func, status, error_message))

    audit_schema = StructType(
        [
            StructField("datatype", StringType(), False),
            StructField("function_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True),
        ]
    )

    audit_df = spark.createDataFrame(audit_rows, schema=audit_schema)

    return audit_df


def apply_and_remove_masking_policies(
    spark: SparkSession,
    warehouse_client: DatabricksWarehouseClient,
    masking_execution_ledger_df: DataFrame,
    secure_functions_catalog: str,
    secure_functions_schema: str,
) -> DataFrame:
    """Apply or remove column-level masking policies on target tables."""
    execution_schema = StructType(
        [
            StructField("catalog", StringType(), False),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("column", StringType(), False),
            StructField("datatype", StringType(), True),
            StructField("table_type", StringType(), True),
            StructField("id", StringType(), True),
            StructField("source_createtime", LongType(), True),
            StructField("source_updatetime", LongType(), True),
            StructField("active_classifications", ArrayType(StringType()), True),
            StructField("target_unmasked_groups", StringType(), True),
            StructField("target_action", StringType(), False),
            StructField("target_mask_function", StringType(), True),
            StructField("discovery_status", StringType(), False),
            StructField("reconciliation_status", StringType(), False),
            StructField("statement_id", StringType(), True),
            StructField("statement", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("history_url", StringType(), True),
            StructField("processed_time", TimestampType(), False),
        ]
    )

    ddl_entity_map = {
        "MANAGED": "TABLE",
        "MATERIALIZED_VIEW": "MATERIALIZED VIEW",
        "STREAMING_TABLE": "STREAMING TABLE",
    }

    if masking_execution_ledger_df.isEmpty():
        empty_ledger_df = spark.createDataFrame([], schema=execution_schema)
        return empty_ledger_df

    completed_execution_rows = []

    for row in masking_execution_ledger_df.toLocalIterator():
        cat = str(row["catalog"])
        sch = str(row["schema"])
        tbl = str(row["table"])
        col = str(row["column"])
        action = str(row["target_action"])
        mask_func = str(row["target_mask_function"])
        groups = str(row["target_unmasked_groups"])
        raw_table_type = str(row["table_type"]).upper().strip() if row["table_type"] else ""

        fqn_asset = f"{cat}.{sch}.{tbl}"
        fqn_func = f"{secure_functions_catalog}.{secure_functions_schema}.{mask_func}"
        current_time = datetime.datetime.now(datetime.UTC)

        ddl_entity = ddl_entity_map.get(raw_table_type)

        if action == "APPLY":
            sql_statement = (
                f"ALTER {ddl_entity} {fqn_asset} ALTER COLUMN {col} SET MASK {fqn_func} USING COLUMNS ('{groups}')"
            )
        elif action == "REMOVE":
            sql_statement = f"ALTER {ddl_entity} {fqn_asset} ALTER COLUMN {col} DROP MASK"
        elif action == "ORPHAN_CLEANUP":
            completed_execution_rows.append(
                (
                    cat,
                    sch,
                    tbl,
                    col,
                    row["datatype"],
                    raw_table_type,
                    row["id"],
                    int(row["source_createtime"]) if row["source_createtime"] else 0,
                    int(row["source_updatetime"]) if row["source_updatetime"] else 0,
                    row["active_classifications"],
                    groups,
                    action,
                    mask_func,
                    str(row["discovery_status"]),
                    "SUCCESS",
                    None,
                    "ASSET_NOT_FOUND_METADATA_CLEANUP_ONLY",
                    None,
                    None,
                    current_time,
                )
            )
            continue
        else:
            continue

        try:
            audit_df = warehouse_client.execute_statement(sql_statement)
            audit_row = audit_df.collect()[0]

            status_string = str(audit_row["status"])
            final_status = "SUCCESS" if status_string == "SUCCEEDED" else "FAILED"
            error_message = audit_row["error_message"]
            statement_id = audit_row["statement_id"]
            history_url = audit_row["history_url"]

        except Exception as e:
            final_status = "FAILED"
            error_message = str(e)
            statement_id = None
            history_url = None

        completed_execution_rows.append(
            (
                cat,
                sch,
                tbl,
                col,
                row["datatype"],
                raw_table_type,
                row["id"],
                int(row["source_createtime"]) if row["source_createtime"] else 0,
                int(row["source_updatetime"]) if row["source_updatetime"] else 0,
                row["active_classifications"],
                groups,
                action,
                mask_func,
                str(row["discovery_status"]),
                final_status,
                statement_id,
                sql_statement,
                error_message,
                history_url,
                current_time,
            )
        )

    final_execution_ledger_df = spark.createDataFrame(completed_execution_rows, schema=execution_schema)
    return final_execution_ledger_df
