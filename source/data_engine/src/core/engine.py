"""Shared engine utilities for entity DDL processing and Spark operations."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce
from itertools import groupby
from operator import attrgetter

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_entity_ddls(
    entities_config: dict,
    target_entities: str,
    silver_catalog: str,
    silver_schema: str,
    gold_catalog: str,
    gold_schema: str,
) -> list[Row]:
    """Load and format entity DDLs from YAML files for selected target tables."""
    fmt_context = {
        "silver_catalog": str(silver_catalog).strip(),
        "silver_schema": str(silver_schema).strip(),
        "gold_catalog": str(gold_catalog).strip(),
        "gold_schema": str(gold_schema).strip(),
    }

    for key, value in fmt_context.items():
        if not value:
            raise ValueError(f"Parameter '{key}' cannot be empty.")

    if not target_entities or not str(target_entities).strip():
        raise ValueError("'target_entities' cannot be empty.")

    target_clean = str(target_entities).strip().lower()
    is_all_mode = target_clean == "all"

    if not is_all_mode:
        target_list = []
        for part in target_clean.split(","):
            cleaned = part.strip()
            if not cleaned:
                raise ValueError(f"Malformed target_entities: '{target_entities}'")
            target_list.append(cleaned)

        target_set = set(target_list)
        if len(target_list) != len(target_set):
            duplicates = {x for x in target_list if target_list.count(x) > 1}
            raise ValueError(f"Duplicate entities found: {list(duplicates)}")
    else:
        target_set = set()

    entity_rows = []

    for entity_type, config in entities_config.items():
        if "path" not in config or "priority" not in config:
            raise KeyError(f"Entity type '{entity_type}' must contain 'path' and 'priority'.")

        path = config["path"]
        priority = int(config["priority"])

        try:
            with open(path, encoding="utf-8") as f:
                content = yaml.safe_load(f)

            if content is None or not isinstance(content, dict):
                continue

            for entity_name, ddl_script in content.items():
                entity_lower = str(entity_name).strip().lower()

                if is_all_mode or entity_lower in target_set:
                    if not ddl_script or not str(ddl_script).strip():
                        raise ValueError(f"DDL is empty for entity '{entity_name}' in {path}")

                    try:
                        formatted_ddl = ddl_script.strip().format(**fmt_context)
                    except KeyError as err:
                        raise KeyError(
                            f"Missing placeholder '{err}' in entity '{entity_name}'. "
                            f"Allowed placeholders: {list(fmt_context.keys())}"
                        ) from err

                    entity_rows.append(
                        Row(
                            entity=entity_lower,
                            priority=priority,
                            type=str(entity_type).strip().lower(),
                            ddl=formatted_ddl,
                        )
                    )

        except FileNotFoundError as exc:
            raise FileNotFoundError(f"YAML file not found: {path}") from exc
        except yaml.YAMLError as exc:
            raise ValueError(f"YAML syntax error in {path}: {exc}") from exc

    if not is_all_mode:
        # Verify all requested entities were found
        found = {row.entity for row in entity_rows}
        missing = target_set - found
        if missing:
            raise LookupError(f"Target entities not found in any config: {list(missing)}")

    return entity_rows


def deploy_ddl_waves(spark, entity_ddls: list, max_workers: int) -> None:
    """Execute DDLs in priority waves with parallel execution within each wave."""
    if not entity_ddls:
        raise ValueError("entity_ddls cannot be empty.")

    try:
        workers = int(max_workers)
        if workers <= 0:
            raise ValueError()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"'max_workers' must be a positive integer. Got: {max_workers}") from exc

    sorted_entities = sorted(entity_ddls, key=attrgetter("priority"))
    deployed_types = []

    for priority, wave in groupby(sorted_entities, key=attrgetter("priority")):
        wave_items = list(wave)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_row = {}

            for row in wave_items:
                if not hasattr(row, "ddl"):
                    raise AttributeError("Row is missing required 'ddl' attribute.")

                future = executor.submit(spark.sql, row.ddl)
                future_to_row[future] = row

            failed = None
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    future.result()
                    deployed_types.append(row.type)
                except Exception as err:
                    failed = {"entity": row.entity, "error": str(err), "exception": err}
                    break

            if failed:
                print(f"[!] Execution failed in priority wave {priority}.")
                executor.shutdown(wait=True, cancel_futures=True)
                raise RuntimeError(
                    f"DDL deployment failed for entity '{failed['entity']}': {failed['error']}"
                ) from failed["exception"]

    # Summary
    summary = Counter(deployed_types)
    details = ", ".join(f"{count} {t.upper()}" for t, count in summary.items())
    print(f"\nDeployment completed successfully: {details}")


def prepare_source_dataframe(spark, df_name: str, df_cfg: dict) -> DataFrame:
    """Build a source DataFrame from blueprint configuration."""
    df: DataFrame = spark.table(df_cfg["path"])

    # Base column selection
    if df_cfg.get("select"):
        df = df.select(*df_cfg["select"])

    # Row-level filter
    if df_cfg.get("filter") is not None:
        df = df.filter(df_cfg["filter"])

    # Sub-joins with collision protection
    for sj in df_cfg.get("sub_joins") or []:
        sj_df: DataFrame = spark.table(sj["table_path"])

        if sj.get("filter"):
            sj_df = sj_df.filter(sj["filter"])

        if sj.get("add_cols"):
            sj_df = sj_df.withColumns(sj["add_cols"])

        if sj.get("select"):
            req_cols = set(sj["select"]) | set(sj["on"]) | set(sj.get("add_cols", {}).keys())
            sj_df = sj_df.select(list(req_cols))

        # Rename overlapping columns
        sub_join_exprs = []
        for col_name in sj_df.columns:
            if col_name not in sj["on"] and col_name in df.columns:
                sub_join_exprs.append(F.col(col_name).alias(f"sub_join_{col_name}"))
            else:
                sub_join_exprs.append(F.col(col_name))
        sj_df = sj_df.select(sub_join_exprs)

        # Build join condition using simple loop
        cond_list = []
        for lk, rk in zip(sj["left_on"], sj["on"], strict=True):
            cond_list.append(df[lk] == sj_df[rk])
        join_cond = reduce(lambda x, y: x & y, cond_list)

        if sj.get("broadcast") is True:
            sj_df = F.broadcast(sj_df)

        df = df.join(sj_df, join_cond, sj.get("type", "inner"))

        # Drop duplicate join keys
        for lk, rk in zip(sj["left_on"], sj["on"], strict=True):
            if lk == rk:
                df = df.drop(sj_df[rk])

    # Add scalar columns
    if df_cfg.get("add_cols"):
        df = df.withColumns(df_cfg["add_cols"])

    # Group By aggregation
    if df_cfg.get("group_by"):
        gb = df_cfg["group_by"]
        agg_exprs = []
        for alias, expr in gb.get("metrics", {}).items():
            agg_exprs.append(expr.alias(alias))
        df = df.groupBy(*gb.get("keys", [])).agg(*agg_exprs)

    # Deduplication
    if df_cfg.get("dedup"):
        d = df_cfg["dedup"]
        order_by_input = d["order_by"]
        order_exprs = [F.expr(o) for o in (order_by_input if isinstance(order_by_input, list) else [order_by_input])]

        w = Window.partitionBy(*d["partition_by"]).orderBy(*order_exprs)
        df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # Namespace prefixing using simple loop
    namespace_exprs = []
    for c in df.columns:
        namespace_exprs.append(F.col(c).alias(f"{df_name}___{c}"))

    return df.select(namespace_exprs)


def unify_source_dataframes(spark, tables_config: dict, final_projection: list[str] | None = None) -> DataFrame:
    """Unify multiple source DataFrames using namespace-aligned joins."""
    config_items = iter(tables_config.items())

    # Seed with first table
    anchor_name, anchor_cfg = next(config_items)
    final_df = prepare_source_dataframe(spark, anchor_name, anchor_cfg)

    for current_name, current_cfg in config_items:
        current_df = prepare_source_dataframe(spark, current_name, current_cfg)
        join_cfg = current_cfg.get("join", {})

        left_on = join_cfg.get("left_on", [])
        right_on = join_cfg.get("on", [])
        left_tables = join_cfg.get("left_tables", [join_cfg.get("left_table")] * len(left_on))

        if not left_on or not right_on or None in left_tables or len(left_on) != len(right_on):
            raise ValueError(f"Incomplete join configuration for table: '{current_name}'")

        # Build join conditions using simple loop
        cond_list = []
        for tab, left_key, right_key in zip(left_tables, left_on, right_on, strict=True):
            cond_list.append(final_df[f"{tab}___{left_key}"] == current_df[f"{current_name}___{right_key}"])
        join_condition = reduce(lambda x, y: x & y, cond_list)

        if join_cfg.get("broadcast") is True:
            current_df = F.broadcast(current_df)

        final_df = final_df.join(current_df, join_condition, join_cfg.get("type", "inner"))

    # Final projection
    if final_projection:
        exprs = []
        for entry in final_projection:
            if ":" in entry:
                src, alias = entry.split(":", 1)
                exprs.append(F.col(src.strip()).alias(alias.strip()))
            else:
                exprs.append(F.col(entry).alias(entry.split("___")[-1]))
        return final_df.select(exprs)

    return final_df


def traceable_join(df1, df2, join_on, join_type, final_projection=None):
    """Join two DataFrames while preserving source column provenance."""
    left_side, right_side = join_on[0].split(":")
    df1_alias = left_side.split("___")[0].strip()
    df2_alias = right_side.split("___")[0].strip()

    # Alias columns for traceability using simple loops
    df1_columns = []
    for c in df1.columns:
        df1_columns.append(F.col(c).alias(f"{df1_alias}___{c}"))
    df1_final = df1.select(df1_columns)

    df2_columns = []
    for c in df2.columns:
        df2_columns.append(F.col(c).alias(f"{df2_alias}___{c}"))
    df2_final = df2.select(df2_columns)

    # Build join conditions
    join_conds = []
    for cond in join_on:
        l_col, r_col = cond.split(":")
        join_conds.append(F.col(l_col.strip()) == F.col(r_col.strip()))

    final_df = df1_final.join(df2_final, reduce(lambda x, y: x & y, join_conds), join_type)

    if final_projection:
        if final_projection in ("*", ["*"]):
            clean_names = []
            for c in final_df.columns:
                clean_names.append(c.split("___")[-1])

            if len(clean_names) != len(set(clean_names)):
                duplicates = [n for n in set(clean_names) if clean_names.count(n) > 1]
                raise ValueError(f"Ambiguous columns found in '*' mode: {duplicates}")

            exprs = []
            for c in final_df.columns:
                exprs.append(F.col(c).alias(c.split("___")[-1]))
            return final_df.select(exprs)

        exprs = []
        for entry in final_projection:
            if ":" in entry:
                src, alias = entry.split(":", 1)
                exprs.append(F.col(src.strip()).alias(alias.strip()))
            else:
                exprs.append(F.col(entry).alias(entry.split("___")[-1]))
        return final_df.select(exprs)

    return final_df


def scoped_merge_engine(spark, source_df: DataFrame, target: str, merge_key: str, write_mode: str, scope: str) -> None:
    """
    Unified Delta write engine.

    - reload: Full partition refresh
    - append: Safe append into empty partition
    - upsert: Standard row-level merge
    """

    # Load target metadata
    target_delta = DeltaTable.forName(spark, target)
    target_df = spark.table(target)
    target_schema = target_df.schema

    # Schema alignment
    for field in target_schema:
        if field.name not in source_df.columns:
            source_df = source_df.withColumn(field.name, F.lit(None).cast(field.dataType))
    source_df = source_df.select(*target_df.columns)

    # Datatype validation
    for src, tgt in zip(source_df.schema, target_schema, strict=True):
        if src.dataType != tgt.dataType:
            raise TypeError(f"Schema mismatch on '{tgt.name}': source={src.dataType}, target={tgt.dataType}")

    mode = str(write_mode).strip().lower()

    # Idempotency guard for append
    if mode == "append" and spark.table(target).filter(scope).limit(1).count() > 0:
        raise RuntimeError(f"Idempotency violation: Partition '{scope}' already contains data.")

    # Join condition
    if mode in ("reload", "append"):
        join_condition = f"{scope} AND 1 = 0"
    elif mode == "upsert":
        join_condition = f"{scope} AND t.{merge_key} = s.{merge_key}"
    else:
        raise ValueError(f"Invalid write_mode: '{write_mode}'. Use 'reload', 'upsert', or 'append'.")

    merge_builder = target_delta.alias("t").merge(source_df.alias("s"), join_condition)

    if mode == "reload":
        merge_builder.whenNotMatchedInsertAll().whenNotMatchedBySourceDelete(scope).execute()
    elif mode == "append":
        merge_builder.whenNotMatchedInsertAll().execute()
    elif mode == "upsert":
        merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete(scope).execute()

    # Metrics
    metrics = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()[0]["operationMetrics"]
    print(
        f"[{mode.upper()}] {target} | "
        f"Inserted: {metrics.get('numTargetRowsInserted', 0)} | "
        f"Deleted: {metrics.get('numTargetRowsDeleted', 0)} | "
        f"Updated: {metrics.get('numTargetRowsUpdated', 0)}"
    )
