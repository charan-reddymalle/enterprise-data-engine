"""Business rules utilities for data engineering."""

from datetime import datetime, timedelta
from types import SimpleNamespace

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.currency import get_latest_currency_exchange_rates


def get_snapshot_context(run_date=None) -> SimpleNamespace:
    """Return snapshot context with date and scheduling flags.

    Determines if the given (or current) date is the last day of the week
    (Saturday) or month for snapshot triggering decisions.
    """
    # Parse run_date or default to current datetime
    if run_date is None:
        run_date = datetime.now()

    # Last day of week = Saturday (weekday == 5)
    is_last_day_of_week = run_date.weekday() == 5

    # Last day of month = tomorrow is the 1st
    is_last_day_of_month = (run_date + timedelta(days=1)).day == 1

    return SimpleNamespace(
        snapshot_date=run_date.date(),
        is_last_day_of_week=is_last_day_of_week,
        is_last_day_of_month=is_last_day_of_month,
    )


def complete_sample_fact_projection(
    standardized_sample_df: DataFrame,
    currency_exchange_rate_fact_df: DataFrame,
    sample_dim_df: DataFrame,
    source_cd: str,
) -> DataFrame:
    """Finalize sample fact by joining dimensions and projecting all required columns.

    Parameters
    ----------
    standardized_sample_df : DataFrame
        Cleansed sample with standardized fields.
    currency_exchange_rate_fact_df : DataFrame
        exchange rate fact (used to get latest currency exchange rates).
    sample_dim_df : DataFrame
        Sample dimension with the required info.
    source_cd : str
        Source system code when multiple systems are involved.

    Returns
    -------
    DataFrame
        Complete sample fact ready for Gold layer schemas.
    """
    current_date = F.current_date()
    source_lit = F.lit(source_cd)
    is_source1 = source_cd.lower().strip() == "source1"

    left_key = "l.col1" if is_source1 else "l.col2"
    right_key = "r.col3" if is_source1 else "r.col4"
    sample_expr = F.concat_ws("-", F.col("col5"), F.col("col6")) if is_source1 else F.col("col7")

    latest_rates = get_latest_currency_exchange_rates(currency_exchange_rate_fact_df)

    right_select_cols = [
        F.col("col1"),
        F.col("col2"),
        F.col("col3").alias("col4"),
    ]

    right = (
        sample_dim_df.alias("f")
        .join(F.broadcast(latest_rates), F.col("currency_uom") == F.col("from_currency_cd"), "left")
        .select(right_select_cols)
    )

    # Build join condition
    join_cond = F.col(left_key) == F.col(right_key)

    # Stage 1: Independent calculations + passthroughs for dependent columns
    stage1 = (
        standardized_sample_df.alias("l")
        .join(F.broadcast(right).alias("r"), join_cond, "inner")
        .withColumns(
            {
                # Keys
                "sample_1_ck": F.concat_ws(
                    "|",
                    sample_expr,
                    F.col("r.col1"),
                    F.coalesce(F.col("r.col2"), F.lit("")),
                    F.coalesce(F.col("r.col3"), F.lit("")),
                    source_lit,
                ),
                "sample_2_ck": F.concat_ws("|", sample_expr, source_lit),
                "sample_record_source_cd": source_lit,
                # Temporal
                "sample_age_days_count": F.when(
                    F.col("sample_date").isNotNull(),
                    F.datediff(F.col("sample_date"), current_date),
                )
                .otherwise(F.lit(None))
                .cast("integer"),
                "sample_diff_days_count": F.when(
                    F.col("end_date").isNotNull() & F.col("start_date").isNotNull(),
                    F.datediff(F.col("end_date"), F.col("start_date")),
                )
                .otherwise(F.lit(None))
                .cast("integer"),
                # Passthroughs
                "sample_flag": F.col("r.some_flag"),
                "s1_exchange_rate": F.col("l.exchange_rate").cast("decimal(38, 10)"),
                # Valuation calculations
                "sample_value_local_currency_amt": F.when(
                    F.col("sample_price_lbs_local_currency_amt").isNotNull() & F.col("sample_lbs_qty_amt").isNotNull(),
                    F.col("sample_price_lbs_local_currency_amt") * F.col("sample_lbs_qty_amt"),
                )
                .otherwise(
                    F.when(
                        F.col("qty_uom") == F.col("sample_price_qty_uom"),
                        F.col("sample_qty_amt") * F.col("sample_price_local_currency_amt"),
                    )
                )
                .cast("decimal(38, 10)"),
                "sample_value_qty_uom": F.when(
                    F.col("sample_price_lbs_local_currency_amt").isNotNull() & F.col("sample_lbs_qty_amt").isNotNull(),
                    F.lit("pound"),
                )
                .otherwise(
                    F.when(
                        F.col("qty_uom") == F.col("sample_price_qty_uom"),
                        F.col("sample_price_qty_uom"),
                    )
                )
                .cast("string"),
            }
        )
    )

    # Stage 2: Resolve dependent columns using Stage 1 results.
    sample_fact = stage1.select(
        # Keys (passthrough)
        F.col("sample_1_ck"),
        F.col("sample_2_ck"),
        # Temporal (passthrough)
        F.col("sample_age_days_count"),
        F.col("sample_diff_days_count"),
        # Status & Buckets
        F.when(F.col("sample_age_days_count") < 0, F.lit("<0"))
        .when(F.col("sample_age_days_count") <= 50, F.lit("0-50"))
        .when(F.col("sample_age_days_count") <= 100, F.lit("50-100"))
        .when(F.col("sample_age_days_count") <= 200, F.lit("100-200"))
        .when(F.col("sample_age_days_count") <= 500, F.lit("200-500"))
        .when(F.col("sample_age_days_count") <= 1000, F.lit("500-1000"))
        .otherwise(F.lit(">1000"))
        .alias("sample_age_bucket_num"),
        # auxiliary (passthrough)
        F.col("random1"),
        F.col("random2"),
        # price
        F.col("sample_price_local_currency_amt"),
        (F.col("sample_price_local_currency_amt") * F.nullif(F.col("s1_exchange_rate"), F.lit(0.0)))
        .cast("decimal(38, 10)")
        .alias("sample_price_enterprise_currency_amt"),
        # value
        F.col("sample_value_local_currency_amt"),
        F.col("sample_value_qty_uom"),
        (F.col("sample_value_local_currency_amt") * F.nullif(F.col("s1_exchange_rate"), F.lit(0.0)))
        .cast("decimal(38, 10)")
        .alias("sample_value_enterprise_currency_amt"),
    )

    return sample_fact


def process_sample_snapshot(
    sample_fact_df: DataFrame,
    run_daily_flag: bool,
    run_weekly_flag: bool,
    run_monthly_flag: bool,
    run_date: str | None = None,
) -> tuple[DataFrame, SimpleNamespace]:
    """Generate daily/weekly/monthly sample snapshots based on run flags.

    Creates partition-isolated snapshots only for active grains (daily/weekly/monthly)
    when the corresponding run flag is enabled and the date condition is met.
    """
    # Get snapshot context
    context = get_snapshot_context(run_date=run_date)

    # Core columns + coalesced valuation fields
    sample_fact_data = sample_fact_df.select(
        F.col("col1"),
        F.col("col2"),
        F.col("col3"),
        F.col("col4"),
    )

    # Add snapshot metadata
    initial_snapshot_df = sample_fact_data.withColumns(
        {
            "sample_snapshot_date": F.lit(context.snapshot_date),
            "sample_snapshot_ck": F.concat_ws(
                "|",
                F.substring_index(F.col("col1"), "|", 1),
                F.substring_index(F.col("col3"), "|", 1),
                F.col("col4"),
                F.lit(context.snapshot_date),
            ),
        }
    )

    # Collect active snapshot frames
    snapshot_frames = []

    if run_daily_flag:
        snapshot_frames.append(initial_snapshot_df.withColumn("sample_snapshot_type", F.lit("daily")))

    if run_weekly_flag and context.is_last_day_of_week:
        snapshot_frames.append(initial_snapshot_df.withColumn("sample_snapshot_type", F.lit("weekly")))

    if run_monthly_flag and context.is_last_day_of_month:
        snapshot_frames.append(initial_snapshot_df.withColumn("sample_snapshot_type", F.lit("monthly")))

    # Safeguard: return empty DataFrame with correct schema if nothing is active
    if not snapshot_frames:
        result_df = initial_snapshot_df.limit(0).withColumn("sample_snapshot_type", F.lit(""))
        return result_df, context

    # Union all active snapshot types
    result_df = snapshot_frames[0]
    for frame in snapshot_frames[1:]:
        result_df = result_df.unionByName(frame)

    return result_df, context
