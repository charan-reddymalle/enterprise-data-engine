"""Utility functions for transforming SAP currency data into standardized dimension and fact tables."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def get_currency_dim(tcurc_raw: DataFrame, tcurt_raw: DataFrame) -> DataFrame:
    """Transform raw SAP currency tables into a standardized Currency Dimension."""
    tcurc_cleaned = tcurc_raw.select(
        F.concat_ws("|", F.col("WAERS"), F.lit("SAPS4HANA")).alias("currency_ck"),
        F.col("WAERS").alias("currency_cd"),
        F.lit("SAPS4HANA").alias("currency_record_source_cd"),
    )

    tcurt_cleaned = tcurt_raw.filter(F.col("SPRAS") == "E").select(
        F.col("WAERS").alias("currency_cd"),
        F.col("LTEXT").alias("currency_name"),
    )

    return tcurc_cleaned.join(F.broadcast(tcurt_cleaned), on="currency_cd", how="left").select(
        "currency_ck",
        "currency_cd",
        "currency_name",
        "currency_record_source_cd",
    )


def get_currency_exchange_rate_fact(
    tcurr_raw: DataFrame,
    exchange_rate_type_filter: F.Column,
    from_currency_filter: F.Column,
    to_currency_filter: F.Column,
) -> DataFrame:
    """Transform raw SAP TCURR exchange rate entries into a standardized Currency Exchange Rate Fact."""
    # Effective date = 99999999 - GDATU (SAP stores inverted dates)
    effective_date_expr = F.to_date((F.lit(99999999) - F.col("GDATU").cast("int")).cast("string"), "yyyyMMdd")

    # Safe division to avoid divide-by-zero on FFACT/TFACT
    exchange_rate_expr = (
        (
            F.when(F.col("TFACT") == 0, F.lit(1)).otherwise(F.col("TFACT"))
            / F.when(F.col("FFACT") == 0, F.lit(1)).otherwise(F.col("FFACT"))
        )
        * F.col("UKURS")
    ).cast("decimal(38, 10)")

    return tcurr_raw.filter(exchange_rate_type_filter & from_currency_filter & to_currency_filter).select(
        F.concat_ws("|", F.col("FCURR"), F.col("TCURR"), effective_date_expr, F.lit("SAPS4HANA")).alias(
            "currency_exchange_rate_ck"
        ),
        F.concat_ws("|", F.col("KURST"), F.lit("SAPS4HANA")).alias("currency_exchange_rate_type_ck"),
        F.concat_ws("|", F.col("FCURR"), F.lit("SAPS4HANA")).alias("exchange_rate_from_currency_ck"),
        F.concat_ws("|", F.col("TCURR"), F.lit("SAPS4HANA")).alias("exchange_rate_to_currency_ck"),
        effective_date_expr.alias("exchange_rate_effective_date"),
        exchange_rate_expr.alias("exchange_rate"),
        F.lit("SAPS4HANA").alias("currency_record_source_cd"),
    )


def get_latest_currency_exchange_rates(currency_exchange_rate_fact_df: DataFrame) -> DataFrame:
    """Return the most recent CAD → USD daily exchange rate."""
    is_daily = F.col("currency_exchange_rate_type_ck") == "M|SAPS4HANA"
    is_usd = F.col("exchange_rate_from_currency_ck") == "USD|SAPS4HANA"
    is_inr = F.col("exchange_rate_to_currency_ck") == "INR|SAPS4HANA"

    filtered = currency_exchange_rate_fact_df.filter(is_daily & is_inr & is_usd)

    # Get the latest effective date
    max_date_df = filtered.select(F.max("exchange_rate_effective_date").alias("max_date"))

    return filtered.join(
        F.broadcast(max_date_df), F.col("exchange_rate_effective_date") == F.col("max_date"), "inner"
    ).select(
        F.col("currency_exchange_rate_ck"),
        F.substring_index(F.col("exchange_rate_from_currency_ck"), "|", 1).alias("from_currency_cd"),
        F.col("exchange_rate_effective_date").alias("exchange_date"),
        F.col("exchange_rate"),
    )
