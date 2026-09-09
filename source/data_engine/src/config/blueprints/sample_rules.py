"""configuration for sample data."""

from pyspark.sql import functions as F

from core.configs import get_pound_conversion_factor_map


def get_sample_blueprint(catalog: str, schema: str, list_filter1: list, list_filter2: list) -> tuple[dict, list]:
    """Generate the execution configuration mapping for sample datasets.

    Adheres strictly to the framework execution lifecycle order:
    Ingest & Select -> Filter -> Sub-Joins -> Add Scalar Columns -> Dedup.
    """
    blueprint = {
        # 1. DRIVER TABLE (The Anchor)
        "sample_driver": {
            "path": f"{catalog}.{schema}.sample_driver",
            "filter": None,
            "select": [
                "col1",
                "col2",
                "col3",
            ],
        },
        # 2. sample table name
        "sample_table1": {
            "path": f"{catalog}.{schema}.sample_table1",
            "filter": None,
            # Explicit primitive base column selections (including raw columns needed for computations)
            "select": [
                "col1",
                "col2",
                "col3",
                "col4",
            ],
            "add_cols": {
                "col5": F.col("col9").cast("decimal(38,10)"),
                "col6": F.coalesce(F.col("col10"), F.lit("")).cast("string"),
            },
            "join": {
                "left_table": "sample_driver",
                "left_on": ["col1"],
                "on": ["col1"],
                "type": "inner",
            },
        },
        # 3. sample table name
        "sample_table2": {
            "path": f"{catalog}.{schema}.sample_table2",
            "filter": (F.col("col2") == "condition1") & (F.col("col3").isin(list_filter2)),
            "select": [
                "col1",
                "col2",
                "col3",
                "col4",
                "col5",
                "col6",
                "col7",
            ],
            "join": {
                "left_tables": ["sample_driver", "sample_table1"],
                "left_on": ["col1", "col2"],
                "on": ["col5", "col6"],
                "type": "inner",
            },
        },
        # 8. Sample (WITH SUB-JOIN CONFIGURATION)
        "sample_table3": {
            "path": f"{catalog}.{schema}.sample_table3",
            "filter": None,
            "sub_joins": [
                {
                    "table_path": f"{catalog}.{schema}.sample_sub_table",
                    "filter": (F.col("col2") == "condition1") & (F.col("col3").isin(list_filter1)),
                    "select": ["col1", "col2", "col3"],
                    "on": ["col1"],
                    "left_on": ["col1"],
                    "type": "left",
                }
            ],
            "select": [
                "col1",
                "col2",
                "col3",
                "col4",
                "col5",
                "col6",
                "col7",
                "col8",
                "col9",
            ],
            "dedup": {
                "partition_by": ["col1", "col2", "col3", "col4"],
                "order_by": [F.col("col5").desc(), F.col("col6").desc()],
            },
            "add_cols": {
                "col10": F.col("col7").cast("timestamp"),
                "col11": F.col("col8").cast("decimal(38, 10)"),
                "col12": F.date_format(
                    F.col("col9").cast("date"),
                    "yyyy-MM",
                ),
            },
            "join": {
                "left_tables": ["sample_table1", "sample_table2", "sample_table3", "driver_table"],
                "left_on": ["col1", "col2", "col3", "col4"],
                "on": ["col5", "col6", "col7", "col8"],
                "type": "left",
            },
        },
    }

    target_projection = [
        "sample_table3___col1:alias1",
        "sample_driver___col1:alias2",
        "sample_table2___col3",
    ]

    return blueprint, target_projection


def standardize_sample_schema(sample_df):
    """Standardize sample data.

    Parameters
    ----------
    sample_df : DataFrame
        Sample DataFrame with the available columns.

    Returns
    -------
    DataFrame
        DataFrame with added  missing cols.
    """
    pound_factor_map = get_pound_conversion_factor_map()

    sample_qty_uom = F.lower(F.col("qty_uom"))
    price_qty_uom = F.lower(F.col("price_qty_uom"))

    sample_conv_factor = pound_factor_map[sample_qty_uom]
    price_conv_factor = pound_factor_map[price_qty_uom]

    standardized_sample_df = sample_df.withColumns(
        {
            "sample_lbs_qty_amt": F.when(
                sample_conv_factor.isNotNull(), F.col("sample_qty_amt") * sample_conv_factor
            ).cast("decimal(38, 10)"),
            "sample_price_lbs_local_currency_amt": F.when(
                price_conv_factor.isNotNull(), F.col("sample_price_local_currency_amt") / price_conv_factor
            ).cast("decimal(38, 10)"),
            "missing1": F.lit(None).cast("decimal(38, 10)"),
            "missing2": F.lit(None).cast("decimal(38, 10)"),
            "missing3": F.lit(None).cast("string"),
        }
    )

    return standardized_sample_df
