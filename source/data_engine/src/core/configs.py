"""Configuration module for SCS Data Engineering.

Provides widget configurations, entity metadata, and other utility mappings.
"""

from pyspark.sql import functions as F

# Widget configurations: key -> (default_value, label)
WIDGETS_CONFIG = {
    "execution_mode": ("prod", "Execution Mode"),
    "bronze_catalog": ("bronze_prd01", "Bronze Catalog"),
    "bronze_schema": ("data_engine", "Bronze Schema for data engine"),
    "silver_catalog": ("silver_dev01", "Silver Catalog"),
    "silver_schema": ("data_engine", "Silver Schema for data engine"),
    "gold_catalog": ("gold_dev01", "Gold Catalog"),
    "gold_schema": ("data_engine", "Gold Schema for data engine"),
    "target_entities": ("all", "Comma-separated list of target entities"),
    "target_currency_dim": ("currency_dim", "Target table for currency dimension"),
    "run_daily": ("false", "Whether to run the pipeline daily"),
    "run_weekly": ("true", "Whether to run the pipeline weekly"),
    "run_monthly": ("true", "Whether to run the pipeline monthly"),
}


def get_data_engine_entity_configs():
    """Return configuration for data engine entities."""
    return {
        "dim": {"path": "./../config/schemas/data_engine_dimensions.yml", "priority": 1},
        "fact": {"path": "./../config/schemas/data_engine_facts.yml", "priority": 2},
        "view": {"path": "./../config/schemas/data_engine_views.yml", "priority": 3},
    }


def get_pound_conversion_factor_map():
    """Return a mapping of units of measure to pound conversion factors.

    Returns
    -------
    dict
        A map of unit of measure codes to their conversion factors relative to pounds.
    """
    uom_to_pound_map = {
        "pound": 1.0,
        "kilo": 2.20462,
        "metric ton": 2204.62,
        "ton": 2000.0,  # short ton
        "hundred weight": 100.0,  # short cwt
    }

    # Map Creation
    pound_conversion_factor_map = F.create_map(
        [item for k, v in uom_to_pound_map.items() for item in (F.lit(k), F.lit(v))]
    )
    return pound_conversion_factor_map
