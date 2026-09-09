"""PyTest fixtures for Spark and Databricks mocking."""

import os
import platform
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark():
    """Create a stable local SparkSession for testing (Windows + Linux compatible)."""
    is_windows = platform.system() == "Windows"

    # Java & Hadoop configuration
    if is_windows:
        repo_root = Path(r"D:\repos")
        os.environ["JAVA_HOME"] = str(repo_root / "jdk17")
        os.environ["HADOOP_HOME"] = str(repo_root / "hadoop")
        os.environ["PATH"] = os.pathsep.join(
            [str(repo_root / "jdk17" / "bin"), str(repo_root / "hadoop" / "bin"), os.environ.get("PATH", "")]
        )
    else:
        if not os.environ.get("JAVA_HOME"):
            for java_path in ["/usr/lib/jvm/java-17-openjdk-amd64", "/usr/lib/jvm/default-java"]:
                if Path(java_path).exists():
                    os.environ["JAVA_HOME"] = java_path
                    break

    # Pin Python interpreter for Spark worker stability
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

    # Spark session configuration
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("data-engine-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.python.worker.reuse", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark_session = None
    try:
        spark_session = builder.getOrCreate()
        spark_session.sparkContext.setLogLevel("WARN")
        yield spark_session
    except Exception as e:
        pytest.fail(f"Failed to create SparkSession: {e}")
    finally:
        if spark_session:
            spark_session.stop()


@pytest.fixture
def test_dataframe(spark):
    """Return a standardized test DataFrame with common data types."""
    schema = T.StructType(
        [
            T.StructField("ColKey", T.LongType(), False),
            T.StructField("ColString", T.StringType(), True),
            T.StructField("ColNum", T.DecimalType(38, 8), True),
            T.StructField("ColDate", T.DateType(), True),
            T.StructField("ColTs", T.TimestampType(), True),
        ]
    )

    data = [
        (10000, "Aaaaa Bbbb", Decimal("1.10000000"), datetime(2024, 1, 1).date(), datetime(2024, 1, 1, 13, 0, 1)),
        (10000, "Aaaab", Decimal("1.20000000"), datetime(2024, 1, 1).date(), datetime(2024, 1, 1, 14, 0, 1)),
        (20000, "Bbbbb", Decimal("2.10000000"), datetime(2024, 1, 2).date(), datetime(2024, 2, 1, 13, 0, 1)),
    ]

    return spark.createDataFrame(data, schema)


class MockWidgets:
    """In-memory mock for Databricks widgets."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def text(self, name: str, default_value: str, label: str = "") -> None:
        """Mock for `dbutils.widgets.text`."""
        if name not in self._store:
            self._store[name] = default_value

    def get_all(self) -> dict[str, str]:
        """Return all widget values as a dictionary."""
        return self._store


class MockDbutils:
    """Mock for Databricks `dbutils` used in tests."""

    def __init__(self) -> None:
        self.widgets = MockWidgets()
        self.notebook = SimpleNamespace(
            getContext=lambda: SimpleNamespace(
                tags=lambda: {"user": "local.tester@adm.com"},
                notebookPath=lambda: SimpleNamespace(
                    getOrElse=lambda default: "/Workspace/Users/local.tester@adm.com/test_run"
                ),
            )
        )


@pytest.fixture(scope="function")
def dbutils() -> MockDbutils:
    """Provide a fresh mock `dbutils` instance for each test."""
    return MockDbutils()
