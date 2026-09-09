"""Environment initialization, widget management, and session utilities."""

import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any


def create_required_widgets(
    dbutils: Any, widgets_config: Mapping[str, tuple[str, str]], required_widgets: list[str]
) -> None:
    """Create Databricks widgets from the master configuration."""
    undefined = set(required_widgets) - set(widgets_config.keys())
    if undefined:
        raise KeyError(f"Requested widgets not found in config: {list(undefined)}")

    for widget in required_widgets:
        default_val, label = widgets_config[widget]
        dbutils.widgets.text(widget, default_val, label)


def fetch_widget_values(dbutils: Any, print_validation: bool = False) -> SimpleNamespace:
    """Return widget values as a SimpleNamespace. Raises error if any value is empty."""
    all_widgets: dict[str, str] = dbutils.widgets.getAll()
    widgets_dict = {k: v.strip() for k, v in all_widgets.items()}

    if print_validation:
        for k, v in widgets_dict.items():
            print(f"{k:<30} : {v if v else 'MISSING VALUE'}")

    missing = [k for k, v in widgets_dict.items() if v == ""]
    if missing:
        raise ValueError(f"Missing values for widgets: {missing}")

    return SimpleNamespace(**widgets_dict)


def initialize_spark_performance(spark: Any, broadcast_mb: int) -> None:
    """Apply optimized Spark and Delta configurations for high performance."""
    configs = {
        # Adaptive Query Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.shuffle.partitions": "1000",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "1000",
        # Cost-Based Optimizer
        "spark.sql.cbo.enabled": "true",
        "spark.sql.cbo.joinReorder.enabled": "true",
        "spark.sql.cbo.joinReorder.maxNumInTables": "12",
        # Broadcast settings
        "spark.sql.autoBroadcastJoinThreshold": str(broadcast_mb * 1024 * 1024),
        "spark.sql.broadcastTimeout": "600",
        # Execution & Caching
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "false",
        "spark.databricks.io.cache.enabled": "true",
        "spark.databricks.io.cache.maxDiskUsagePerNode": "100g",
        # Delta optimizations
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoOptimize.autoCompact": "true",
        "spark.databricks.delta.merge.repartitionBeforeWrite.enabled": "true",
        "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
    }

    for key, value in configs.items():
        spark.conf.set(key, value)


def log(message: str, execution_mode: str = "dev") -> None:
    """Print message only when not running in production."""
    if str(execution_mode).strip().lower() != "prod":
        print(message)


def view(data_payload: Any, execution_mode: str = "dev") -> None:
    """Display data only in non-production mode."""
    if str(execution_mode).strip().lower() == "prod":
        return

    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        try:
            frame = sys._getframe()
            display_hook = None

            while frame:
                if "display" in frame.f_locals:
                    display_hook = frame.f_locals["display"]
                    break
                if "display" in frame.f_globals:
                    display_hook = frame.f_globals["display"]
                    break
                frame = frame.f_back

            if display_hook and callable(display_hook):
                display_hook(data_payload)
                return
        except Exception:
            pass

    if hasattr(data_payload, "show"):
        data_payload.show(truncate=False)
    else:
        print(data_payload)


def install_project_wheel(project_name: str) -> None:
    """Install the latest project wheel from the artifacts folder."""
    if "JOB_ID" in os.environ or os.environ.get("DB_IS_JOB_CLUSTER") == "true":
        return

    current_dir = Path.cwd().resolve()
    project_root = next((p for p in [current_dir, *current_dir.parents] if p.name == project_name), None)

    if not project_root:
        raise FileNotFoundError(f"Could not locate project root '{project_name}' from {current_dir}")

    if ".bundle" in project_root.as_posix():
        artifacts_dir = project_root / "dev_dev" / "artifacts" / ".internal"
    else:
        artifacts_dir = project_root / "artifacts" / ".internal"

    if not artifacts_dir.exists():
        raise FileNotFoundError(f"Artifacts folder not found: {artifacts_dir}")

    wheels = list(artifacts_dir.glob("*.whl"))
    if not wheels:
        raise FileNotFoundError(f"No .whl files found in {artifacts_dir}")

    latest_wheel = max(wheels, key=lambda p: p.stat().st_mtime)

    pip_cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        str(latest_wheel),
        "--force-reinstall",
        "--no-cache-dir",
        "--quiet",
    ]

    try:
        subprocess.check_call(pip_cmd)
        print(f"[WHEEL] Installed: {latest_wheel.name}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to install wheel: {latest_wheel.name}") from e


def install_local_requirements(requirements_filenames: list[str], project_name: str) -> None:
    """Install dependencies from local requirements files."""
    if "JOB_ID" in os.environ or os.environ.get("DB_IS_JOB_CLUSTER") == "true":
        print("[INFO] Running on job cluster. Skipping local requirements installation.")
        return

    if not requirements_filenames:
        raise ValueError("requirements_filenames cannot be empty.")

    current_dir = Path.cwd().resolve()
    project_root = next((p for p in [current_dir, *current_dir.parents] if p.name == project_name), None)

    if not project_root:
        raise FileNotFoundError(f"Could not locate project root '{project_name}' from {current_dir}")

    if ".bundle" in project_root.as_posix():
        requirements_dir = project_root / "dev_dev" / "files" / "requirements"
    else:
        requirements_dir = project_root / "requirements"

    pip_cmd = [sys.executable, "-m", "pip", "install"]

    for filename in requirements_filenames:
        file_path = requirements_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Requirements file not found: {file_path}")
        pip_cmd.extend(["-r", str(file_path)])

    pip_cmd.extend(["--force-reinstall", "--no-cache-dir", "--quiet", "--disable-pip-version-check"])

    try:
        subprocess.check_call(pip_cmd)
        print(f"[SUCCESS] Installed requirements: {', '.join(requirements_filenames)}")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to install requirements. Return code: {e.returncode}")
        raise


def get_orchestration_run_id() -> str:
    """Return the current job run ID or 'local_sandbox_run' if not running in a job."""
    return os.environ.get("DB_RUN_ID") or "local_sandbox_run"
