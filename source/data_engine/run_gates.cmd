@echo off

echo Running Quality Gates...
echo.

:: Gate 1: pyproject.toml formatting
echo [1/4] Checking pyproject.toml formatting...
python -m pyproject_fmt pyproject.toml --check || goto :fail

:: Gate 2: Ruff linting
echo [2/4] Running Ruff checks...
python -m ruff check src/core/ || goto :fail

:: Gate 3: Unit tests
echo [3/4] Running unit tests...
python -m pytest || goto :fail

:: Gate 4: Databricks Bundle validation
echo [4/4] Validating Databricks bundle...
call databricks bundle validate || goto :fail

:: All gates passed
echo.
echo All quality gates passed successfully.
exit /b 0

:fail
echo.
echo One or more quality gates failed.
exit /b 1