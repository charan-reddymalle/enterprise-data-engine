@echo off

:: Rebuild isolated Python environment for local or target (dev/ci/etc)

:: Usage:
::   rebuild          → local
::   rebuild dev      → dev

set "ENV_TARGET=local"
if not "%~1"=="" set "ENV_TARGET=%~1"

set "VENV_DIR=.venv_%ENV_TARGET%"

echo Purging old environment and caches [%VENV_DIR%]...
rmdir /s /q "%VENV_DIR%" .pytest_cache .ruff_cache build dist spark-warehouse metastore_db 2>nul

:: Delete .egg-info folders
for /d /r . %%d in (*.egg-info) do @if exist "%%d" rmdir /s /q "%%d" 2>nul

:: Delete .egg-info files (if any)
for /r . %%f in (*.egg-info) do @if exist "%%f" del /f /q "%%f" 2>nul

:: Delete __pycache__ folders
for /d /r . %%d in (__pycache__) do @if exist "%%d" rmdir /s /q "%%d" 2>nul

del /f /q derby.log 2>nul

echo Creating Python 3.11 virtual environment [%VENV_DIR%]...
py -3.11 -m venv "%VENV_DIR%"
if %ERRORLEVEL% neq 0 goto :FAIL

call "%VENV_DIR%\Scripts\activate.bat"
if %ERRORLEVEL% neq 0 goto :FAIL

echo Bootstrapping core tools...
python -m pip install --upgrade pip setuptools wheel build --quiet
if %ERRORLEVEL% neq 0 goto :FAIL

echo Installing project dependencies for [%ENV_TARGET%]...
python -m pip install -e ".[%ENV_TARGET%]" --quiet
if %ERRORLEVEL% neq 0 goto :FAIL

echo.
python --version
pytest --version

echo.
echo SUCCESS: Environment ready for [%ENV_TARGET%] - %VENV_DIR%
exit /b 0

:FAIL
echo.
echo ERROR: Rebuild failed.
exit /b 1