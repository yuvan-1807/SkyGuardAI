@echo off
setlocal
cd /d "%~dp0"

echo.
echo ==============================================
echo          SKYGUARD AI - READY TO RUN
echo ==============================================
echo.

if not exist ".venv\Scripts\python.exe" (
  echo No local .venv found. Creating it now...
  where python >nul 2>&1
  if errorlevel 1 (
    echo Python was not found on PATH.
    echo Install Python 3.11+ and enable Add Python to PATH.
    pause
    exit /b 1
  )
  python -m venv .venv
  if errorlevel 1 (
    echo Failed to create .venv.
    pause
    exit /b 1
  )
  echo Installing SkyGuard dependencies...
  ".venv\Scripts\python.exe" -m pip install --upgrade pip
  ".venv\Scripts\python.exe" -m pip install -r requirements.txt
  if errorlevel 1 (
    echo Dependency installation failed.
    pause
    exit /b 1
  )
)

set "PYEXE=%~dp0.venv\Scripts\python.exe"
echo Python: %PYEXE%
echo Dashboard: http://127.0.0.1:8050/
echo.
"%PYEXE%" CHECK_ENVIRONMENT.py
if errorlevel 1 (
  echo.
  echo The local .venv is missing or has incompatible packages.
  echo Run INSTALL_DEPENDENCIES.bat once and retry.
  pause
  exit /b 1
)

echo.
echo Starting SkyGuard...
echo.
"%PYEXE%" -u app_new.py
if errorlevel 1 (
  echo.
  echo SkyGuard stopped with an error. The exact interpreter is shown above.
  pause
  exit /b 1
)
pause
