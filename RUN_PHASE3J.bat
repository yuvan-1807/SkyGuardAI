@echo off
setlocal
cd /d "%~dp0"

echo.
echo ==============================================
echo        SKYGUARD AI - FINAL REGRESSION
echo ==============================================
echo.

if not exist ".venv\Scripts\python.exe" (
  echo No local .venv found. Creating it now...
  where python >nul 2>&1
  if errorlevel 1 (
    echo Python was not found on PATH.
    pause
    exit /b 1
  )
  python -m venv .venv
  if errorlevel 1 (
    echo Failed to create .venv.
    pause
    exit /b 1
  )
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
"%PYEXE%" CHECK_ENVIRONMENT.py
if errorlevel 1 (
  echo.
  echo Tests cannot run because the local .venv is not ready.
  echo Run INSTALL_DEPENDENCIES.bat once, then retry.
  pause
  exit /b 1
)

echo.
"%PYEXE%" -u run_phase3j.py > PHASE3J_RESULTS.txt 2>&1
set "TEST_RC=%ERRORLEVEL%"
type PHASE3J_RESULTS.txt
if not "%TEST_RC%"=="0" (
  echo.
  echo PHASE 3J FAILED. Full output is saved in PHASE3J_RESULTS.txt
  pause
  exit /b %TEST_RC%
)
echo.
echo PHASE 3J PASSED. Full output is saved in PHASE3J_RESULTS.txt
pause
exit /b 0
