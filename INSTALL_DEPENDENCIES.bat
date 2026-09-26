@echo off
setlocal
cd /d "%~dp0"
echo ==============================================
echo SKYGUARD AI - INSTALL / UPDATE DEPENDENCIES
echo ==============================================
echo.

where python >nul 2>&1
if errorlevel 1 (
  echo Python was not found on PATH.
  echo Install Python 3.11 or newer and enable "Add Python to PATH".
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Creating isolated SkyGuard Python environment...
  python -m venv .venv
  if errorlevel 1 (
    echo Failed to create .venv.
    pause
    exit /b 1
  )
)

set "PYEXE=%~dp0.venv\Scripts\python.exe"
echo.
echo Using: %PYEXE%
"%PYEXE%" -m pip install --upgrade pip
if errorlevel 1 (
  echo.
  echo pip upgrade failed. Continuing to dependency installation...
)

"%PYEXE%" -m pip install -r requirements.txt
if errorlevel 1 (
  echo.
  echo Dependency installation failed. Check internet access and Python setup.
  pause
  exit /b 1
)

"%PYEXE%" CHECK_ENVIRONMENT.py
if errorlevel 1 (
  pause
  exit /b 1
)

echo.
echo Dependencies are ready in the local .venv.
echo START_SKYGUARD.bat and RUN_PHASE3J.bat will automatically use it.
pause
