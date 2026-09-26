@echo off
setlocal
cd /d "%~dp0"
echo Starting SkyGuard dashboard...
start "SkyGuard Dashboard" cmd /k "python app_new.py"
timeout /t 3 >nul
echo Starting 10-station live demo...
python live_network_simulator.py --demo --scenario mixed
pause
