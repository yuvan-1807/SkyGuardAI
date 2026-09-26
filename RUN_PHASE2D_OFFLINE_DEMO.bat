@echo off
start "SkyGuard Dashboard" cmd /k python app_new.py
timeout /t 4 >nul
start "SkyGuard Secure Network" cmd /k python live_network_simulator.py --secure --demo --scenario mixed --offline-seconds 15
