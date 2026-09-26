@echo off
title SkyGuard - Weather Event Injector

cd /d "%~dp0"

echo.
echo ==========================================
echo      SKYGUARD WEATHER EVENT INJECTOR
echo ==========================================
echo.

python INJECT_WEATHER_EVENT.py

pause