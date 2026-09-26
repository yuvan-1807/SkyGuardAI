# SkyGuard AI — Final Ready-to-Run Build

This package is rebuilt from the original consolidated baseline and includes:

- Windows-safe SQLite connection cleanup for temporary test databases.
- Stable live edge heartbeat handling.
- Exact-observation Weather Events context loading.
- Live Edge anomaly state included in dashboard station state and map markers.
- Faster SHAP model reuse during live simulation.
- Automatic local `.venv` creation and dependency installation from the supplied batch launchers.
- Correct Phase 3J exit reporting.

## First run

You do **not** need to copy files into another SkyGuard folder.

1. Extract this ZIP anywhere.
2. Open the extracted folder.
3. Double-click `START_SKYGUARD.bat`.
4. On first run it creates `.venv` and installs `requirements.txt` automatically.
5. Open `http://127.0.0.1:8050/` if it does not open automatically.
6. Start Live Network from the dashboard.

## Regression

After the dashboard is verified, run `RUN_PHASE3J.bat` from this same folder.

The `.venv` is created locally on the Windows machine because Python virtual environments contain platform-specific executables and cannot be safely bundled from a Linux build environment.


## Recording / Showcase mode

Use the dashboard **Start Live Network** button for a controlled 10-station showcase. While the simulator is running, the operational UI presents all configured demo stations as LIVE so the recording does not flicker because of timing jitter. Weather Events has a dedicated recent-weather feed so its page remains populated whenever the command-center weather KPI is non-zero.
