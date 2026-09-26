# SkyGuard AI — FINAL CONSOLIDATED OPERATIONAL PACKAGE

This is the consolidated prototype for the current SkyGuard AI website. It preserves the existing command-center UI and completes the Phase 2C–3J backend integration, including the final failure/resilience fixes.

## What is included

- Edge AI hard-fault gate and PC-based edge emulator
- Temporal and multivariate anomaly analysis
- ExtraTrees expected-value / residual evidence
- Magnus–Tetens weather-context validation and genuine-weather classification
- Spatial consistency evidence
- Multi-evidence decision fusion
- Sensor health, degradation and maintenance advisory
- Read-only self-healing/imputation advisor
- SHAP explainability on the anomaly inspection path
- Quality gate, quarantine and durable observation lineage
- Trusted `validated_observations` training source
- HMAC-SHA256 authenticated envelopes
- Device registry, station binding, revoke/restore lifecycle
- Monotonic sequence / replay protection and duplicate idempotency
- Local outbox, ACK-based delivery and Offline → Synced handling
- Separate `event_time` and `receive_time`
- Event-time ordering / out-of-order backfill handling
- 10-station secure live simulator
- Operational APIs and readable data-quality/security views
- Light-green UI with larger typography, especially the SHAP explanation window

## Important scope note

The current website is a Python/Flask/SQLite prototype with a PC edge emulator. ESP32-S3/FreeRTOS hardware, FastAPI/Gunicorn, PostgreSQL/TimescaleDB, Redis, hardware-backed device keys, ST-GCN, a physics-informed autoencoder and a BRITS/NWP production imputation loop are documented production targets, not silently represented as already deployed in this prototype.

## Run the website

1. Extract the ZIP.
2. Run `INSTALL_DEPENDENCIES.bat` once to create the isolated `.venv`, then the launchers reuse the same Python environment. `CHECK_ENVIRONMENT.py` is available for diagnosis.
3. Run `START_SKYGUARD.bat`.
4. Open `http://127.0.0.1:8050/`.
5. Press **Start Live Network** in the dashboard to run the secure 10-station simulator.

## Run the final regression

Run `RUN_PHASE3J.bat`.

The BAT uses the same selected Python interpreter as the dashboard launcher, performs an environment preflight, isolates test runtime environment variables, runs the full deterministic regression list plus the Phase 3J failure matrix, and saves the complete output to `PHASE3J_RESULTS.txt`.

A successful run ends with:

`PHASE 3J REGRESSION SUITE: PASS`

## Manual Python commands

```powershell
python CHECK_ENVIRONMENT.py
python app_new.py
python run_phase3j.py
python test_phase3j_failure_matrix.py
python test_shap.py
```

## Map note

The India map uses Leaflet + OpenStreetMap tiles, so the basemap requires network access. The station telemetry and backend remain local.
