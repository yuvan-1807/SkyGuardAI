# SkyGuard AI — Final Consolidated Solution (Prototype Website)

## Purpose
This package is the consolidated operational prototype for the current SkyGuard AI website. It keeps the existing command-center layout, the light-green visual system, station-aware telemetry, the India AWS map, anomaly/weather views, sensor health, data-quality lineage, security status, and the live 10-station simulator.

The master blueprint explicitly calls for Edge checks, ExtraTrees regression, Magnus–Tetens thermodynamic validation, spatial context, SHAP explainability, trusted-data quarantine, secure identity/replay controls, outage recovery, timestamp ordering and predictive maintenance concepts. fileciteturn13file0L2-L7

## Implemented in the current prototype

**Detection and intelligence**
- Edge AI hard checks: invalid/range values, spikes/drops, frozen/stuck behaviour and local blocking.
- Soft-backend temporal analysis.
- Multivariate deviation analysis.
- Station-specific ExtraTrees evidence: expected value, residual, normalized residual and confidence.
- Magnus–Tetens dew-point validation and coordinated weather-transition detection.
- Spatial consistency evidence using trusted nearby observations and distance weighting.
- Decision fusion producing `NORMAL`, `GENUINE_WEATHER_EVENT`, or `SENSOR_ANOMALY`.
- SHAP-based read-only anomaly explanation path.

**Trust, security and resilience**
- HMAC-SHA256 envelope authentication.
- Per-device derived keys in the prototype and device registry with station binding.
- ACTIVE/REVOKED/RESTORED lifecycle.
- Replay protection with monotonic device sequence enforcement.
- Exact duplicate idempotency before sequence authorization.
- Local store-and-forward outbox with ACK deletion.
- `event_time` and `receive_time` preserved separately.
- Out-of-order/backfill flagging based on event time.
- Quality gate, quarantine and durable lineage.
- `validated_observations` view excludes suspicious/imputed/untrusted data from trusted training input.

**Operations and website**
- 10-station simulator.
- Secure live and Offline → Synced event visibility.
- Station map and station inspector.
- Live monitoring, anomaly alerts, weather events, station list, health, data quality and system/security views.
- Larger typography throughout, including SHAP explanation windows and evidence rows.
- Light-green operational palette.
- One dashboard entry point: `START_SKYGUARD.bat`.
- One regression entry point: `RUN_PHASE3J.bat`.

## What is *not* literally implemented in this Python prototype

The master blueprint also describes production/advanced targets such as ESP32-S3 dual-core FreeRTOS deployment, FastAPI/Gunicorn, PostgreSQL/TimescaleDB, Redis, hardware-protected device secrets, an ST-GCN spatial mesh, a physics-informed autoencoder, and a BRITS/NWP self-healing loop. The blueprint presents those as part of the target architecture; the current website prototype remains Python/Flask/SQLite with a PC-based edge emulator, as also stated by the existing package documentation. fileciteturn13file0L113-L139 fileciteturn13file0L163-L174

Therefore the demo should present those items as **production target architecture**, not claim that the current Flask/SQLite prototype is already a hardware-backed ESP32/TimescaleDB/ST-GCN/BRITS deployment.

## Operational data flow

`AWS/Simulator → Edge AI → HMAC + Sequence → Secure Ingest → Event-Time Ordering → Quality Gate → Temporal + Multivariate + ExtraTrees + Weather Physics + Spatial Evidence → Decision Fusion → NORMAL / WEATHER EVENT / SENSOR ANOMALY → SHAP/Health Evidence → VALIDATED or QUARANTINED → Trusted Training Source`

The underlying blueprint describes the same core separation between sensor-fault quarantine and physically validated weather events. fileciteturn13file0L152-L166

## Run order

1. Extract the package.
2. Run `INSTALL_DEPENDENCIES.bat` once to create the isolated `.venv`, then the launchers reuse the same Python environment. `CHECK_ENVIRONMENT.py` is available for diagnosis.
3. Start the website with `START_SKYGUARD.bat`.
4. Open `http://127.0.0.1:8050/`.
5. Use **Start Live Network** inside the dashboard for the secure 10-station simulator.
6. Run `RUN_PHASE3J.bat` separately to execute the complete regression + failure matrix. Output is also saved to `PHASE3J_RESULTS.txt`.
