# SkyGuard Phase 2C — Sensor Health, Degradation, Maintenance & Self-Healing

## Scope
Phase 2C is additive. It reads telemetry and edge-event history and adds four capabilities:

1. Explainable sensor-health scoring for all 10 AWS stations.
2. Short-window degradation trend and risk assessment.
3. Maintenance / calibration recommendations based on observed evidence.
4. Read-only self-healing suggestions using trusted station history plus nearby-station IDW.

## Data safety
The following proven live path remains frozen and is not modified:

`Edge emulator / live network -> /api/ingest -> anomaly pipeline -> sensor_readings`

Phase 2C never overwrites raw observations. Self-healing returns a proposed value only.

## Health evidence
The health score combines bounded penalties for:
- backend anomaly rate in the recent window,
- hard edge events in the last 24 hours,
- distribution drift between the two halves of the recent history,
- frozen-value patterns,
- long communication gaps,
- invalid/out-of-range values.

Parameter health is computed separately for temperature, pressure and humidity. A current score is compared with the immediately preceding window to label the trajectory `IMPROVING`, `STABLE`, `WATCH`, or `DEGRADING`.

The system reports **degradation risk** rather than claiming an exact failure date when the data does not support one.

## Maintenance recommendation
Recommendations are evidence-based and deliberately operational:
- `ROUTINE`
- `CALIBRATION WATCH`
- `INSPECTION`
- `URGENT`

## Self-healing
For a selected anomaly, the preview engine estimates a replacement candidate for each requested parameter using:

- trusted temporal median from the same station,
- spatial inverse-distance weighting (IDW) from up to three nearby stations,
- a combined estimate when both sources are available.

The response records the method, reference counts and confidence. The raw observation remains preserved.

## Dashboard integration
The Health & Status tab polls `/api/phase2c/health` and shows network health, at-risk stations, maintenance queue, per-parameter health, trend and degradation risk.

The existing anomaly explanation window also includes a **Generate Correction Suggestion** action that calls `/api/phase2c/heal` without mutating the database.
