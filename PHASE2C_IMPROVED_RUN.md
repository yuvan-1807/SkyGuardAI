# SkyGuard Phase 2C — Improved Self-Healing v23

## Purpose
This branch improves only the Phase 2C read-only self-healing layer. The proven Phase 2B/2C ingestion and live-network paths remain frozen.

## How the suggested value is produced
For each parameter, the advisor can use up to four independent sources:

1. Recent trusted station history (non-anomalous values).
2. Same-month station context, blended with recent history.
3. Nearby-station residual transfer: each neighbor's current value is compared with its own same-month baseline, and that deviation is spatially weighted with IDW before being transferred to the target station.
4. Cross-parameter ExtraTrees regression trained only on trusted station observations using the other two meteorological parameters plus cyclical time features. Three-fold time-series cross-validation estimates MAE/RMSE.

Available components are combined with evidence weights. Stronger model validation and more reference data increase weight; disagreement among components lowers confidence.

A physical-consistency guardrail is run on the complete corrected triplet. It is visible in the API/dashboard and lowers overall confidence when the correction creates a reported physics concern. It is a diagnostic guardrail, not ground truth.

## Data safety
- Raw sensor observations are never overwritten.
- `POST /api/phase2c/heal` remains read-only.
- No changes are made to `/api/ingest`.
- No changes are made to the live Edge AI or live-network simulator.

## Run
1. Start the dashboard:

```powershell
python app_new.py
```

2. Start the live ten-station demo:

```powershell
python live_network_simulator.py --demo --scenario mixed
```

3. Open an anomaly and click **Generate Correction Suggestion**.

The panel shows observed value, suggested value, confidence, and the temporal/spatial/ML component estimates used to form the suggestion.

## Direct smoke test
```powershell
python test_self_healing.py
```

Expected:
```text
Phase 2C improved self-healing smoke test: PASS
```
