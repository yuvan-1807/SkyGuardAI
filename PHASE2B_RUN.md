# SkyGuard Phase 2B — Edge AI Hard-Anomaly Gate

## What changed
The existing `edge_emulator.py` was useful as a starting point, but it originally sent every generated sample directly to `/api/ingest`. That did not model the intended ESP32 architecture.

This phase upgrades it into a virtual ESP32 edge node:

- local hard-anomaly gate
- compact portable decision-tree model
- local spike/range/frozen-value safety checks
- hard anomalies are blocked from normal `/api/ingest`
- only a small edge event is sent for blocked faults
- normal observations are buffered locally
- normal observations are transmitted in batches
- `--demo` compresses the 5/10 minute window to 10 seconds for SIH video recording

## Frozen integration guard
The existing `data_injector.py`, `config.py`, and `/api/ingest` function are unchanged from the working Phase 2A/SHAP baseline. Phase 2B adds separate `/api/edge-*` endpoints and additive edge tables.

## Start dashboard
```powershell
python app_new.py
```

## Demo: normal edge filtering + batching
```powershell
python edge_emulator.py --demo --max-samples 20
```

The emulator will buffer normal readings. Every ~10 seconds in demo mode it sends a trusted batch through `/api/edge-batch`, which routes each reading through the existing `/api/ingest` behavior.

## Demo: hard anomaly blocked locally
```powershell
python edge_emulator.py --demo --fault PHYSICS_BREACH --fault-after 10 --max-samples 11
```

Expected sequence:
- first 10 observations: `EDGE GATE PASS -> buffered`
- fault observation: `EDGE GATE BLOCKED locally`
- a compact `/api/edge-event` is recorded
- the bad telemetry is NOT posted to `/api/ingest`

Other demonstrations:
```powershell
python edge_emulator.py --demo --fault SPIKE --fault-after 10 --max-samples 11
python edge_emulator.py --demo --fault FROZEN_SENSOR --fault-after 10 --max-samples 16
python edge_emulator.py --demo --fault INVALID --fault-after 10 --max-samples 11
```

## Check edge status
Open:

`http://127.0.0.1:8050/api/edge-status`

or watch the **Edge AI Quality Gate** panel in the dashboard overview.

## Offline unit tests
```powershell
python test_edge_ai.py
```

## Re-train/export the portable model
```powershell
python train_edge_model.py
```

The generated `edge_ai_model.json` is used by the emulator at runtime. The actual ESP32 firmware does not need scikit-learn; the exported tree can be embedded as native C/C++ comparisons.


## Unified anomaly feed (v18)
Hard anomalies blocked by Edge AI are stored as compact `edge_events` and are now merged into the same dashboard anomaly feed as backend/soft anomalies. Clicking an edge event opens an Edge AI explanation panel; backend anomalies continue to use the SHAP explanation path. The frozen `/api/ingest` path is unchanged.
