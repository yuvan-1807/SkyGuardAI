# SkyGuard AI — Phase 2A Explainability (Actual SHAP)

This branch is based on the verified live-injection build and keeps the ingestion contract frozen.

## Frozen live path

The following files are byte-for-byte unchanged from the verified v15 baseline:

- `data_injector.py`
- `config.py`
- `data_loader.py`
- `edge_emulator.py`

The `/api/ingest` implementation is also unchanged. The live ingestion pipeline does **not** import or execute SHAP during ingestion.

## Actual SHAP implementation

The explanation path now uses the official `shap` library (`0.52.0`) with `shap.TreeExplainer` and a station-specific `IsolationForest` trained on that station's observations currently marked normal.

The existing SkyGuard consolidated anomaly verdict remains the source of truth. The IsolationForest is an **explanation model**; it is not silently replacing the existing anomaly rules.

For a clicked anomaly the API returns:

- signed SHAP values for Temperature, Pressure, and Humidity;
- anomaly-driving contribution percentages;
- contribution direction (`toward anomaly` / `toward normality`);
- SHAP base value and model output;
- IsolationForest prediction and score;
- station baseline values;
- human-readable SHAP narrative;
- existing layer-by-layer evidence.

## Why SHAP is read-only

SHAP is calculated only by `GET /api/anomaly/<row_id>` when an operator opens an anomaly explanation. `process_reading()` explicitly sets `include_shap=False`, preventing explainability computation from slowing or destabilizing the proven injector -> API -> database flow.

## Python compatibility

`requirements.txt` pins `shap==0.52.0`. The current PyPI release metadata lists Python 3.14 support for this release.
