# Phase 2A — Actual SHAP Build

## Frozen baseline

The verified v15 live-injection path is preserved. Do not edit these files when experimenting with explainability:

- `data_injector.py`
- `config.py`
- `data_loader.py`
- `edge_emulator.py`

Their SHA-256 hashes remain exactly the same as the frozen baseline recorded in `FROZEN_INJECTION_HASHES.txt`.

## Explainability changes only

This branch changes only the explainability path:

- `shap_explainer.py`: replaced the hand-written z-score attribution with the official `shap` library using `TreeExplainer` on a station-specific `IsolationForest` explanation model.
- `anomaly_pipeline.py`: SHAP is explicitly deferred during live ingestion (`include_shap=False`) and enabled only for read-only anomaly inspection.
- `app_new.py`: the anomaly explanation endpoint requests actual SHAP results and the modal displays method, model verdict, signed SHAP values, and contribution percentages.
- `requirements.txt`: adds `shap==0.52.0`.

## Verification performed

- All Python files compile successfully with `python -m py_compile *.py`.
- Actual SHAP execution was tested against a temporary SQLite AWS dataset using `SHAP TreeExplainer + station-specific IsolationForest`; it returned three signed SHAP values and anomaly-driving contribution percentages.

## Install

Run in the project folder:

```powershell
python -m pip install -r requirements.txt
```

## Explainability test

With the project database populated:

```powershell
python test_shap.py
```

Expected output includes:

```text
SHAP method: SHAP TreeExplainer + station-specific IsolationForest
SHAP library: 0.52.0
```

The live injector is not used by this test.
