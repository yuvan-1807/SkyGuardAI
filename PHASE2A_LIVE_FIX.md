# Phase 2A Live Dashboard — Stable Live Path

The live dashboard is served directly by Flask and polls `/api/dashboard-data` every 2 seconds using browser JavaScript. It does not depend on Dash callbacks or Plotly component state. The `/api/ingest` endpoint and existing anomaly pipeline remain the processing path.

## Run

```powershell
python -m pip install -r requirements.txt
python app_new.py
```

Open `http://127.0.0.1:8050` and hard-refresh once (`Ctrl+Shift+R`). Then in another terminal:

```powershell
python data_injector.py
```

The **Total Readings** number should change within 2 seconds of every accepted injection.

## Direct proof

Open `http://127.0.0.1:8050/api/debug` in the browser. The `count` and `latest` values come from the same SQLite database used by `/api/ingest`.
