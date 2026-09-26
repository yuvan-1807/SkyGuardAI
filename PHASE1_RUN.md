# SkyGuard Phase 1 – Unified Data Pipeline

## What changed

- Added `config.py` as the single source of truth for the SQLite database and API URL.
- Added `POST /api/ingest` to `app_new.py`.
- Added `GET /api/health` for a quick backend check.
- Changed `data_injector.py` to send readings through `/api/ingest` instead of writing directly to SQLite.
- Fixed `edge_emulator.py` to use the same ingestion endpoint.
- Changed `data_loader.py` and processing layers to use the shared database path.
- Fixed the anomaly modal callback so its analysis content is actually returned to `modal-body`.

## Run

From the project folder:

```bash
pip install -r requirements.txt
python data_loader.py
python app_new.py
```

Open:

`http://127.0.0.1:8050`

Check the backend:

`http://127.0.0.1:8050/api/health`

## Test one injected anomaly

In another terminal:

```bash
python test_ingest.py
```

The dashboard should show a new `AWS_03_Chennai` reading within the next 5-second refresh cycle.

## Run continuous injection

With the dashboard still running:

```bash
python data_injector.py
```

The injector now sends every reading to the backend. The backend runs the existing processing layers, stores the final verdict, and the dashboard reads the same database.

## Run the edge emulator

```bash
python edge_emulator.py
```

The edge emulator also uses `/api/ingest`, so it follows the same path as the normal injector.

## Optional configuration

Set these environment variables if required:

- `SKYGUARD_DB_PATH` – SQLite database location
- `SKYGUARD_CSV_PATH` – combined AWS CSV location
- `SKYGUARD_API_URL` – ingestion endpoint

Example on Windows PowerShell:

```powershell
$env:SKYGUARD_DB_PATH = "$PWD\aws_data.db"
$env:SKYGUARD_API_URL = "http://127.0.0.1:8050/api/ingest"
```
