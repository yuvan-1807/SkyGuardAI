# SkyGuard AI — Phase 2A v7

This build preserves the working Phase 1 ingestion path and makes Phase 2A robust against a fresh/empty database.

## Run

1. Keep the known-working Phase 1 folder as a backup.
2. Extract this folder.
3. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

4. Start the dashboard FIRST:

```bash
python app_new.py
```

On startup, the app automatically creates `aws_data.db` and, if it is empty, seeds it from the bundled `10_AWS_stations_combined.csv`.

5. In another terminal run:

```bash
python test_ingest.py
```

6. Then run continuous injection:

```bash
python data_injector.py
```

The injector performs an API health check before starting and prints the database path it is using.

## Expected injector output

```text
✅ Loaded baseline data for 10 stations from ...\aws_data.db
✅ API connected: ...\aws_data.db
🚀 Starting real-time data injection (5s interval)...
```

Then each reading should print `NORMAL` or `ANOMALY` and the dashboard should refresh within 5 seconds.

## If it still does not update

Run this in another terminal while the dashboard is running:

```bash
python test_ingest.py
```

If it returns HTTP 200, the API and database path are working; the issue is dashboard refresh/display. If it returns HTTP 500, copy the dashboard terminal traceback.


## v13 live overview fix
The three System Overview trend panels are plain HTML containers owned by `skyguard_live.js`, avoiding conflicts between external Plotly updates and Dash `dcc.Graph`. The injector/API/database pipeline is unchanged.


## Phase 2A v14 dashboard refresh
The System Overview is now updated by the native Dash `dcc.Interval` callback every 5 seconds. The three trend panels are native `dcc.Graph` components. No custom browser-side polling is used. Keep `app_new.py` running and start `data_injector.py` in a second terminal.
