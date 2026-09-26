# Phase 3G — Event-Time Backfill Run Guide

This build keeps the clean operational UI unchanged and adds backend event-time ordering/out-of-order handling.

## 1. Automated regression tests

Run:

```powershell
python test_phase3g_event_time.py
python test_phase3f_lineage_bootstrap.py
python test_phase3b_weather.py
python test_phase3c_extratrees.py
python test_phase3d_spatial.py
python test_phase3e_decision_fusion.py
python test_ui_baseline.py
```

## 2. Start the app

```powershell
python app_new.py
```

The app will initialize a fresh SQLite database from the bundled CSV when needed.

## 3. Manual event-time / backfill test

Keep the app running. Open a second PowerShell window and run the following three requests from the package folder.

```powershell
$base = @{
  station_id='AWS_03_Chennai'
  device_id='3G-TEST-PS'
  temperature=31.2
  pressure=1008.3
  humidity=72.0
  source='PHASE3G_MANUAL_TEST'
  detection_source='Soft Backend'
  delivery_mode='LIVE'
}

Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8050/api/ingest' -ContentType 'application/json' -Body (([pscustomobject]($base + @{sequence=1;message_id='3g-ps-001';event_time='2026-09-25 10:00:00'})) | ConvertTo-Json)
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8050/api/ingest' -ContentType 'application/json' -Body (([pscustomobject]($base + @{sequence=2;message_id='3g-ps-002';event_time='2026-09-25 10:02:00'})) | ConvertTo-Json)
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8050/api/ingest' -ContentType 'application/json' -Body (([pscustomobject]($base + @{sequence=3;message_id='3g-ps-003';event_time='2026-09-25 10:01:00'})) | ConvertTo-Json)
```

The third response should contain:

```text
out_of_order : True
processing_basis : event_time
```

Then open:

```text
http://127.0.0.1:8050/api/backfill-status
```

It should report at least one `out_of_order_observation` and list the late observation.

## 4. What this proves

The backend keeps the physical insertion record, but logical analytics and dashboard/historical ordering use `event_time`. Temporal, multivariate, ExtraTrees and spatial baselines are cut off at the observation's event time, preventing future leakage during backfill processing.
