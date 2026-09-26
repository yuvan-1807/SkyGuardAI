# Phase 2C Run Guide

## 1. Start the dashboard

```powershell
python app_new.py
```

Open `http://127.0.0.1:8050`.

## 2. Keep the working live network

In another terminal:

```powershell
python live_network_simulator.py --demo --scenario mixed
```

The proven Phase 2B live flow remains unchanged.

## 3. Check Health & Status

Open the `Health & Status` tab. It should show all 10 configured stations, network health, at-risk count, maintenance queue and per-station health metrics.

## 4. Test self-healing preview

Open an anomaly from the Anomaly Feed and click:

`Generate Correction Suggestion`

The result is read-only and shows observed vs suggested values, method and confidence.

## 5. Direct API checks

Health:

`GET http://127.0.0.1:8050/api/phase2c/health`

One station:

`GET http://127.0.0.1:8050/api/phase2c/station/AWS_03_Chennai`

Correction preview example:

```powershell
$body = @{ station_id='AWS_03_Chennai'; timestamp='2026-09-23 12:00:00'; temperature=55; pressure=1070; humidity=15 } | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8050/api/phase2c/heal -Method Post -ContentType 'application/json' -Body $body
```

## 6. Regression rule

Do not replace or edit `data_injector.py`, `config.py`, `edge_emulator.py`, `live_network_simulator.py`, or the `/api/ingest` function for Phase 2C work.
