# Phase 3I — PC Test Run

## 1. Start

```powershell
python app_new.py
```

## 2. Registry should now be usable with secure live simulator

In a second PowerShell:

```powershell
python live_network_simulator.py --demo --secure --fresh-outbox
```

## 3. Check registry

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/device-registry | ConvertTo-Json -Depth 8
```

Expected summary after simulator startup:

- total: 10
- active: 10
- revoked: 0

## 4. Check edge security

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/edge-security-status | ConvertTo-Json -Depth 8
```

`authenticated_messages` should increase while the secure simulator runs. `devices` should contain the demo device IDs.

## 5. Check dashboard operational snapshot

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/final-dashboard | ConvertTo-Json -Depth 10
```

Verify:

- `live_nodes` reaches 10 while all 10 heartbeats are active.
- each item in `stations` has a non-null `health` object once data is available.
- `system_health_score` is not simply the network live percentage.
- `network_coverage_pct` reports the separate connectivity percentage.

## 6. Fast station endpoint

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/station-view/AWS_03_Chennai | ConvertTo-Json -Depth 8
```

This should return quickly without recomputing the full multi-layer analysis.

## 7. Deep station analysis

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/station-analysis/AWS_03_Chennai | ConvertTo-Json -Depth 12
```

This is intentionally separate from the fast station-view call.

## 8. Fast anomaly summary

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/anomaly/1/summary | ConvertTo-Json -Depth 8
```

Use a real anomaly ID when one exists.
