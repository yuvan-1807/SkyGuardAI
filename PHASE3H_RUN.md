# Phase 3H — Live Device Registry Integration

This phase finishes the prototype security lifecycle by connecting the 10-station secure simulator to the device registry.

## 1. Automated registry test

```powershell
python test_phase3h_device_registry.py
```

Expected:

```text
PHASE 3H DEVICE REGISTRY TESTS: PASS
10 devices provisioned: True
Active authorization: True
Revoked device rejected: True
Other device continues: True
Restore works: True
Registry summary: {'total': 10, 'active': 10, 'revoked': 0}
```

## 2. Start the backend

```powershell
python app_new.py
```

## 3. Check registry before the live demo

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/device-registry | ConvertTo-Json -Depth 6
```

A fresh DB may show zero devices before the secure simulator starts. That is expected.

## 4. Start the secure live simulator

In a second PowerShell window:

```powershell
python live_network_simulator.py --demo --secure --fresh-outbox
```

The simulator now provisions/validates all 10 persistent demo device IDs before sending heartbeats/events/batches.

Then:

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/device-registry | ConvertTo-Json -Depth 6
```

Expected summary:

```text
total  = 10
active = 10
revoked = 0
```

## 5. Check secure traffic

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/edge-security-status | ConvertTo-Json -Depth 6
```

The authenticated message count and device list should increase as the simulator runs.

## 6. Live revocation test

First obtain the device ID for a station:

```powershell
(Invoke-RestMethod http://127.0.0.1:8050/api/device-registry).devices | Where-Object station_id -eq 'AWS_03_Chennai' | Format-List
```

Copy its `device_id`, then revoke it:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8050/api/device-registry/revoke -ContentType 'application/json' -Body (@{device_id='PASTE_DEVICE_ID';source='LIVE_TEST';reason='intentional revocation test'} | ConvertTo-Json)
```

Expected:

```text
status = ok
status of device = REVOKED
```

Keep the simulator running for several seconds. The revoked station's next secure messages should be rejected while the other stations continue operating.

Then check:

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/device-registry | ConvertTo-Json -Depth 6
Invoke-RestMethod http://127.0.0.1:8050/api/edge-security-status | ConvertTo-Json -Depth 6
```

The registry should show 1 revoked device and 9 active devices. `rejected_messages` should increase.

## 7. Restore the device

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8050/api/device-registry/restore -ContentType 'application/json' -Body (@{device_id='PASTE_DEVICE_ID';source='LIVE_TEST'} | ConvertTo-Json)
```

After restoration, new secure messages from that device can be accepted again.

## Important security behavior

The registry is enforcement-aware:
- If the registry is empty, legacy Phase 2D secure test clients remain compatible.
- Once a device is provisioned, the registry becomes authoritative.
- Unknown or revoked devices are rejected after HMAC verification.
- Station binding is enforced.
- The device secret is not stored; only a key fingerprint is exposed by the registry.
