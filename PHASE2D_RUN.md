# Phase 2D — Edge Security + Offline Resilience

This phase is built on the frozen Phase 2C/v23 data pipeline. The legacy `/api/ingest`, `data_injector.py`, Edge AI model and existing live-network simulator behavior remain available.

## 1. Secure live network demo

Terminal 1:
```powershell
python app_new.py
```

Terminal 2:
```powershell
python live_network_simulator.py --secure --demo --scenario mixed
```

## 2. Offline store-and-forward demonstration

Start dashboard, then:
```powershell
python live_network_simulator.py --secure --demo --scenario mixed --offline-seconds 15
```

During the first 15 seconds the emulator behaves as if the network is unavailable. Normal batches and hard-anomaly events are persisted in `.edge_outbox/` and are NOT deleted. After the outage window, the transport retries oldest-first and removes each message only after an HTTP acknowledgment.

Look for:
- `SECURE QUEUE` / `EDGE OUTBOX` messages during the outage
- `STORE-FORWARD` messages after reconnect
- `synced N queued message(s)` after recovery

## 3. Inspect the security status

Open:
`http://127.0.0.1:8050/api/edge-security-status`

The dashboard Edge AI panel also shows authenticated messages, rejected messages, duplicate protections and rate-limit events.

## 4. Security self-test

```powershell
python test_phase2d.py
```

## 5. Production-style window

```powershell
python live_network_simulator.py --secure --scenario mixed --batch-seconds 600
```

Use `--batch-seconds 300` for a 5-minute window.

## Security behavior

- HMAC-SHA256 device-authenticated envelopes
- device/station identity binding
- sequence + message-id idempotency / replay protection
- max envelope size
- backend rate limiting
- secure model integrity hash check
- durable local outbox for events and batches
- exponential retry/backoff
- delete-on-ACK only
- ordered store-and-forward recovery

For physical ESP32 deployment, provision a unique secret per device rather than the demo master secret; use secure boot/flash encryption and signed firmware where supported by the selected ESP32 hardware.
