# Phase 3J — Failure / Resilience Matrix

## Purpose

3J deliberately breaks the prototype to verify that the backend validates failures, rejects unsafe input, preserves data, and keeps the normal system available.

## Covered cases

- malformed JSON
- missing station ID
- invalid numeric sensor value
- invalid event timestamp
- invalid HMAC/signature
- unsupported secure message kind
- oversized secure envelope
- duplicate message idempotency
- sequence collision
- stale/replayed transport sequence
- device/station binding mismatch
- revoked device
- other devices continuing after revocation
- event-time out-of-order/backfill
- physically impossible humidity
- sensor anomaly quarantine
- rate limiting
- operational API surface
- durable lineage/audit persistence

## Security invariant added during 3J

Transport sequence numbers are monotonic per device. A new message whose sequence is at or below the last accepted sequence is rejected as stale/replayed. Exact retransmission of an already-recorded `(device_id, sequence, message_id)` remains idempotent.
