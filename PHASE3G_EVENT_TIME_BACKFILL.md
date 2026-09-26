# Phase 3G — Event-Time Ordering + Out-of-Order Backfill

## Scope

Preserve `event_time` as the authoritative measurement timestamp and `receive_time` as backend arrival time. Late observations are flagged with `out_of_order=1` and analyzed against only observations that occurred before their event time.

## Behavior

- Incoming `event_time` is parsed and validated.
- Duplicate `message_id` and `(device_id, sequence)` observations are rejected/idempotently acknowledged.
- A reading is marked late when its `event_time` precedes the station's latest stored event time.
- Dashboard/historical queries use `event_time` ordering rather than insertion order.
- Temporal, multivariate baseline, ExtraTrees training history, and spatial baselines exclude future observations relative to the analyzed event.
- Late observations do not mutate the live rolling entropy/frozen-state detector.
- `/api/backfill-status` exposes late-observation counts and recent backfills.

## Prototype note

The SQLite rows are still physically appended in arrival order; logical analytics and dashboard ordering are event-time based. This is the appropriate prototype behavior and preserves the raw arrival record.
