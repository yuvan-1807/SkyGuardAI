# SkyGuard Phase 3A — Canonical Observation Data Contract

The v37 baseline is preserved. Phase 3A adds an additive observation contract without removing legacy fields.

## Core observation fields

- `station_id` / legacy `station_name`
- `device_id`
- `sequence`
- `message_id`
- `event_time` — when the sensor measurement occurred
- `receive_time` — when the backend received the observation
- `temperature`
- `pressure`
- `humidity`

## Data-quality / lineage fields

- `classification`
- `source`
- `model_version`
- `anomaly_score`
- `is_clean`
- `validation_status`
- `imputation_status`
- `quarantine_status`

## Compatibility

The legacy `timestamp` column remains intact and is treated as an API/dashboard alias for `event_time`.
Existing rows are backfilled with `event_time = timestamp` and `receive_time = created_at`.

Legacy ingestion remains accepted: if a client sends only `timestamp`, it becomes `event_time`; `receive_time` is assigned by the backend.

## Critical rule

`event_time` and `receive_time` are never conflated. A delayed/offline observation retains its original measurement time while recording its later backend arrival time.

This contract is the foundation for later weather-event classification, ML residual evidence, spatial evidence, quarantine/lineage, and out-of-order processing.


## Phase 3A Fix 1

The bootstrap path now populates the canonical contract immediately. Existing and CSV-seeded rows receive `station_id`, `event_time`, `receive_time`, `source`, `validation_status`, and `is_clean` values rather than remaining NULL after first startup.

A stable `station_id` column is persisted separately from `station_name`; the canonical station identifier follows the normalized station key used by the API.
