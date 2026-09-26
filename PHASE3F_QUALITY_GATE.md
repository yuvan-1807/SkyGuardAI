# Phase 3F — Observation Quality Gate, Quarantine & Data Lineage

Phase 3F adds the data-quality lifecycle required by the SkyGuard master architecture.

```text
RAW RECEIVED
     ↓
QUALITY GATE
     ├── NORMAL ──────────────→ VALIDATED
     ├── WEATHER EVENT ───────→ VALIDATED (unless conflict review is required)
     └── SENSOR ANOMALY ──────→ QUARANTINED

Imputed / self-healed observations → QUARANTINED
Weather/sensor conflict → REVIEW_REQUIRED + QUARANTINE_CANDIDATE
```

## Persistent structures

- `sensor_readings`: retains the raw meteorological observation plus current quality metadata.
- `observation_quarantine`: durable quarantine queue for suspicious/untrusted observations.
- `observation_lineage`: append-only processing history for RAW_RECEIVED / QUALITY_GATE and migrated records.
- `validated_observations`: read-only SQL view used as the trusted ML training source.

## Trust rule

An observation is training-eligible only when:

- `is_clean = 1`
- `validation_status = VALIDATED`
- `quarantine_status = NONE`
- `imputation_status = NONE`
- temperature, pressure and humidity are present

ExtraTrees and Spatial Consistency read from this canonical validated source.

## API

`GET /api/data-quality` returns totals for validated, quarantined, review-required, imputed, trusted-training and lineage records, plus recent quarantine entries.

## Test

```powershell
python test_phase3f_quality_gate.py
```
