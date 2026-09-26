# SkyGuard Phase 3C — ExtraTrees ML Evidence

Phase 3C adds station-specific ExtraTrees regression as a **new evidence layer**. It does not replace the existing Edge, temporal, multivariate, psychrometric, security, or weather-context detectors. Final three-way classification remains reserved for the Decision Fusion phase.

## What it does

For each target parameter, SkyGuard estimates an expected value from the other sensor parameters plus calendar context:

```text
pressure + humidity + time context
             ↓
        ExtraTrees
             ↓
 Expected temperature
             ↓
Observed − Expected
             ↓
        ML residual
```

Equivalent target-specific models are used for pressure and humidity.

## Anti-leakage and trust rules

- Training history is ordered by `event_time` and excludes known anomalies/quarantined observations.
- Walk-forward time-series validation is used instead of random shuffling.
- Current target value is **not** used as a model feature; it is the quantity being evaluated.
- Out-of-bounds current companion parameters make ML evidence unavailable rather than feeding obviously corrupt inputs to the model.
- Raw observations are never overwritten.

## Output

`layers.ml_evidence` contains model availability, per-parameter expected values, signed/absolute residuals, normalized residuals, cross-validated MAE/RMSE, model quality, training sample counts, and anomaly-evidence flags.

The current threshold marks a parameter as an ML anomaly candidate at >= 4 robust residual-scale units. This is evidence only; Phase 3E will combine it with the other layers before producing NORMAL / GENUINE WEATHER EVENT / SENSOR ANOMALY.

## Prototype vs production

The SIH prototype uses Python + scikit-learn + SQLite. The production architecture can move the same model/evidence contract to the scalable API/time-series stack described in the master specification.
