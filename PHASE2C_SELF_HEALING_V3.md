# Phase 2C Improved Self-Healing

## What changed
Self-healing remains read-only and does not modify the ingestion path or raw sensor data.

The correction advisor is now a hybrid estimator rather than a single median/IDW rule:

1. **Recent trusted station history** — robust median of non-anomalous recent observations.
2. **Calendar-context baseline** — same-station values from the same month are used to reduce bias from seasonal/climate differences.
3. **Climatology-adjusted spatial IDW** — nearby stations contribute their deviation from their own baseline, which is transferred to the target station baseline. This avoids directly copying a warm/cool coastal or inland climate into another station.
4. **Cross-parameter ML** — a station-specific ExtraTrees regressor is trained only on trusted records to predict one parameter from the other two parameters plus cyclical time features. A time-series cross-validation MAE/RMSE is reported for transparency.
5. **Evidence-weighted ensemble** — available estimates are combined according to data support and model quality; disagreement reduces confidence.
6. **Physical bounds guardrail** — corrected values are kept within engineering/meteorological parameter bounds.

## Interpretation
The output is an **evidence-based imputation**, not a claim that the hidden true value is known exactly. The dashboard shows the individual component estimates and confidence so an evaluator can see why the suggestion was produced.

## Data safety
The original raw observation is never overwritten. The endpoint remains `POST /api/phase2c/heal` and returns a read-only proposal.

## Run smoke test
```powershell
python test_self_healing.py
```
