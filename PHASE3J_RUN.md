# Phase 3J — Final Regression / Failure-Matrix Run Guide

## 1. One-click run

Use:

`RUN_PHASE3J.bat`

The launcher:

1. switches to the package folder;
2. prints the Python interpreter in use;
3. validates Flask/Pandas/NumPy/SciPy/scikit-learn/Requests/SHAP;
4. runs the complete Phase 2C–3J regression set;
5. saves all output to `PHASE3J_RESULTS.txt`.

Expected final line:

`PHASE 3J REGRESSION SUITE: PASS`

## 2. Failure matrix alone

```powershell
python test_phase3j_failure_matrix.py
```

Expected ending:

`PHASE 3J FAILURE MATRIX: PASS`

The matrix covers malformed JSON, invalid values, HMAC failures, unsupported kinds, oversized envelopes, duplicate idempotency, sequence collisions, stale/replayed sequences, device/station binding, revocation, event-time backfill, physical-value rejection, quarantine, rate limiting, and durable lineage/audit state.

## 3. Important fixes in the final package

- `SKYGUARD_DB` is accepted as a compatibility alias for `SKYGUARD_DB_PATH`.
- Backend schema initialization runs on module import, so operational API tests and test clients always get a ready database.
- Registered devices reject stale or replayed sequence numbers while exact duplicates remain idempotent.
- Legacy/bootstrap observations receive both migrated and quality-gate lineage stages so the stored history has a complete audit representation.
- The UI readability guard is intentionally larger, including the SHAP/evidence modal.
- `test_phase3i_operational.py` is included in the final regression runner.

## 4. Live runtime spot-check

Start the app:

```powershell
python app_new.py
```

Then use the dashboard **Start Live Network** control.

Optional PowerShell checks:

```powershell
Invoke-RestMethod http://127.0.0.1:8050/api/device-registry | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8050/api/edge-security-status | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8050/api/data-quality | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8050/api/backfill-status | ConvertTo-Json -Depth 8
```
