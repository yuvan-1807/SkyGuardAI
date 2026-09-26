# SkyGuard AI — Final Run Order

## Normal demo
1. Extract this ZIP to a normal writable folder.
2. Run `INSTALL_DEPENDENCIES.bat` once. It creates the local `.venv` and installs the required Python packages.
3. Run `START_SKYGUARD.bat`.
4. Open `http://127.0.0.1:8050/` if the browser does not open automatically.
5. Press **Start Live Network** inside the dashboard.

## Final backend verification
Run `RUN_PHASE3J.bat` in a separate command window. It does not require the dashboard server to be running because the regression tests use isolated databases and Flask test clients. The full log is saved as `PHASE3J_RESULTS.txt`.

## SHAP-only smoke check
`test_shap.py` remains available as a dedicated explainability smoke test. It is intentionally separate from the main regression runner because SHAP can be comparatively heavy on some Windows Python environments.

## Troubleshooting
If either BAT reports missing packages, run `INSTALL_DEPENDENCIES.bat` once and then rerun the BAT. The launchers automatically reuse `.venv`, so the dashboard and regression suite use the same interpreter and package set.
