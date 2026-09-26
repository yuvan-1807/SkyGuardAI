# Phase 2C Frozen Guard

Phase 2C is built from the verified Phase 2B live-network baseline. The following live-path files were copied without modification:

- `data_injector.py`
- `config.py`
- `data_loader.py`
- `edge_emulator.py`
- `live_network_simulator.py`
- `anomaly_pipeline.py`
- `edge_ai.py`
- `edge_ai_model.json`

The `/api/ingest` function body in `app_new.py` is also unchanged. Phase 2C adds new endpoints and read-only services around it.

The actual SHA-256 of `edge_emulator.py` in both the Phase 2B baseline and Phase 2C build is:

`0ed67db0769d09f31098e4a4aa68118817136d04db27af751677139c5dae656e`

The existing `FROZEN_INJECTION_HASHES.txt` in the inherited baseline contains an older edge-emulator hash; the file itself was not modified by Phase 2C. The baseline-to-Phase-2C file comparison confirms the edge emulator bytes are identical.
