# Phase 2B frozen integration guard

This branch was created from the working Phase 2A + SHAP baseline.

Verified unchanged:
- `data_injector.py` SHA-256: `6898cb5f3036cbac420846dfd1757966`
- `config.py` MD5: `6f64d141b98053fd07337ee7d4f744c8` (kept unchanged)
- `/api/ingest` function SHA-256: `523a67b75c741c8a5fc85b1af116189860ecc22875df14747938463f6c76f734`

Phase 2B changes are additive:
- `edge_ai.py`
- `edge_ai_model.json`
- upgraded `edge_emulator.py`
- `edge` status/events tables and `/api/edge-*` endpoints
- dashboard display of edge state and all configured stations
- ESP32 reference firmware `EDGE_AI_ESP32.ino`
