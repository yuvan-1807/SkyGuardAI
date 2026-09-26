from pathlib import Path
import ast, re, zipfile, hashlib

ROOT = Path(__file__).resolve().parent
app = ROOT / "app_new.py"
assert app.exists(), "app_new.py missing"
s = app.read_text(encoding="utf-8")
ast.parse(s)
required = [
    "DASHBOARD_HTML", "weather_context", "ExtraTrees", "spatial_evidence",
    "device_registry", "validated_observations", "observation_lineage",
    "/api/final-dashboard", "/api/station-view/", "/api/anomaly/",
    "/api/simulation/start", "/api/simulation/stop", "/api/simulation/status",
    "Offline → Synced", "Explainability", "SHAP", "Start Live Network", "SIMULATION RUNNING",
    "FINAL-CONSOLIDATED-3J", "validated_observations", "QUALITY_GATE"
]
missing = [x for x in required if x not in s]
assert not missing, f"Missing required UI/backend markers: {missing}"
registry_text = (ROOT / "device_registry.py").read_text(encoding="utf-8")
assert "Replay or stale sequence" in registry_text, "Monotonic replay guard missing"
# Large-text guardrails: values must remain at readable sizes.
for needle in ["body{font-size:17px!important", ".detail-row{grid-template-columns:210px 115px minmax(0,1fr)!important", ".detail-row strong,.detail-row span{font-size:16px!important}", ".explain-title{font-size:20px!important"]:
    assert needle in s, f"Readability guard missing: {needle}"
print("FINAL STATIC VALIDATION: PASS")
print(f"app_new.py sha256={hashlib.sha256(s.encode()).hexdigest()}")
