from __future__ import annotations
import importlib.util
import platform
import sys

MODULES = [
    ("flask", "Flask"), ("pandas", "Pandas"), ("numpy", "NumPy"),
    ("scipy", "SciPy"), ("sklearn", "scikit-learn"), ("requests", "Requests"),
    ("shap", "SHAP"),
]

print("SkyGuard AI environment check")
print("Python:", sys.executable)
print("Version:", platform.python_version(), platform.platform())
missing=[]
for mod,label in MODULES:
    spec=importlib.util.find_spec(mod)
    if spec is None:
        print(f"MISSING: {label} ({mod})")
        missing.append(label)
    else:
        try:
            m=__import__(mod)
            print(f"OK: {label} {getattr(m, '__version__', '')}".rstrip())
        except Exception as exc:
            print(f"BROKEN: {label}: {exc}")
            missing.append(label)
if missing:
    print("\nInstall with: python -m pip install -r requirements.txt")
    raise SystemExit(1)
print("ENVIRONMENT: PASS")
