from __future__ import annotations
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TESTS = [
    "test_edge_ai.py",
    "test_phase2c.py",
    "test_phase2d.py",
    "test_phase3b_weather.py",
    "test_phase3c_extratrees.py",
    "test_phase3d_spatial.py",
    "test_phase3e_decision_fusion.py",
    "test_phase3f_lineage_bootstrap.py",
    "test_phase3f_quality_gate.py",
    "test_phase3g_event_time.py",
    "test_phase3h_device_registry.py",
    "test_phase3i_all10_registry.py",
    "test_phase3i_operational.py",
    "test_self_healing.py",
    "test_ui_baseline.py",
    "validate_final_static.py",
    "test_phase3j_failure_matrix.py",
]

# These variables are application-runtime settings. Individual tests that need a
# database or CSV path set their own isolated values. Removing inherited values
# prevents a user's global environment from leaking into the regression suite.
ISOLATED_ENV_KEYS = {
    "SKYGUARD_DB",
    "SKYGUARD_DB_PATH",
    "SKYGUARD_CSV_PATH",
    "SKYGUARD_API_URL",
}
TEST_TIMEOUT_SECONDS = 300


def main() -> int:
    print(f"Python interpreter: {sys.executable}")
    print(f"Working directory: {ROOT}")
    print(f"Per-test timeout: {TEST_TIMEOUT_SECONDS}s")

    test_env = os.environ.copy()
    for key in ISOLATED_ENV_KEYS:
        test_env.pop(key, None)
    test_env["PYTHONUTF8"] = "1"
    test_env["PYTHONUNBUFFERED"] = "1"
    # Keep each sklearn/scipy child test lightweight and deterministic on
    # Windows laptops where many subprocesses can otherwise over-subscribe CPU threads.
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        test_env[key] = "1"

    failures: list[str] = []
    for name in TESTS:
        print(f"\n===== {name} =====")
        try:
            result = subprocess.run(
                [sys.executable, "-u", str(ROOT / name)],
                cwd=ROOT,
                env=test_env,
                timeout=TEST_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            failures.append(name)
            print(f"TIMEOUT: {name} (>{TEST_TIMEOUT_SECONDS}s)")
            continue
        except OSError as exc:
            failures.append(name)
            print(f"EXECUTION ERROR: {name}: {exc}")
            continue

        if result.returncode != 0:
            failures.append(name)
            print(f"FAIL: {name} (exit={result.returncode})")
        else:
            print(f"PASS: {name}")

    if failures:
        print("\nPHASE 3J REGRESSION SUITE: FAIL")
        print("Failed tests:", ", ".join(failures))
        return 1

    print("\nPHASE 3J REGRESSION SUITE: PASS")
    print("All verified phases + the Phase 3J failure matrix passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
