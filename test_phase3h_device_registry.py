from __future__ import annotations
import sqlite3
import tempfile
from pathlib import Path

from config import STATIONS
from device_registry import ensure_registry_table, provision, revoke, restore, authorize, summary
from edge_security import build_envelope, get_master_secret, verify_envelope


def run():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "registry.db")
        ensure_registry_table(db)
        ids = {s: f"3H-TEST-{i:02d}" for i, s in enumerate(STATIONS, 1)}
        for station, device in ids.items():
            provision(db, device, station, source="TEST")
        s = summary(db)
        assert s == {"total": 10, "active": 10, "revoked": 0}, s

        # Authentic message + active registry = authorized.
        env = build_envelope(ids[STATIONS[0]], STATIONS[0], 1, "heartbeat", {"mode":"test"}, get_master_secret())
        verify_envelope(env, get_master_secret())
        auth = authorize(db, ids[STATIONS[0]], STATIONS[0], 1)
        assert auth["authorized"]

        # Revoke one device; its own traffic is denied, another station remains active.
        revoke(db, ids[STATIONS[0]], source="TEST", reason="integration revoke")
        denied = authorize(db, ids[STATIONS[0]], STATIONS[0], 2)
        assert not denied["authorized"] and "REVOKED" in denied["reason"]
        allowed = authorize(db, ids[STATIONS[1]], STATIONS[1], 1)
        assert allowed["authorized"]

        restore(db, ids[STATIONS[0]], source="TEST")
        restored = authorize(db, ids[STATIONS[0]], STATIONS[0], 3)
        assert restored["authorized"]

        print("PHASE 3H DEVICE REGISTRY TESTS: PASS")
        print("10 devices provisioned: True")
        print("Active authorization: True")
        print("Revoked device rejected: True")
        print("Other device continues: True")
        print("Restore works: True")
        print("Registry summary:", summary(db))


if __name__ == "__main__":
    run()
