"""Phase 3J — deliberate failure / resilience matrix for the SkyGuard prototype.

This test intentionally exercises malformed input, invalid values, security
failures, replay/idempotency, ordering/backfill, quarantine, rate limiting,
and the operational API surface. It uses an isolated SQLite database and
never modifies the user's normal aws_data.db.
"""
from __future__ import annotations

import csv
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE_CSV = ROOT / "10_AWS_stations_combined.csv"


def make_small_csv(destination: Path, rows_per_station: int = 30) -> None:
    counts: dict[str, int] = {}
    with SOURCE_CSV.open(newline="", encoding="utf-8") as src, destination.open("w", newline="", encoding="utf-8") as dst:
        reader = csv.DictReader(src)
        fields = reader.fieldnames or []
        writer = csv.DictWriter(dst, fieldnames=fields)
        writer.writeheader()
        for row in reader:
            station = row.get("station", "")
            if counts.get(station, 0) >= rows_per_station:
                continue
            writer.writerow(row)
            counts[station] = counts.get(station, 0) + 1
            if len(counts) == 10 and all(v >= rows_per_station for v in counts.values()):
                break


def assert_status(resp, expected: int, label: str):
    if resp.status_code != expected:
        raise AssertionError(f"{label}: expected HTTP {expected}, got {resp.status_code}: {resp.get_json(silent=True)}")
    return resp.get_json(silent=True) or {}


def run():
    with tempfile.TemporaryDirectory(prefix="skyguard_3j_") as td:
        td_path = Path(td)
        db = td_path / "phase3j.db"
        csv_path = td_path / "mini.csv"
        make_small_csv(csv_path)

        os.environ["SKYGUARD_DB_PATH"] = str(db)
        os.environ["SKYGUARD_CSV_PATH"] = str(csv_path)
        os.environ["SKYGUARD_EDGE_SECRET"] = "skyguard-demo-master-secret-change-me"

        # Import only after environment variables are set; app_new initializes the isolated DB.
        import app_new  # type: ignore
        from config import STATIONS
        from device_registry import provision_batch, revoke
        from edge_security import build_envelope, get_master_secret

        client = app_new.app.test_client()
        station1, station2, station3 = STATIONS[:3]
        devices = [
            {"device_id": f"3J-DEV-{i:02d}", "station_id": station, "source": "3J"}
            for i, station in enumerate(STATIONS, 1)
        ]
        provision_batch(str(db), devices, source="3J")
        app_new._EDGE_RATE.clear()

        # 1) Basic operational endpoints must respond cleanly.
        for route in (
            "/api/health",
            "/api/data-quality",
            "/api/backfill-status",
            "/api/device-registry",
            "/api/edge-security-status",
            "/api/final-dashboard",
            f"/api/station-view/{station1}",
            f"/api/station-analysis/{station1}",
            "/api/anomaly/999999/summary",
        ):
            resp = client.get(route)
            if route.endswith("/summary"):
                assert resp.status_code in (200, 404), (route, resp.status_code, resp.get_json(silent=True))
            else:
                assert resp.status_code == 200, (route, resp.status_code, resp.get_json(silent=True))

        # 2) Malformed / invalid normal-ingest inputs.
        assert_status(client.post("/api/ingest", data=b"{bad", content_type="application/json"), 400, "invalid JSON")
        assert_status(client.post("/api/ingest", json={"temperature": 30, "pressure": 1012, "humidity": 70}), 400, "missing station")
        assert_status(client.post("/api/ingest", json={"station_id": station1, "temperature": "abc", "pressure": 1012, "humidity": 70}), 400, "invalid temperature")
        assert_status(client.post("/api/ingest", json={"station_id": station1, "temperature": 30, "pressure": 1012, "humidity": 70, "event_time": "not-a-time"}), 400, "invalid event_time")

        # 3) Secure envelope failures.
        missing_identity = build_envelope("3J-UNKNOWN", station1, 1, "heartbeat", {"mode": "test"}, get_master_secret())
        assert_status(client.post("/api/edge-secure", json=missing_identity), 403, "unknown registered device")

        bad_sig = build_envelope("3J-DEV-01", station1, 1, "heartbeat", {"mode": "test"}, get_master_secret())
        bad_sig["signature"] = "0" * len(bad_sig["signature"])
        assert_status(client.post("/api/edge-secure", json=bad_sig), 400, "invalid HMAC")

        bad_kind = build_envelope("3J-DEV-01", station1, 2, "heartbeat", {"mode": "test"}, get_master_secret())
        bad_kind["kind"] = "bogus"
        assert_status(client.post("/api/edge-secure", json=bad_kind), 400, "invalid kind")

        oversized = b"x" * (256 * 1024 + 1)
        assert_status(client.post("/api/edge-secure", data=oversized, content_type="application/json"), 413, "oversized envelope")

        # 4) Valid secure message, duplicate idempotency, and sequence collision.
        hb1 = build_envelope("3J-DEV-01", station1, 10, "heartbeat", {"mode": "test", "samples_processed": 1}, get_master_secret())
        assert_status(client.post("/api/edge-secure", json=hb1), 200, "secure heartbeat")
        dup = assert_status(client.post("/api/edge-secure", json=hb1), 200, "exact duplicate")
        assert dup.get("status") == "duplicate", dup

        collision = build_envelope("3J-DEV-01", station1, 10, "heartbeat", {"mode": "test"}, get_master_secret())
        assert_status(client.post("/api/edge-secure", json=collision), 400, "sequence collision")

        # 5) Stale-but-never-before-seen sequence must be rejected (replay/staleness invariant).
        hb12 = build_envelope("3J-DEV-01", station1, 12, "heartbeat", {"mode": "test"}, get_master_secret())
        assert_status(client.post("/api/edge-secure", json=hb12), 200, "newer sequence")
        hb11 = build_envelope("3J-DEV-01", station1, 11, "heartbeat", {"mode": "test"}, get_master_secret())
        stale = assert_status(client.post("/api/edge-secure", json=hb11), 403, "stale sequence")
        assert "sequence" in str(stale.get("error", "")).lower(), stale

        # 6) Device/station binding mismatch.
        mismatch = build_envelope("3J-DEV-02", station3, 1, "heartbeat", {"mode": "test"}, get_master_secret())
        assert_status(client.post("/api/edge-secure", json=mismatch), 403, "binding mismatch")

        # 7) Revoked device is blocked; another device remains operational.
        revoke(str(db), "3J-DEV-03", source="3J", reason="failure matrix revoke")
        revoked = build_envelope("3J-DEV-03", station3, 1, "heartbeat", {"mode": "test"}, get_master_secret())
        assert_status(client.post("/api/edge-secure", json=revoked), 403, "revoked device")
        other = build_envelope("3J-DEV-04", STATIONS[3], 1, "heartbeat", {"mode": "test"}, get_master_secret())
        assert_status(client.post("/api/edge-secure", json=other), 200, "other active device")

        # 8) Normal API sequence collision / duplicate.
        ingest1 = {
            "station_id": station2, "device_id": "3J-DEV-02", "sequence": 20, "message_id": "3J-M1",
            "event_time": "2030-01-02 12:00:00", "temperature": 30.0, "pressure": 1012.0, "humidity": 70.0,
            "source": "3J",
        }
        r1 = assert_status(client.post("/api/ingest", json=ingest1), 200, "normal ingest")
        assert r1.get("event_time") == ingest1["event_time"]
        assert r1.get("receive_time")
        dup_ingest = dict(ingest1)
        dup_ingest["temperature"] = 31.0
        dup_resp = assert_status(client.post("/api/ingest", json=dup_ingest), 200, "ingest duplicate")
        assert dup_resp.get("status") == "duplicate", dup_resp
        collision_ingest = dict(ingest1)
        collision_ingest["message_id"] = "3J-M2"
        assert_status(client.post("/api/ingest", json=collision_ingest), 400, "ingest sequence collision")

        # 9) True event-time out-of-order insertion.
        ingest_late = {
            "station_id": station2, "device_id": "3J-DEV-02", "sequence": 22, "message_id": "3J-M3",
            "event_time": "2030-01-01 12:00:00", "temperature": 30.1, "pressure": 1011.9, "humidity": 70.2,
            "source": "3J",
        }
        late = assert_status(client.post("/api/ingest", json=ingest_late), 200, "out-of-order ingest")
        assert late.get("out_of_order") is True, late
        assert late.get("processing_basis") == "event_time", late

        # 10) Physically impossible humidity is handled without crashing and is not trusted.
        bad_value = {
            "station_id": station1, "device_id": "3J-DEV-01", "sequence": 30, "message_id": "3J-M4",
            "event_time": "2030-01-03 12:00:00", "temperature": 30.0, "pressure": 1012.0, "humidity": 120.0,
            "source": "3J",
        }
        bad = assert_status(client.post("/api/ingest", json=bad_value), 200, "invalid physical sensor value")
        assert bad["data_quality"]["training_eligible"] is False, bad
        assert bad["classification"] if "classification" in bad else True

        # 11) Deliberate sensor anomaly should be quarantined, while raw data remains present.
        anomaly = {
            "station_id": STATIONS[4], "device_id": "3J-DEV-05", "sequence": 40, "message_id": "3J-M5",
            "event_time": "2030-01-04 12:00:00", "temperature": 58.0, "pressure": 1070.0, "humidity": 15.0,
            "source": "3J",
        }
        an = assert_status(client.post("/api/ingest", json=anomaly), 200, "sensor anomaly ingest")
        assert an["data_quality"]["training_eligible"] is False, an
        dq = client.get("/api/data-quality").get_json()
        assert int(dq["quarantined_observations"]) >= 1, dq
        assert int(dq["lineage_records"]) >= int(dq["total_observations"]) * 2, dq

        # 12) Rate limiter eventually rejects excessive secure traffic.
        app_new._EDGE_RATE.clear()
        rate_statuses = []
        next_seq = 100
        for i in range(125):
            env = build_envelope("3J-DEV-06", STATIONS[5], next_seq + i, "heartbeat", {"mode": "rate-test"}, get_master_secret())
            rate_statuses.append(client.post("/api/edge-secure", json=env).status_code)
        assert 429 in rate_statuses, f"rate limiter never triggered: {rate_statuses[-10:]}"

        # 13) Verify backfill/security counters reflect exercised failures.
        backfill = client.get("/api/backfill-status").get_json()
        assert backfill["processing_basis"] == "event_time"
        assert int(backfill["out_of_order_observations"]) >= 1

        security = client.get("/api/edge-security-status").get_json()
        assert int(security["authenticated_messages"]) >= 3
        assert int(security["rejected_messages"]) >= 3
        assert int(security["rate_limited"]) >= 1

        # 14) Database has the expected durable state and no 500-class response occurred above.
        conn = sqlite3.connect(db)
        try:
            assert conn.execute("SELECT COUNT(*) FROM device_registry").fetchone()[0] == 10
            assert conn.execute("SELECT COUNT(*) FROM edge_secure_receipts").fetchone()[0] >= 3
            assert conn.execute("SELECT COUNT(*) FROM observation_lineage").fetchone()[0] >= conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] * 2
            assert conn.execute("SELECT COUNT(*) FROM observation_quarantine").fetchone()[0] >= 1
        finally:
            conn.close()

    print("PHASE 3J FAILURE MATRIX: PASS")
    print("Malformed JSON handled: True")
    print("Missing/invalid ingest fields rejected safely: True")
    print("Invalid HMAC rejected safely: True")
    print("Invalid envelope kind rejected safely: True")
    print("Oversized envelope rejected safely: True")
    print("Duplicate message idempotency: True")
    print("Sequence collision rejected: True")
    print("Stale/replayed sequence rejected: True")
    print("Device/station binding enforced: True")
    print("Revoked device blocked / other device continues: True")
    print("Event-time out-of-order handling: True")
    print("Impossible physical value not trusted: True")
    print("Sensor anomaly quarantined: True")
    print("Rate limiting triggered: True")
    print("Durable lineage + audit preserved: True")
    print("Operational API surface: PASS")


if __name__ == "__main__":
    run()
