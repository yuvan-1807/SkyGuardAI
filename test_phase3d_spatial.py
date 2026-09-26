from __future__ import annotations

import os
import sqlite3
import tempfile

from spatial_evidence import SpatialConsistencyEvidence
from anomaly_pipeline import AnomalyPipeline

EVENT_NORMAL = "2024-02-15 12:00:00"
EVENT_ISOLATED = "2024-03-15 12:00:00"
EVENT_COORDINATED = "2024-04-15 12:00:00"


def make_db(path: str):
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_name TEXT NOT NULL,
            timestamp TEXT,
            temperature REAL,
            pressure REAL,
            humidity REAL,
            rainfall REAL,
            label INTEGER DEFAULT 0,
            is_anomaly INTEGER DEFAULT 0,
            anomaly_reason TEXT,
            confidence REAL DEFAULT 0,
            created_at TEXT,
            event_time TEXT,
            receive_time TEXT,
            device_id TEXT,
            sequence INTEGER,
            message_id TEXT,
            classification TEXT,
            source TEXT,
            model_version TEXT,
            anomaly_score REAL,
            is_clean INTEGER DEFAULT 1,
            validation_status TEXT DEFAULT 'VALIDATED',
            imputation_status TEXT DEFAULT 'ORIGINAL',
            quarantine_status TEXT DEFAULT 'NONE'
        )"""
    )
    rows = []
    # 40 days of trusted baselines. Neighbours and target share a stable local
    # pattern; their climatology-adjusted residuals therefore carry across.
    import datetime as _dt
    start = _dt.date(2024, 1, 1)
    for offset in range(120):
        ts = f"{start + _dt.timedelta(days=offset)} 12:00:00"
        # Gujarat normal temperature ~30, Mumbai ~29, Kolkata ~27; all have
        # stable pressure/RH for the synthetic spatial-consistency test.
        vals = {
            "AWS_10_Gujarat": (30.0, 1010.0, 60.0),
            "AWS_02_Mumbai": (29.0, 1009.0, 65.0),
            "AWS_08_Rajasthan": (32.0, 1007.0, 50.0),
        }
        for station, (t, p, h) in vals.items():
            rows.append((station, ts, t, p, h, ts, ts))
    conn.executemany(
        "INSERT INTO sensor_readings(station_name,timestamp,temperature,pressure,humidity,event_time,receive_time) VALUES(?,?,?,?,?,?,?)",
        rows,
    )
    conn.execute("""CREATE VIEW validated_observations AS
      SELECT id, station_name, COALESCE(event_time,timestamp) AS event_time, receive_time,
             temperature, pressure, humidity, is_clean, validation_status, imputation_status, quarantine_status
      FROM sensor_readings
      WHERE temperature IS NOT NULL AND pressure IS NOT NULL AND humidity IS NOT NULL
        AND COALESCE(is_clean,0)=1
        AND COALESCE(validation_status,'')='VALIDATED'
        AND COALESCE(imputation_status,'NONE') IN ('NONE','ORIGINAL','NOT_IMPUTED')
        AND COALESCE(quarantine_status,'NONE')='NONE'""")
    conn.commit()
    conn.close()


def run():
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "spatial.db")
        make_db(db)
        e = SpatialConsistencyEvidence(db, max_neighbors=2, time_tolerance_hours=3)

        normal = e.analyze("AWS_10_Gujarat", EVENT_NORMAL, 30.0, 1010.0, 60.0)
        assert normal["available"] is True
        assert normal["neighbor_count"] == 2
        assert normal["anomaly_candidate"] is False, normal

        isolated = e.analyze("AWS_10_Gujarat", EVENT_ISOLATED, 47.0, 1010.0, 60.0)
        assert isolated["available"] is True
        assert isolated["anomaly_candidate"] is True, isolated
        assert isolated["strongest_parameter"] == "temperature", isolated
        assert isolated["parameters"]["temperature"]["residual"] > 10

        # Simulate the same atmospheric +4C transition at the two nearby stations.
        # Spatial transfer should therefore track it instead of calling the target isolated.
        # NOTE: sqlite3.Connection implements a transaction context manager,
        # but `with sqlite3.connect(...) as conn:` does NOT close the connection
        # on exit. On Windows this leaves the temporary .db file locked until
        # the connection is garbage-collected, which can break TemporaryDirectory
        # cleanup (WinError 32), especially on Python 3.14.
        conn = sqlite3.connect(db)
        try:
            conn.execute("UPDATE sensor_readings SET temperature=34 WHERE station_name='AWS_10_Gujarat' AND event_time=?", (EVENT_COORDINATED,))
            conn.execute("UPDATE sensor_readings SET temperature=33 WHERE station_name='AWS_02_Mumbai' AND event_time=?", (EVENT_COORDINATED,))
            conn.execute("UPDATE sensor_readings SET temperature=36 WHERE station_name='AWS_08_Rajasthan' AND event_time=?", (EVENT_COORDINATED,))
            conn.commit()
        finally:
            conn.close()
        coordinated = e.analyze("AWS_10_Gujarat", EVENT_COORDINATED, 34.0, 1010.0, 60.0)
        assert coordinated["anomaly_candidate"] is False, coordinated

        missing = e.analyze("AWS_10_Gujarat", "2026-09-24 12:00:00", 30.0, 1010.0, 60.0)
        assert missing["available"] is False
        assert missing["anomaly_candidate"] is False

        # Pipeline integration: expose spatial evidence and allow Phase 3E
        # Decision Fusion to consume it as a sensor-fault evidence source.
        p = AnomalyPipeline(db)
        result = p.analyze_reading("AWS_10_Gujarat", 47.0, 1010.0, 60.0, event_time=EVENT_ISOLATED)
        layer = result["layers"]["spatial_evidence"]
        assert layer["available"] is True
        assert layer["anomaly_candidate"] is True
        assert any(item.get("source") == "spatial" for item in result["signals"]), result
        assert result["classification"] == "SENSOR_ANOMALY", result
        assert result["layers"]["decision_fusion"]["classification"] == "SENSOR_ANOMALY", result

    print("PHASE 3D SPATIAL TESTS: PASS")
    print("Normal spatial candidate: False")
    print("Isolated spatial anomaly: True")
    print("Missing spatial reference: False")
    print("Pipeline integration: PASS / fusion-connected")


if __name__ == "__main__":
    run()
