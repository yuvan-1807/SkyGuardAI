"""Phase 3G: event-time ordering, backfill, and no-future-leakage tests.

The integration step runs in a child Python process on Windows so any SQLite
handles created by imported SkyGuard modules are released before the parent
process removes the temporary test database.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


def init_min_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("""CREATE TABLE sensor_readings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_name TEXT NOT NULL,
            station_id TEXT,
            timestamp TEXT NOT NULL,
            event_time TEXT,
            receive_time TEXT,
            device_id TEXT,
            sequence INTEGER,
            message_id TEXT,
            temperature REAL,
            pressure REAL,
            humidity REAL,
            is_anomaly INTEGER DEFAULT 0,
            confidence REAL DEFAULT 0,
            detection_source TEXT,
            delivery_mode TEXT,
            secure_authenticated INTEGER DEFAULT 0,
            classification TEXT,
            source TEXT,
            model_version TEXT,
            anomaly_score REAL,
            is_clean INTEGER DEFAULT 1,
            validation_status TEXT DEFAULT 'VALIDATED',
            imputation_status TEXT DEFAULT 'NONE',
            quarantine_status TEXT DEFAULT 'NONE',
            out_of_order INTEGER DEFAULT 0
        )""")
        conn.execute("CREATE INDEX idx_sensor_event_order ON sensor_readings(station_id,event_time,id)")
        for i, ts in enumerate([
            "2023-01-01 10:00:00",
            "2023-01-01 10:01:00",
            "2023-01-01 10:02:00",
        ], 1):
            conn.execute("""INSERT INTO sensor_readings(
                station_name,station_id,timestamp,event_time,receive_time,temperature,pressure,humidity,
                is_anomaly,is_clean,validation_status,quarantine_status,out_of_order
            ) VALUES(?,?,?,?,?,?,?,?,0,1,'VALIDATED','NONE',0)""",
                ("AWS_01_Delhi","AWS_01_Delhi",ts,ts,ts,25.0+i,1012.0,60.0))
        conn.execute("""CREATE VIEW validated_observations AS
            SELECT id, station_name, station_id, event_time, receive_time, device_id, sequence, message_id,
                   temperature, pressure, humidity, NULL AS rainfall, 'NORMAL' AS classification,
                   'TEST' AS source, 'test' AS model_version, 0.0 AS anomaly_score,
                   is_clean, validation_status, imputation_status, quarantine_status, out_of_order
            FROM sensor_readings
            WHERE COALESCE(is_clean,0)=1 AND validation_status='VALIDATED'
              AND COALESCE(quarantine_status,'NONE')='NONE'
              AND COALESCE(imputation_status,'NONE') IN ('NONE','ORIGINAL','NOT_IMPUTED')
              AND temperature IS NOT NULL AND pressure IS NOT NULL AND humidity IS NOT NULL
        """)
        conn.commit()
    finally:
        conn.close()


def test_db_ordering(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("""INSERT INTO sensor_readings(
            station_name,station_id,timestamp,event_time,receive_time,temperature,pressure,humidity,
            is_anomaly,is_clean,validation_status,quarantine_status,out_of_order
        ) VALUES(?,?,?,?,?,?,?,?,0,1,'VALIDATED','NONE',1)""",
            ("AWS_01_Delhi","AWS_01_Delhi","2023-01-01 10:10:00","2023-01-01 10:01:30","2023-01-01 10:10:00",99.0,1012.0,60.0))
        conn.commit()
        latest_by_event = conn.execute(
            "SELECT event_time FROM sensor_readings ORDER BY event_time DESC,id DESC LIMIT 1"
        ).fetchone()[0]
        late_count = conn.execute(
            "SELECT COUNT(*) FROM sensor_readings WHERE out_of_order=1"
        ).fetchone()[0]
        assert latest_by_event == "2023-01-01 10:02:00"
        assert late_count == 1
    finally:
        conn.close()


def test_pipeline_context_uses_prior_event_only(path: Path) -> None:
    # Run the import/integration in a subprocess. This guarantees every SQLite
    # handle owned by imported dependencies is released before parent cleanup.
    code = r'''
import os
from anomaly_pipeline import AnomalyPipeline
path = os.environ["SKYGUARD_DB_PATH"]
p = AnomalyPipeline(path)
result = p.analyze_reading(
    "AWS_01_Delhi", 26.5, 1011.8, 61.0,
    update_state=False, event_time="2023-01-01 10:01:30", out_of_order=True,
)
assert result["layers"]["temporal"].get("baseline_samples", 0) == 2, result["layers"]["temporal"]
assert result["event_time"] == "2023-01-01 10:01:30"
print("PIPELINE_CHILD_OK")
'''
    env = os.environ.copy()
    env["SKYGUARD_DB_PATH"] = str(path)
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            "Pipeline child test failed.\nSTDOUT:\n" + result.stdout +
            "\nSTDERR:\n" + result.stderr
        )
    assert "PIPELINE_CHILD_OK" in result.stdout


def cleanup_path(path: Path) -> None:
    # The child-process design should release all DB handles. Retry a few times
    # for antivirus/indexer timing on Windows.
    import shutil
    last_error = None
    for _ in range(8):
        try:
            shutil.rmtree(path)
            return
        except PermissionError as exc:
            last_error = exc
            import time
            time.sleep(0.15)
    if last_error:
        raise last_error


def run() -> None:
    td = Path(tempfile.mkdtemp(prefix="skyguard_phase3g_"))
    db = td / "phase3g.db"
    try:
        init_min_db(db)
        test_db_ordering(db)
        test_pipeline_context_uses_prior_event_only(db)
        conn = sqlite3.connect(db)
        try:
            late_rows = conn.execute(
                "SELECT COUNT(*) FROM sensor_readings WHERE out_of_order=1"
            ).fetchone()[0]
        finally:
            conn.close()
        print("Out-of-order rows:", late_rows)
        print("PHASE 3G EVENT-TIME TESTS: PASS")
        print("Event ordering by event_time: True")
        print("Late/backfill flagging: True")
        print("Future leakage excluded: True")
        print("Pipeline integration (component level): PASS")
    finally:
        cleanup_path(td)


if __name__ == "__main__":
    run()
