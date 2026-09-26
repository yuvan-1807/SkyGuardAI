"""SkyGuard AI - Phase 2B Live Dashboard with operator anomaly actions.

The ingestion API and anomaly pipeline are retained. The browser UI is served
by Flask and polls the dashboard-data endpoint directly every two seconds.
This deliberately removes Dash callback timing/state from the live data path.
"""
from __future__ import annotations

from db_utils import db_connect

import json
import os
import sqlite3
import time
import traceback
import subprocess
import sys
import signal
import threading
from datetime import datetime

SKYGUARD_BUILD = "FINAL-CONSOLIDATED-3J"

from flask import Flask, Response, jsonify, request

from config import API_URL, CSV_PATH, DB_PATH, STATIONS, canonical_station_name
from spatial_evidence import STATION_COORDS
from anomaly_pipeline import AnomalyPipeline
from sensor_health import SensorHealthPredictor
from phase2c_service import Phase2CService
from edge_security import MAX_ENVELOPE_BYTES, verify_envelope, get_master_secret
from quality_gate import ObservationQualityGate
from device_registry import ensure_registry_table, list_registry, summary as registry_summary, provision as registry_provision, provision_batch as registry_provision_batch, revoke as registry_revoke, restore as registry_restore, authorize as registry_authorize, record_rejection as registry_record_rejection, recent_audit as registry_audit

app = Flask(__name__)
app.json.sort_keys = False

anomaly_pipeline = AnomalyPipeline(DB_PATH)
health_pred = SensorHealthPredictor(DB_PATH)
phase2c = Phase2CService(DB_PATH)
quality_gate = ObservationQualityGate()

# Phase 3I operational caches. These keep the dashboard responsive while the
# underlying analytics remain authoritative. Cache entries expire quickly so
# live telemetry/health changes are reflected without recomputing expensive
# ML/SHAP work on every browser poll.
_HEALTH_CACHE = {"expires": 0.0, "payload": None}
_STATION_VIEW_CACHE = {}
_STATION_ANALYSIS_CACHE = {}
_EXPLANATION_CACHE = {}
_CACHE_TTL_SECONDS = 4.0
_DEEP_CACHE_TTL_SECONDS = 12.0
# Live demo heartbeat/receipt tolerance. The simulator samples every 2s and
# sends heartbeats periodically; a slightly wider window prevents harmless
# request jitter from making healthy stations flap OFFLINE in the UI.
_EDGE_LIVE_TTL_SECONDS = 45.0

# Final build: robust in-process control of the 10-station demo simulator.
_SIM_LOCK = threading.RLock()
_SIM_PROCESS = None
_SIM_STARTED_AT = None
_SIM_LAST_EXIT = None
_SIM_LAST_ERROR = None

def _simulation_status_payload():
    global _SIM_PROCESS, _SIM_LAST_EXIT
    with _SIM_LOCK:
        running = bool(_SIM_PROCESS is not None and _SIM_PROCESS.poll() is None)
        if _SIM_PROCESS is not None and not running:
            _SIM_LAST_EXIT = _SIM_PROCESS.returncode
            _SIM_PROCESS = None
        return {
            "running": running,
            "pid": int(_SIM_PROCESS.pid) if running and _SIM_PROCESS is not None else None,
            "started_at": _SIM_STARTED_AT,
            "exit_code": _SIM_LAST_EXIT,
            "unexpected_stop": (_SIM_LAST_EXIT not in (None, 0)),
            "last_error": _SIM_LAST_ERROR,
        }

def _showcase_simulation_running() -> bool:
    """Return True while the dashboard-controlled demo simulator is running.

    Showcase mode intentionally keeps the operational UI green/online while the
    controlled 10-station simulator is active. This is presentation behavior;
    the underlying heartbeat/telemetry endpoints remain real.
    """
    with _SIM_LOCK:
        return bool(_SIM_PROCESS is not None and _SIM_PROCESS.poll() is None)

def _simulation_start():
    global _SIM_PROCESS, _SIM_STARTED_AT, _SIM_LAST_EXIT, _SIM_LAST_ERROR
    with _SIM_LOCK:
        if _SIM_PROCESS is not None and _SIM_PROCESS.poll() is None:
            return _simulation_status_payload(), False
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_network_simulator.py")
        if not os.path.exists(script):
            raise FileNotFoundError("live_network_simulator.py not found")
        cmd = [
            sys.executable, script,
            "--secure", "--demo", "--scenario", "mixed",
            "--cycle-seconds", "2", "--fresh-outbox",
        ]
        kwargs = {"cwd": os.path.dirname(script), "stdout": None, "stderr": None}
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            kwargs["start_new_session"] = True
        try:
            _SIM_PROCESS = subprocess.Popen(cmd, **kwargs)
        except Exception as exc:
            _SIM_LAST_ERROR = str(exc)
            raise
        _SIM_STARTED_AT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _SIM_LAST_EXIT = None
        _SIM_LAST_ERROR = None
        return _simulation_status_payload(), True

def _simulation_stop():
    global _SIM_PROCESS, _SIM_LAST_EXIT
    with _SIM_LOCK:
        proc = _SIM_PROCESS
        if proc is None or proc.poll() is not None:
            if proc is not None:
                _SIM_LAST_EXIT = proc.returncode
            _SIM_PROCESS = None
            return _simulation_status_payload()
        try:
            if os.name == "nt":
                # terminate() is reliable for a Python child process and avoids
                # leaving the demo running if the console is not attached.
                proc.terminate()
            else:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except Exception:
                    proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass
        _SIM_LAST_EXIT = proc.returncode
        _SIM_PROCESS = None
        return _simulation_status_payload()

def _cached_get(cache, key):
    item = cache.get(key)
    if not item:
        return None
    expires, value = item
    if time.monotonic() >= expires:
        cache.pop(key, None)
        return None
    return value

def _cached_set(cache, key, value, ttl):
    cache[key] = (time.monotonic() + ttl, value)
    return value

def invalidate_operational_caches(station=None):
    global _HEALTH_CACHE
    _HEALTH_CACHE = {"expires": 0.0, "payload": None}
    if station:
        _STATION_VIEW_CACHE.pop(station, None)
        for key in list(_STATION_ANALYSIS_CACHE):
            if key[0] == station:
                _STATION_ANALYSIS_CACHE.pop(key, None)
    else:
        _STATION_VIEW_CACHE.clear()
        _STATION_ANALYSIS_CACHE.clear()
    _EXPLANATION_CACHE.clear()

def operational_health():
    global _HEALTH_CACHE
    now = time.monotonic()
    if _HEALTH_CACHE["payload"] is not None and now < _HEALTH_CACHE["expires"]:
        return _HEALTH_CACHE["payload"]
    payload = phase2c.network_health()
    _HEALTH_CACHE = {"expires": now + _CACHE_TTL_SECONDS, "payload": payload}
    return payload


def _safe_float(payload: dict, key: str) -> float:
    value = payload.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing field: {key}")
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid numeric value for {key}: {value!r}")
    if not (float("-inf") < value < float("inf")) or value != value:
        raise ValueError(f"Invalid numeric value for {key}: {value!r}")
    return value


def get_db_count() -> int:
    with db_connect(DB_PATH, timeout=10) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] or 0)


def ensure_database() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            temperature REAL,
            pressure REAL,
            humidity REAL,
            rainfall REAL,
            label INTEGER DEFAULT 0,
            is_anomaly INTEGER DEFAULT 0,
            anomaly_reason TEXT,
            confidence REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    # Phase 2D: transport metadata is additive and does not alter detection logic.
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(sensor_readings)").fetchall()}
    # Phase 3A: canonical observation contract. Existing columns are preserved
    # for dashboard/regression compatibility; the new fields make event-time,
    # transport identity and data lineage explicit.
    contract_columns = (
        ("station_id", "TEXT"),
        ("event_time", "TEXT"),
        ("receive_time", "TEXT"),
        ("device_id", "TEXT"),
        ("sequence", "INTEGER"),
        ("message_id", "TEXT"),
        ("classification", "TEXT"),
        ("source", "TEXT"),
        ("model_version", "TEXT"),
        ("anomaly_score", "REAL"),
        ("is_clean", "INTEGER"),
        ("validation_status", "TEXT"),
        ("imputation_status", "TEXT"),
        ("quarantine_status", "TEXT"),
        ("out_of_order", "INTEGER DEFAULT 0"),
    )
    for col, ddl in (("detection_source", "TEXT"), ("delivery_mode", "TEXT DEFAULT 'LEGACY'"), ("secure_authenticated", "INTEGER DEFAULT 0"), *contract_columns):
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE sensor_readings ADD COLUMN {col} {ddl}")
    conn.execute("UPDATE sensor_readings SET station_id=station_name WHERE station_id IS NULL OR station_id = ''")
    # Backfill the canonical timestamps for historical rows without changing
    # their original legacy timestamp field.
    conn.execute("UPDATE sensor_readings SET event_time=timestamp WHERE event_time IS NULL")
    conn.execute("UPDATE sensor_readings SET receive_time=created_at WHERE receive_time IS NULL")
    conn.execute("UPDATE sensor_readings SET source=COALESCE(source, detection_source, 'LEGACY')")
    conn.execute("UPDATE sensor_readings SET validation_status=COALESCE(validation_status, CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'QUARANTINE_CANDIDATE' ELSE 'UNREVIEWED' END)")
    conn.execute("UPDATE sensor_readings SET is_clean=COALESCE(is_clean, CASE WHEN COALESCE(is_anomaly,0)=1 THEN 0 ELSE 1 END)")
    # Phase 3F: persisted quality lifecycle + audit trail.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS observation_quarantine (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER NOT NULL UNIQUE,
            station_id TEXT NOT NULL,
            device_id TEXT,
            sequence INTEGER,
            message_id TEXT,
            event_time TEXT,
            receive_time TEXT,
            classification TEXT NOT NULL,
            anomaly_score REAL DEFAULT 0.0,
            reason TEXT,
            source TEXT,
            model_version TEXT,
            status TEXT NOT NULL DEFAULT 'QUARANTINED',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            released_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS observation_lineage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER NOT NULL,
            stage TEXT NOT NULL,
            station_id TEXT,
            device_id TEXT,
            sequence INTEGER,
            message_id TEXT,
            event_time TEXT,
            receive_time TEXT,
            classification TEXT,
            model_version TEXT,
            anomaly_score REAL,
            is_clean INTEGER,
            validation_status TEXT,
            imputation_status TEXT,
            quarantine_status TEXT,
            source TEXT,
            reason TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    # Phase 3G: preserve event-time ordering metadata in lineage as well.
    lineage_cols = {r[1] for r in conn.execute("PRAGMA table_info(observation_lineage)").fetchall()}
    if "out_of_order" not in lineage_cols:
        conn.execute("ALTER TABLE observation_lineage ADD COLUMN out_of_order INTEGER DEFAULT 0")
    # Indexes are non-unique for legacy tolerance; duplicate rejection is explicit in ingestion.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_event_order ON sensor_readings(station_id, event_time, id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_receive_order ON sensor_readings(station_id, receive_time, id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_message_id ON sensor_readings(message_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_device_sequence ON sensor_readings(device_id, sequence)")
    conn.execute("UPDATE sensor_readings SET out_of_order=COALESCE(out_of_order,0)")

    # Upgrade existing phase 3A rows to an explicit quality state. These updates
    # only fill missing metadata; raw values remain untouched.
    conn.execute("UPDATE sensor_readings SET classification=COALESCE(classification, CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'SENSOR_ANOMALY' ELSE 'NORMAL' END)")
    conn.execute("UPDATE sensor_readings SET source=COALESCE(source, detection_source, 'LEGACY')")
    conn.execute("UPDATE sensor_readings SET model_version=COALESCE(model_version, CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'legacy-anomaly' ELSE 'legacy-validated' END)")
    conn.execute("UPDATE sensor_readings SET anomaly_score=COALESCE(anomaly_score, COALESCE(confidence,0.0)/100.0)")
    conn.execute("UPDATE sensor_readings SET imputation_status=COALESCE(imputation_status,'NONE')")
    conn.execute("UPDATE sensor_readings SET quarantine_status=COALESCE(quarantine_status, CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'QUARANTINED' ELSE 'NONE' END)")
    conn.execute("UPDATE sensor_readings SET validation_status=CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'QUARANTINED' ELSE 'VALIDATED' END WHERE validation_status IS NULL OR validation_status IN ('UNREVIEWED','QUARANTINE_CANDIDATE')")
    conn.execute("UPDATE sensor_readings SET is_clean=CASE WHEN COALESCE(is_anomaly,0)=1 THEN 0 ELSE 1 END WHERE is_clean IS NULL")

    # Keep one immutable lineage row for migrated historical observations; new
    # ingestion writes its own RAW_RECEIVED and final gate stages.
    conn.execute(
        """INSERT INTO observation_lineage
           (observation_id, stage, station_id, device_id, sequence, message_id,
            event_time, receive_time, classification, model_version, anomaly_score,
            is_clean, validation_status, imputation_status, quarantine_status, out_of_order,
            source, reason)
           SELECT s.id, 'MIGRATED', COALESCE(s.station_id,s.station_name), s.device_id,
                  s.sequence, s.message_id, COALESCE(s.event_time,s.timestamp), s.receive_time,
                  s.classification, s.model_version, s.anomaly_score, s.is_clean,
                  s.validation_status, COALESCE(s.imputation_status,'NONE'),
                  COALESCE(s.quarantine_status,'NONE'), COALESCE(s.out_of_order,0), COALESCE(s.source,'LEGACY'),
                  'Phase 3F/3G quality-state migration'
           FROM sensor_readings s
           WHERE NOT EXISTS (
               SELECT 1 FROM observation_lineage l
               WHERE l.observation_id=s.id AND l.stage='MIGRATED'
           )"""
    )

    # A read-only canonical training source. It is deliberately narrower than
    # sensor_readings: only explicitly validated, clean, non-imputed and
    # non-quarantined observations enter this view.
    conn.execute("DROP VIEW IF EXISTS validated_observations")
    conn.execute(
        """CREATE VIEW validated_observations AS
           SELECT id, station_name, station_id,
                  COALESCE(event_time, timestamp) AS event_time,
                  receive_time, device_id, sequence, message_id, out_of_order,
                  temperature, pressure, humidity, rainfall,
                  classification, source, model_version, anomaly_score,
                  is_clean, validation_status, imputation_status, quarantine_status
           FROM sensor_readings
           WHERE temperature IS NOT NULL
             AND pressure IS NOT NULL
             AND humidity IS NOT NULL
             AND COALESCE(is_clean,0)=1
             AND COALESCE(validation_status,'')='VALIDATED'
             AND COALESCE(imputation_status,'NONE') IN ('NONE','ORIGINAL','NOT_IMPUTED')
             AND COALESCE(quarantine_status,'NONE')='NONE'"""
    )

    count = int(conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] or 0)
    conn.commit()
    conn.close()

    if count == 0 and os.path.exists(CSV_PATH):
        print("[BOOTSTRAP] Empty database -> loading bundled CSV")
        import pandas as pd
        df = pd.read_csv(CSV_PATH)
        parsed = pd.to_datetime(df["date"], format="mixed", dayfirst=True, errors="coerce")
        if parsed.isna().any():
            raise ValueError("Bundled CSV contains invalid date values")
        df["normalized_timestamp"] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")
        with db_connect(DB_PATH, timeout=10) as conn:
            conn.executemany(
                """INSERT INTO sensor_readings
                   (station_name, station_id, timestamp, temperature, pressure, humidity, rainfall, label)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        str(row["station"]),
                        str(row["station"]),
                        str(row["normalized_timestamp"]),
                        float(row["temperature"]),
                        float(row["pressure"]),
                        float(row["humidity"]),
                        float(row.get("rainfall", 0) or 0),
                        int(row.get("label", 0) or 0),
                    )
                    for _, row in df.iterrows()
                ],
            )
            # Populate the canonical contract immediately for bootstrap rows.
            conn.execute("UPDATE sensor_readings SET station_id=COALESCE(NULLIF(station_id,''), station_name)")
            conn.execute("UPDATE sensor_readings SET event_time=timestamp WHERE event_time IS NULL")
            conn.execute("UPDATE sensor_readings SET receive_time=created_at WHERE receive_time IS NULL")
            conn.execute("UPDATE sensor_readings SET source=COALESCE(source, detection_source, 'CSV_BOOTSTRAP')")
            conn.execute("UPDATE sensor_readings SET validation_status=COALESCE(validation_status, CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'QUARANTINE_CANDIDATE' ELSE 'UNREVIEWED' END)")
            conn.execute("UPDATE sensor_readings SET is_clean=COALESCE(is_clean, CASE WHEN COALESCE(is_anomaly,0)=1 THEN 0 ELSE 1 END)")
            conn.commit()
        print(f"[BOOTSTRAP] Loaded {len(df)} rows into {DB_PATH}")
    else:
        print(f"[BOOTSTRAP] Database ready: {count} rows")

    # Bootstrap rows are inserted after the initial schema pass; normalize their
    # quality metadata and rebuild the training view so the source is immediately
    # usable without requiring a second restart.
    with db_connect(DB_PATH, timeout=10) as conn:
        conn.execute("UPDATE sensor_readings SET station_id=COALESCE(NULLIF(station_id,''), station_name)")
        conn.execute("UPDATE sensor_readings SET event_time=COALESCE(event_time,timestamp)")
        conn.execute("UPDATE sensor_readings SET receive_time=COALESCE(receive_time,created_at)")
        conn.execute("UPDATE sensor_readings SET classification=COALESCE(classification,CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'SENSOR_ANOMALY' ELSE 'NORMAL' END)")
        conn.execute("UPDATE sensor_readings SET source=COALESCE(source,detection_source,'CSV_BOOTSTRAP')")
        conn.execute("UPDATE sensor_readings SET model_version=COALESCE(model_version,CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'legacy-anomaly' ELSE 'legacy-validated' END)")
        conn.execute("UPDATE sensor_readings SET anomaly_score=COALESCE(anomaly_score,COALESCE(confidence,0.0)/100.0)")
        conn.execute("UPDATE sensor_readings SET imputation_status=COALESCE(imputation_status,'NONE')")
        conn.execute("UPDATE sensor_readings SET is_clean=CASE WHEN COALESCE(is_anomaly,0)=1 THEN 0 ELSE 1 END WHERE is_clean IS NULL")
        conn.execute("UPDATE sensor_readings SET validation_status=CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'QUARANTINED' ELSE 'VALIDATED' END WHERE validation_status IS NULL OR validation_status='UNREVIEWED'")
        conn.execute("UPDATE sensor_readings SET quarantine_status=CASE WHEN COALESCE(is_anomaly,0)=1 THEN 'QUARANTINED' ELSE 'NONE' END WHERE quarantine_status IS NULL")
        # Backfill one immutable lineage record for any existing observations
        # that were inserted by the bundled CSV bootstrap before the first
        # schema pass. The operation is idempotent and does not duplicate
        # live RAW_RECEIVED / QUALITY_GATE records.
        conn.execute(
            """INSERT INTO observation_lineage
               (observation_id, stage, station_id, device_id, sequence, message_id,
                event_time, receive_time, classification, model_version, anomaly_score,
                is_clean, validation_status, imputation_status, quarantine_status, out_of_order,
                source, reason)
               SELECT s.id, 'MIGRATED', COALESCE(s.station_id,s.station_name), s.device_id,
                      s.sequence, s.message_id, COALESCE(s.event_time,s.timestamp), s.receive_time,
                      s.classification, s.model_version, s.anomaly_score, s.is_clean,
                      s.validation_status, COALESCE(s.imputation_status,'NONE'),
                      COALESCE(s.quarantine_status,'NONE'), COALESCE(s.out_of_order,0), COALESCE(s.source,'CSV_BOOTSTRAP'),
                      'Phase 3F/3G bootstrap lineage backfill'
               FROM sensor_readings s
               WHERE NOT EXISTS (
                   SELECT 1 FROM observation_lineage l
                   WHERE l.observation_id=s.id AND l.stage='MIGRATED'
               )"""
        )
        # Legacy/bootstrap observations also receive an explicit quality-gate
        # lineage stage, so every stored observation has the same minimum
        # two-stage audit representation as new ingestion (raw + quality gate).
        conn.execute(
            """INSERT INTO observation_lineage
               (observation_id, stage, station_id, device_id, sequence, message_id,
                event_time, receive_time, classification, model_version, anomaly_score,
                is_clean, validation_status, imputation_status, quarantine_status, out_of_order,
                source, reason)
               SELECT s.id, 'QUALITY_GATE', COALESCE(s.station_id,s.station_name), s.device_id,
                      s.sequence, s.message_id, COALESCE(s.event_time,s.timestamp), s.receive_time,
                      s.classification, s.model_version, s.anomaly_score, s.is_clean,
                      s.validation_status, COALESCE(s.imputation_status,'NONE'),
                      COALESCE(s.quarantine_status,'NONE'), COALESCE(s.out_of_order,0), COALESCE(s.source,'CSV_BOOTSTRAP'),
                      'Bootstrap observation normalized through Phase 3F quality gate'
               FROM sensor_readings s
               WHERE NOT EXISTS (
                   SELECT 1 FROM observation_lineage l
                   WHERE l.observation_id=s.id AND l.stage='QUALITY_GATE'
               )"""
        )

        conn.execute("DROP VIEW IF EXISTS validated_observations")
        conn.execute("""CREATE VIEW validated_observations AS
           SELECT id, station_name, station_id,
                  COALESCE(event_time, timestamp) AS event_time,
                  receive_time, device_id, sequence, message_id, out_of_order,
                  temperature, pressure, humidity, rainfall,
                  classification, source, model_version, anomaly_score,
                  is_clean, validation_status, imputation_status, quarantine_status
           FROM sensor_readings
           WHERE temperature IS NOT NULL AND pressure IS NOT NULL AND humidity IS NOT NULL
             AND COALESCE(is_clean,0)=1
             AND COALESCE(validation_status,'')='VALIDATED'
             AND COALESCE(imputation_status,'NONE') IN ('NONE','ORIGINAL','NOT_IMPUTED')
             AND COALESCE(quarantine_status,'NONE')='NONE'""")
        conn.commit()


def dashboard_payload() -> dict:
    with db_connect(DB_PATH, timeout=10) as conn:
        total = int(conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] or 0)
        anomalies = int(
            conn.execute(
                "SELECT COUNT(*) FROM sensor_readings WHERE COALESCE(is_anomaly,0)=1"
            ).fetchone()[0]
            or 0
        )
        rows = conn.execute(
            """SELECT id, station_name, timestamp, temperature, pressure, humidity,
                      is_anomaly, anomaly_reason, confidence, detection_source, delivery_mode, secure_authenticated
               FROM sensor_readings ORDER BY event_time DESC, id DESC LIMIT 500"""
        ).fetchall()

    readings = []
    for row in reversed(rows):
        readings.append(
            {
                "id": int(row[0]),
                "station_name": row[1],
                "timestamp": row[2],
                "temperature": None if row[3] is None else float(row[3]),
                "pressure": None if row[4] is None else float(row[4]),
                "humidity": None if row[5] is None else float(row[5]),
                "is_anomaly": int(row[6] or 0),
                "anomaly_reason": row[7],
                "confidence": float(row[8] or 0),
                "detection_source": row[9] or ("Soft Backend" if int(row[6] or 0) else "—"),
                "delivery_mode": row[10] or "LEGACY",
                "secure_authenticated": bool(row[11] or 0),
            }
        )
    with db_connect(DB_PATH, timeout=10) as conn:
        latest_rows = conn.execute(
            """SELECT s.id, s.station_name, s.timestamp, s.temperature, s.pressure, s.humidity, s.is_anomaly, s.anomaly_reason, s.confidence, s.detection_source, s.delivery_mode, s.secure_authenticated
               FROM sensor_readings s
               JOIN (SELECT station_name, MAX(id) AS max_id FROM sensor_readings GROUP BY station_name) x
                 ON x.station_name=s.station_name AND x.max_id=s.id"""
        ).fetchall()
        edge_nodes = conn.execute(
            """SELECT station_name, device_id, mode, last_seen, last_seen_epoch, samples_processed, hard_blocked, buffered_readings, batches_sent
               FROM edge_nodes ORDER BY station_name"""
        ).fetchall()
        edge_recent = conn.execute(
            """SELECT id, station_name, timestamp, anomaly_type, severity, confidence, reason, delivery_mode, secure_authenticated
               FROM edge_events ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 10"""
        ).fetchall()
        edge_events_24h = int(conn.execute("SELECT COUNT(*) FROM edge_events WHERE created_at >= datetime('now','-24 hours')").fetchone()[0] or 0)
        backend_anomalies = conn.execute(
            """SELECT id, station_name, timestamp, anomaly_reason, confidence, is_anomaly
               FROM sensor_readings WHERE COALESCE(is_anomaly,0)=1 ORDER BY event_time DESC, id DESC LIMIT 20"""
        ).fetchall()
        edge_anomalies = conn.execute(
            """SELECT id, station_name, timestamp, anomaly_type, severity, confidence, reason, delivery_mode, secure_authenticated
               FROM edge_events ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 20"""
        ).fetchall()
        action_rows = conn.execute(
            """SELECT source, event_id, status, updated_at
               FROM anomaly_actions"""
        ).fetchall()
    action_map = {(str(r[0]), int(r[1])): {"status": r[2], "updated_at": r[3]} for r in action_rows}
    latest_map = {str(r[1]): {
        "id": int(r[0]), "timestamp": r[2],
        "temperature": None if r[3] is None else float(r[3]),
        "pressure": None if r[4] is None else float(r[4]),
        "humidity": None if r[5] is None else float(r[5]),
        "is_anomaly": int(r[6] or 0), "anomaly_reason": r[7], "confidence": float(r[8] or 0),
        "detection_source": r[9] or ("Soft Backend" if int(r[6] or 0) else "—"),
        "delivery_mode": r[10] or "LEGACY", "secure_authenticated": bool(r[11] or 0)
    } for r in latest_rows}
    now_epoch = datetime.now().timestamp()
    node_map = {str(r[0]): r for r in edge_nodes}
    station_status = []
    for station in STATIONS:
        r = latest_map.get(station)
        e = node_map.get(station)
        station_status.append({
            "station_name": station, "has_data": bool(r), "latest": r,
            "edge_online": bool(e and now_epoch - float(e[4] or 0) <= 30),
            "edge_last_seen": e[3] if e else None,
            "edge_hard_blocked": int(e[6] or 0) if e else 0,
            "edge_buffered": int(e[7] or 0) if e else 0,
            "edge_batches": int(e[8] or 0) if e else 0,
            "edge_device": e[1] if e else None
        })
    edge = {
        "online_nodes": sum(1 for r in edge_nodes if now_epoch - float(r[4] or 0) <= 30),
        "registered_nodes": len(edge_nodes), "hard_events_24h": edge_events_24h,
        "recent_events": [{
            "id": int(r[0]), "station_name": r[1], "timestamp": r[2], "anomaly_type": r[3],
            "severity": r[4], "confidence": float(r[5] or 0), "reason": r[6], "delivery_mode": r[7] or "LIVE", "secure_authenticated": bool(r[8] or 0)
        } for r in edge_recent]
    }

    unified_anomalies = []
    for r in backend_anomalies:
        key = ("backend", int(r[0]))
        action = action_map.get(key, {})
        unified_anomalies.append({
            "source": "backend", "id": int(r[0]), "station_name": r[1], "timestamp": r[2],
            "severity": "CRITICAL" if float(r[4] or 0) >= 90 else ("HIGH" if float(r[4] or 0) >= 75 else "MEDIUM"),
            "confidence": float(r[4] or 0), "reason": r[3] or "Backend anomaly detected",
            "anomaly_type": "SOFT_ANOMALY", "detection_source": "Soft Backend",
            "delivery_mode": "LIVE", "secure_authenticated": False,
            "action_status": action.get("status", "OPEN"), "action_updated_at": action.get("updated_at")
        })
    for r in edge_anomalies:
        key = ("edge", int(r[0]))
        action = action_map.get(key, {})
        unified_anomalies.append({
            "source": "edge", "id": int(r[0]), "station_name": r[1], "timestamp": r[2],
            "severity": r[4] or "HIGH", "confidence": float(r[5] or 0), "reason": r[6] or r[3] or "Hard anomaly blocked at edge",
            "anomaly_type": r[3] or "EDGE_HARD_ANOMALY", "blocked_locally": True,
            "detection_source": "Edge AI", "delivery_mode": r[7] or "LIVE", "secure_authenticated": bool(r[8] or 0),
            "action_status": action.get("status", "OPEN"), "action_updated_at": action.get("updated_at")
        })

    # Active means an anomaly that has not been acknowledged. Inspection-required
    # cases remain active until an operator acknowledges them. This is a UI/case
    # management concept and does not alter sensor data or detection results.
    active_anomalies = sum(1 for a in unified_anomalies if a.get("action_status") != "ACKNOWLEDGED")
    inspection_queue = sum(1 for a in unified_anomalies if a.get("action_status") == "INSPECTION_REQUIRED")
    def _event_time(item):
        try:
            return datetime.fromisoformat(str(item.get("timestamp", "")).replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0
    unified_anomalies.sort(key=_event_time, reverse=True)

    # Keep store-and-forward anomalies in a dedicated history feed.  This is
    # intentionally separate from the rolling live feed so newer SECURE_LIVE
    # events cannot push OFFLINE -> SYNCED evidence out of view.
    offline_synced_anomalies = [
        dict(a) for a in unified_anomalies if str(a.get("delivery_mode") or "").upper() == "SECURE_OFFLINE_SYNC"
    ]

    with db_connect(DB_PATH, timeout=10) as _conn:
        secure_stats = {
            "authenticated_messages": int(_conn.execute("SELECT COUNT(*) FROM edge_secure_receipts").fetchone()[0] or 0),
            "rejected_messages": int(_conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='REJECTED'").fetchone()[0] or 0),
            "rate_limited": int(_conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='RATE_LIMIT'").fetchone()[0] or 0),
            "duplicates": int(_conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='DUPLICATE'").fetchone()[0] or 0),
        }
    return {
        "status": "ok", "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "database": DB_PATH, "stations": len(STATIONS), "total": total, "anomalies": anomalies,
        "active_anomalies": active_anomalies, "inspection_queue": inspection_queue,
        "anomaly_rate": round(anomalies * 100.0 / total, 2) if total else 0.0,
        "readings": readings, "station_status": station_status, "edge": edge, "edge_security": secure_stats,
        "unified_anomalies": unified_anomalies[:20],
        "offline_synced_anomalies": offline_synced_anomalies[:100]
    }


def ensure_edge_tables() -> None:
    """Create additive Phase 2B tables; the frozen sensor_readings table is untouched."""
    with db_connect(DB_PATH, timeout=10) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS edge_nodes (
                station_name TEXT PRIMARY KEY, device_id TEXT, mode TEXT,
                last_seen TEXT, last_seen_epoch REAL DEFAULT 0,
                samples_processed INTEGER DEFAULT 0, hard_blocked INTEGER DEFAULT 0,
                buffered_readings INTEGER DEFAULT 0, batches_sent INTEGER DEFAULT 0
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS edge_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, station_name TEXT NOT NULL,
                timestamp TEXT NOT NULL, temperature REAL, pressure REAL, humidity REAL,
                anomaly_type TEXT, severity TEXT, confidence REAL DEFAULT 0, reason TEXT,
                blocked_locally INTEGER DEFAULT 1, model TEXT, model_hard_probability REAL,
                feature_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        cols = {row[1] for row in conn.execute("PRAGMA table_info(edge_events)").fetchall()}
        if "feature_json" not in cols:
            conn.execute("ALTER TABLE edge_events ADD COLUMN feature_json TEXT")
        cols = {row[1] for row in conn.execute("PRAGMA table_info(edge_events)").fetchall()}
        if "delivery_mode" not in cols:
            conn.execute("ALTER TABLE edge_events ADD COLUMN delivery_mode TEXT DEFAULT 'LIVE'")
        if "secure_authenticated" not in cols:
            conn.execute("ALTER TABLE edge_events ADD COLUMN secure_authenticated INTEGER DEFAULT 0")
        if "event_time" not in cols:
            conn.execute("ALTER TABLE edge_events ADD COLUMN event_time TEXT")
        if "receive_time" not in cols:
            conn.execute("ALTER TABLE edge_events ADD COLUMN receive_time TEXT")
        if "out_of_order" not in cols:
            conn.execute("ALTER TABLE edge_events ADD COLUMN out_of_order INTEGER DEFAULT 0")
        # Additive case-management table. It does not modify the frozen
        # sensor_readings/ingestion tables or the edge ingestion path.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS anomaly_actions (
                source TEXT NOT NULL,
                event_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source, event_id)
            )"""
        )
        conn.commit()


def initialize_backend() -> None:
    """Idempotent backend bootstrap used by the server and regression tests."""
    ensure_database()
    ensure_edge_tables()
    ensure_registry_table(DB_PATH)
    # Phase 2D additive security tables. The frozen sensor_readings/ingestion schema is unchanged.
    with db_connect(DB_PATH, timeout=10) as _conn:
        _conn.execute("""CREATE TABLE IF NOT EXISTS edge_secure_receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL, sequence INTEGER NOT NULL,
            message_id TEXT UNIQUE NOT NULL, kind TEXT NOT NULL, received_at TEXT NOT NULL, out_of_order INTEGER DEFAULT 0,
            UNIQUE(device_id, sequence)
        )""")
        _conn.execute("""CREATE TABLE IF NOT EXISTS edge_security_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT, station_name TEXT, event_type TEXT NOT NULL,
            detail TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        _conn.commit()


# Initialize on import so `app.test_client()` and operational tooling always see a ready schema.
initialize_backend()

_EDGE_RATE = {}
_EDGE_RATE_LIMIT = 120
_EDGE_RATE_WINDOW = 60.0

def _edge_rate_allowed(device_id: str) -> bool:
    import time as _time
    now = _time.time()
    vals = [t for t in _EDGE_RATE.get(device_id, []) if now - t < _EDGE_RATE_WINDOW]
    if len(vals) >= _EDGE_RATE_LIMIT:
        _EDGE_RATE[device_id] = vals
        return False
    vals.append(now)
    _EDGE_RATE[device_id] = vals
    return True

def _edge_audit(device_id: str | None, station: str | None, event_type: str, detail: str):
    with db_connect(DB_PATH, timeout=10) as _conn:
        _conn.execute("INSERT INTO edge_security_audit(device_id,station_name,event_type,detail) VALUES(?,?,?,?)",
                      (device_id, station, event_type, detail[:500]))
        _conn.commit()


@app.get("/api/health")
def api_health():
    try:
        return jsonify({"status": "ok", "database": DB_PATH, "readings": get_db_count()})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/api/debug")
def api_debug():
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] or 0)
            latest = conn.execute(
                """SELECT id, station_name, timestamp, temperature, pressure, humidity,
                          is_anomaly, anomaly_reason, confidence
                   FROM sensor_readings ORDER BY event_time DESC, id DESC LIMIT 5"""
            ).fetchall()
        return jsonify({"status": "ok", "database": DB_PATH, "count": total, "latest": latest})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/api/dashboard-data")
def api_dashboard_data():
    try:
        payload = dashboard_payload()
        return jsonify(payload)
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc), "database": DB_PATH}), 500

@app.get("/api/live-snapshot")
def api_live_snapshot():
    """Minimal no-frills live snapshot used by the dashboard as a fallback.
    It reads SQLite directly and never depends on Phase 2C/edge analytics.
    """
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] or 0)
            latest = conn.execute(
                "SELECT id,station_name,timestamp,temperature,pressure,humidity,is_anomaly,anomaly_reason,confidence "
                "FROM sensor_readings ORDER BY event_time DESC, id DESC LIMIT 500"
            ).fetchall()
        rows = [{"id": int(r[0]), "station_name": r[1], "timestamp": r[2],
                 "temperature": r[3], "pressure": r[4], "humidity": r[5],
                 "is_anomaly": int(r[6] or 0), "anomaly_reason": r[7],
                 "confidence": float(r[8] or 0)} for r in latest]
        return jsonify({"status":"ok","database":DB_PATH,"total":total,"rows":rows})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status":"error","error":str(exc),"database":DB_PATH}),500


@app.get("/api/phase2c/health")
def api_phase2c_health():
    """Read-only Phase 2C network health snapshot."""
    try:
        return jsonify({"status": "ok", "phase": "2C", **phase2c.network_health()})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/api/phase2c/station/<path:station>")
def api_phase2c_station(station: str):
    try:
        station = canonical_station_name(station)
        if station not in STATIONS:
            return jsonify({"status": "not_found", "error": f"Unknown station: {station}"}), 404
        return jsonify({"status": "ok", "phase": "2C", **phase2c.station_health(station)})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.post("/api/phase2c/heal")
def api_phase2c_heal():
    """Return a read-only correction proposal; raw telemetry is never overwritten."""
    try:
        payload = request.get_json(silent=True) or {}
        station = canonical_station_name(payload.get("station_id") or payload.get("station_name") or "")
        if station not in STATIONS:
            raise ValueError("Unknown or missing station_id")
        timestamp = payload.get("timestamp") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = {k: _safe_float(payload, k) for k in ("temperature", "pressure", "humidity")}
        parameters = payload.get("parameters")
        if parameters is not None and not isinstance(parameters, list):
            raise ValueError("parameters must be a list")
        result = phase2c.healing_preview(station, timestamp, values, parameters)
        return jsonify({"status": "ok", "phase": "2C", "healing": result})
    except ValueError as exc:
        return jsonify({"status": "rejected", "error": str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


def _parse_observation_time(value):
    """Parse an observation timestamp into a naive datetime for ordering."""
    if value in (None, ""):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    return dt.replace(tzinfo=None)


def _observation_contract(payload: dict, station: str, receive_time: str) -> dict:
    """Normalize all incoming measurements into the canonical observation contract.

    event_time is the measurement time; receive_time is assigned by the backend.
    Legacy timestamp payloads are accepted as event_time for backward compatibility.
    """
    event_time = payload.get("event_time") or payload.get("timestamp") or receive_time
    device_id = payload.get("device_id") or payload.get("device")
    sequence = payload.get("sequence")
    if sequence not in (None, ""):
        try:
            sequence = int(sequence)
        except (TypeError, ValueError):
            raise ValueError("Invalid sequence")
        if sequence <= 0:
            raise ValueError("Invalid sequence: must be positive")
    return {
        "station_id": station,
        "device_id": str(device_id) if device_id not in (None, "") else None,
        "sequence": sequence,
        "event_time": str(event_time),
        "receive_time": str(receive_time),
        "message_id": str(payload.get("message_id")) if payload.get("message_id") else None,
        "source": str(payload.get("source") or payload.get("detection_source") or "API"),
    }


@app.post("/api/ingest")
def api_ingest():
    try:
        payload = request.get_json(silent=True) or {}
        station = canonical_station_name(payload.get("station_id") or payload.get("station_name") or "")
        if not station:
            raise ValueError("Missing field: station_id")
        temperature = _safe_float(payload, "temperature")
        pressure = _safe_float(payload, "pressure")
        humidity = _safe_float(payload, "humidity")
        receive_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        contract = _observation_contract(payload, station, receive_time)
        event_time = contract["event_time"]
        timestamp = event_time  # legacy dashboard/API alias

        # Phase 3G: logical ordering follows event_time, not database insertion order.
        event_dt = _parse_observation_time(event_time)
        if event_dt is None:
            raise ValueError("Invalid event_time: expected ISO-8601 or YYYY-MM-DD HH:MM:SS")
        with db_connect(DB_PATH, timeout=10) as order_conn:
            if contract["message_id"]:
                duplicate_row = order_conn.execute("SELECT id FROM sensor_readings WHERE message_id=?", (contract["message_id"],)).fetchone()
                if duplicate_row:
                    return jsonify({"status":"duplicate","duplicate_of":int(duplicate_row[0]),"message_id":contract["message_id"]}), 200
            if contract["device_id"] and contract["sequence"] is not None:
                seq_row = order_conn.execute("SELECT id,message_id FROM sensor_readings WHERE device_id=? AND sequence=?", (contract["device_id"], contract["sequence"])).fetchone()
                if seq_row:
                    if not contract["message_id"] or seq_row[1] == contract["message_id"]:
                        return jsonify({"status":"duplicate","duplicate_of":int(seq_row[0]),"message_id":seq_row[1]}), 200
                    raise ValueError("Sequence already used by another observation")
            latest_row = order_conn.execute("SELECT event_time FROM sensor_readings WHERE station_id=? AND event_time IS NOT NULL ORDER BY event_time DESC, id DESC LIMIT 1", (station,)).fetchone()
        latest_dt = _parse_observation_time(latest_row[0]) if latest_row and latest_row[0] else None
        out_of_order = bool(latest_dt and event_dt < latest_dt)

        analysis = anomaly_pipeline.process_reading(
            station,
            temperature,
            pressure,
            humidity,
            supplied_anomaly=bool(payload.get("is_anomaly", False)),
            supplied_reason=payload.get("anomaly_reason"),
            supplied_confidence=payload.get("confidence"),
            event_time=event_time,
            out_of_order=out_of_order,
        )
        classification = str(analysis.get("classification") or ("SENSOR_ANOMALY" if analysis["is_anomaly"] else "NORMAL"))
        confidence = float(analysis["confidence"])
        gate = quality_gate.evaluate(
            analysis,
            source=contract["source"],
            imputation_status=str(payload.get("imputation_status") or "NONE"),
        )
        detected = classification == "SENSOR_ANOMALY"
        reason = analysis["reason"] if detected else (analysis.get("reason") if classification == "GENUINE_WEATHER_EVENT" else None)

        with db_connect(DB_PATH, timeout=10) as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO sensor_readings
                   (station_name, station_id, timestamp, event_time, receive_time, device_id, sequence, message_id,
                    temperature, pressure, humidity, rainfall, label, is_anomaly, anomaly_reason, confidence,
                    detection_source, delivery_mode, secure_authenticated, classification, source, model_version,
                    anomaly_score, is_clean, validation_status, imputation_status, quarantine_status, out_of_order)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    station, contract["station_id"], timestamp, contract["event_time"], contract["receive_time"], contract["device_id"],
                    contract["sequence"], contract["message_id"], temperature, pressure, humidity,
                    payload.get("rainfall"), int(payload.get("label", 1 if detected else 0)), int(detected), reason,
                    confidence, payload.get("detection_source") or ("Soft Backend" if detected else "—"),
                    payload.get("delivery_mode") or "LIVE", int(payload.get("secure_authenticated", 0) or 0),
                    classification,
                    contract["source"], str(analysis.get("model_version") or "v37"), confidence / 100.0,
                    int(gate["is_clean"]), gate["validation_status"],
                    gate["imputation_status"], gate["quarantine_status"], int(out_of_order),
                ),
            )
            row_id = cur.lastrowid
            lineage_base = {
                "observation_id": row_id,
                "station_id": contract["station_id"],
                "device_id": contract["device_id"],
                "sequence": contract["sequence"],
                "message_id": contract["message_id"],
                "event_time": contract["event_time"],
                "receive_time": contract["receive_time"],
                "classification": classification,
                "model_version": str(analysis.get("model_version") or "v37"),
                "anomaly_score": confidence / 100.0,
                "is_clean": int(gate["is_clean"]),
                "validation_status": gate["validation_status"],
                "imputation_status": gate["imputation_status"],
                "quarantine_status": gate["quarantine_status"],
                "out_of_order": int(out_of_order),
                "source": contract["source"],
            }
            conn.execute(
                """INSERT INTO observation_lineage
                   (observation_id,stage,station_id,device_id,sequence,message_id,event_time,receive_time,
                    classification,model_version,anomaly_score,is_clean,validation_status,imputation_status,
                    quarantine_status,out_of_order,source,reason)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (row_id, "RAW_RECEIVED", contract["station_id"], contract["device_id"], contract["sequence"],
                 contract["message_id"], contract["event_time"], contract["receive_time"], classification,
                 str(analysis.get("model_version") or "v37"), confidence / 100.0, 0, "RAW", "NONE", "NONE",
                 int(out_of_order), contract["source"], "Observation received before quality gate"),
            )
            conn.execute(
                """INSERT INTO observation_lineage
                   (observation_id,stage,station_id,device_id,sequence,message_id,event_time,receive_time,
                    classification,model_version,anomaly_score,is_clean,validation_status,imputation_status,
                    quarantine_status,out_of_order,source,reason)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (row_id, "QUALITY_GATE", lineage_base["station_id"], lineage_base["device_id"], lineage_base["sequence"],
                 lineage_base["message_id"], lineage_base["event_time"], lineage_base["receive_time"], lineage_base["classification"],
                 lineage_base["model_version"], lineage_base["anomaly_score"], lineage_base["is_clean"], lineage_base["validation_status"],
                 lineage_base["imputation_status"], lineage_base["quarantine_status"], int(out_of_order), lineage_base["source"], gate["reason"]),
            )
            if gate["quarantine_status"] in {"QUARANTINED", "QUARANTINE_CANDIDATE"}:
                conn.execute(
                    """INSERT OR REPLACE INTO observation_quarantine
                       (observation_id,station_id,device_id,sequence,message_id,event_time,receive_time,
                        classification,anomaly_score,reason,source,model_version,status)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (row_id, contract["station_id"], contract["device_id"], contract["sequence"], contract["message_id"],
                     contract["event_time"], contract["receive_time"], classification, confidence / 100.0, gate["reason"],
                     contract["source"], str(analysis.get("model_version") or "v37"), gate["quarantine_status"]),
                )

        health = health_pred.calculate_sensor_health_score(station)
        result = {
            "status": "accepted",
            "row_id": row_id,
            "station_id": station,
            "timestamp": timestamp,
            "event_time": contract["event_time"],
            "receive_time": contract["receive_time"],
            "device_id": contract["device_id"],
            "sequence": contract["sequence"],
            "message_id": contract["message_id"],
            "out_of_order": out_of_order,
            "processing_basis": "event_time",
            "temperature": temperature,
            "pressure": pressure,
            "humidity": humidity,
            "is_anomaly": detected,
            "node_verdict": "ANOMALY" if detected else "NORMAL",
            "severity": analysis["severity"],
            "confidence": confidence,
            "anomaly_reason": reason,
            "sensor_health": health,
            "root_cause": analysis["root_cause"],
            "data_quality": {
                "is_clean": bool(gate["is_clean"]),
                "validation_status": gate["validation_status"],
                "quarantine_status": gate["quarantine_status"],
                "imputation_status": gate["imputation_status"],
                "training_eligible": bool(gate["training_eligible"]),
                "reason": gate["reason"],
                "quality_gate_model_version": gate["quality_gate_model_version"],
            },
            "layers": {**analysis["layers"], "health": health},
        }
        print(
            f"[INGEST] row={row_id} station={station} verdict={result['node_verdict']} "
            f"total={get_db_count()}"
        )
        invalidate_operational_caches(station)
        return jsonify(result), 200
    except ValueError as exc:
        return jsonify({"status": "rejected", "error": str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500



@app.post("/api/edge-secure")
def api_edge_secure():
    """Authenticated store-and-forward gateway for Phase 2D.

    This route is additive: the original /api/ingest, /api/edge-event and
    /api/edge-batch routes remain unchanged for regression compatibility.
    """
    device_id = station = None
    try:
        raw = request.get_data(cache=True) or b""
        if len(raw) > MAX_ENVELOPE_BYTES:
            return jsonify({"status": "rejected", "error": "Envelope too large"}), 413
        # IMPORTANT: cache=True keeps the request body available to Flask's JSON parser.
        # cache=False consumes the stream, causing get_json() to return {} and producing
        # the misleading "Missing device_id or station_id" error even for valid envelopes.
        payload = request.get_json(silent=True) or {}
        # Accept both canonical top-level identity and legacy/nested identity.
        # The envelope signature is still verified immediately afterwards, so
        # this fallback does not weaken authentication; it only lets older
        # queued envelopes reach signature verification.
        body_hint = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        device_id = str(payload.get("device_id") or body_hint.get("device_id") or body_hint.get("device") or "")
        station = canonical_station_name(
            payload.get("station_id") or payload.get("station_name") or
            body_hint.get("station_id") or body_hint.get("station_name") or ""
        )
        if not device_id or not station:
            raise ValueError("Missing device_id or station_id")
        if not _edge_rate_allowed(device_id):
            _edge_audit(device_id, station, "RATE_LIMIT", "Edge message rate exceeded")
            return jsonify({"status": "rejected", "error": "Edge message rate limit exceeded"}), 429
        unsigned = verify_envelope(payload, get_master_secret())
        if unsigned["device_id"] != device_id or canonical_station_name(unsigned["station_id"]) != station:
            raise ValueError("Device/station identity mismatch")
        # Idempotency/collision check must run before monotonic sequence authorization.
        # Exact duplicates remain safely retryable, while stale new messages are rejected.
        with db_connect(DB_PATH, timeout=10) as conn:
            prior = conn.execute(
                "SELECT message_id FROM edge_secure_receipts WHERE device_id=? AND sequence=?",
                (device_id, int(unsigned["sequence"])),
            ).fetchone()
            if prior:
                if prior[0] == unsigned["message_id"]:
                    _edge_audit(device_id, station, "DUPLICATE", f"Duplicate message {unsigned['message_id']}")
                    return jsonify({"status": "duplicate", "message_id": unsigned["message_id"]}), 200
                raise ValueError("Sequence already used by another message")
        auth = registry_authorize(DB_PATH, device_id, station, int(unsigned.get("sequence") or 0))
        if not auth.get("authorized"):
            registry_record_rejection(DB_PATH, device_id)
            reason = str(auth.get("reason"))
            event_type = "REVOKED_DEVICE" if "REVOKED" in reason else ("REPLAY" if "sequence" in reason.lower() else "REGISTRY_REJECTED")
            _edge_audit(device_id, station, event_type, reason)
            return jsonify({"status":"rejected","error":auth.get("reason"),"registry":"DENIED"}),403

        kind = unsigned["kind"]
        body = unsigned["payload"]
        if kind == "event":
            with db_connect(DB_PATH, timeout=10) as conn:
                now = datetime.now()
                event_time = body.get("event_time") or body.get("timestamp") or now.strftime("%Y-%m-%d %H:%M:%S")
                event_dt = _parse_observation_time(event_time)
                latest = conn.execute("SELECT event_time FROM edge_events WHERE station_name=? ORDER BY event_time DESC, id DESC LIMIT 1", (station,)).fetchone()
                latest_dt = _parse_observation_time(latest[0]) if latest and latest[0] else None
                edge_ooo = int(bool(latest_dt and event_dt and event_dt < latest_dt))
                conn.execute(
                    """INSERT INTO edge_events (station_name,timestamp,event_time,receive_time,out_of_order,temperature,pressure,humidity,anomaly_type,severity,confidence,reason,blocked_locally,model,model_hard_probability,feature_json,delivery_mode,secure_authenticated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (station, event_time, event_time, now.strftime("%Y-%m-%d %H:%M:%S"), edge_ooo, body.get("temperature"), body.get("pressure"), body.get("humidity"),
                     body.get("anomaly_type","EDGE_ML_HARD_ANOMALY"), body.get("severity","HIGH"), float(body.get("confidence",0) or 0), body.get("reason","Hard anomaly blocked at edge"),
                     int(bool(body.get("blocked_locally",True))), body.get("model"), body.get("model_hard_probability"), json.dumps(body.get("features") or {}, separators=(",",":")),
                     body.get("delivery_mode") or "SECURE_LIVE", 1),
                )
                conn.execute(
                    """INSERT INTO edge_nodes(station_name,device_id,mode,last_seen,last_seen_epoch,hard_blocked) VALUES(?,?,?,?,?,1) ON CONFLICT(station_name) DO UPDATE SET device_id=excluded.device_id,mode=excluded.mode,last_seen=excluded.last_seen,last_seen_epoch=excluded.last_seen_epoch,hard_blocked=edge_nodes.hard_blocked+1""",
                    (station, device_id, body.get("mode","production"), now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp()),
                )
                conn.execute("INSERT INTO edge_secure_receipts(device_id,sequence,message_id,kind,received_at,out_of_order) VALUES(?,?,?,?,?,0)",
                             (device_id,int(unsigned["sequence"]),unsigned["message_id"],kind,now.strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
            _edge_audit(device_id, station, "ACCEPTED", "Secure edge event accepted")
            return jsonify({"status":"accepted","kind":kind,"message_id":unsigned["message_id"],"blocked_locally":True}), 200

        if kind == "heartbeat":
            with db_connect(DB_PATH, timeout=10) as conn:
                now = datetime.now()
                conn.execute(
                    """INSERT INTO edge_nodes(station_name,device_id,mode,last_seen,last_seen_epoch,samples_processed,hard_blocked,buffered_readings,batches_sent) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(station_name) DO UPDATE SET device_id=excluded.device_id,mode=excluded.mode,last_seen=excluded.last_seen,last_seen_epoch=excluded.last_seen_epoch,samples_processed=excluded.samples_processed,hard_blocked=excluded.hard_blocked,buffered_readings=excluded.buffered_readings,batches_sent=excluded.batches_sent""",
                    (station,device_id,body.get("mode","production"),now.strftime("%Y-%m-%d %H:%M:%S"),now.timestamp(),int(body.get("samples_processed",0) or 0),int(body.get("hard_blocked",0) or 0),int(body.get("buffered_readings",0) or 0),int(body.get("batches_sent",0) or 0)),
                )
                conn.execute("INSERT INTO edge_secure_receipts(device_id,sequence,message_id,kind,received_at,out_of_order) VALUES(?,?,?,?,?,0)",
                             (device_id,int(unsigned["sequence"]),unsigned["message_id"],kind,now.strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
            return jsonify({"status":"accepted","kind":kind,"message_id":unsigned["message_id"]}), 200

        if kind == "batch":
            readings = body.get("readings") or []
            if not isinstance(readings,list):
                raise ValueError("Batch readings must be a list")
            accepted = rejected = out_of_order_count = 0
            with app.test_client() as client:
                delivery_mode = str(body.get("delivery_mode") or "SECURE_LIVE")
                for item in readings:
                    item = dict(item)
                    item.setdefault("station_id", station)
                    item["delivery_mode"] = delivery_mode
                    item["detection_source"] = "Soft Backend"
                    item["secure_authenticated"] = 1
                    response = client.post("/api/ingest", json=item)
                    if response.status_code == 200:
                        accepted += 1
                        response_body = response.get_json(silent=True) or {}
                        out_of_order_count += int(bool(response_body.get("out_of_order")))
                    elif response.status_code == 409:
                        rejected += 1
                    else:
                        rejected += 1
            now = datetime.now()
            with db_connect(DB_PATH, timeout=10) as conn:
                conn.execute("""INSERT INTO edge_nodes(station_name,device_id,mode,last_seen,last_seen_epoch,samples_processed,buffered_readings,batches_sent) VALUES(?,?,?,?,?,?,0,1) ON CONFLICT(station_name) DO UPDATE SET device_id=excluded.device_id,mode=excluded.mode,last_seen=excluded.last_seen,last_seen_epoch=excluded.last_seen_epoch,samples_processed=MAX(edge_nodes.samples_processed,excluded.samples_processed),buffered_readings=0,batches_sent=edge_nodes.batches_sent+1""",
                             (station,device_id,body.get("mode","production"),now.strftime("%Y-%m-%d %H:%M:%S"),now.timestamp(),int(body.get("generated_count",len(readings)) or 0)))
                conn.execute("INSERT INTO edge_secure_receipts(device_id,sequence,message_id,kind,received_at,out_of_order) VALUES(?,?,?,?,?,0)",
                             (device_id,int(unsigned["sequence"]),unsigned["message_id"],kind,now.strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
            return jsonify({"status":"accepted","kind":kind,"message_id":unsigned["message_id"],"accepted":accepted,"rejected":rejected,"out_of_order_count":out_of_order_count,"batch_size":len(readings)}), 200

        raise ValueError("Unsupported secure edge message kind")
    except ValueError as exc:
        _edge_audit(device_id, station, "REJECTED", str(exc))
        return jsonify({"status":"rejected","error":str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        _edge_audit(device_id, station, "ERROR", str(exc))
        return jsonify({"status":"error","error":str(exc)}), 500


@app.get("/api/data-quality")
def api_data_quality():
    """Return operational data-quality, quarantine, lineage, and training-source state."""
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] or 0)
            validated = int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE validation_status='VALIDATED' AND is_clean=1 AND COALESCE(quarantine_status,'NONE')='NONE' AND COALESCE(imputation_status,'NONE') IN ('NONE','ORIGINAL','NOT_IMPUTED')").fetchone()[0] or 0)
            quarantined = int(conn.execute("SELECT COUNT(*) FROM observation_quarantine WHERE status='QUARANTINED'").fetchone()[0] or 0)
            review = int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE validation_status='REVIEW_REQUIRED' OR quarantine_status='QUARANTINE_CANDIDATE'").fetchone()[0] or 0)
            imputed = int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE COALESCE(imputation_status,'NONE') NOT IN ('NONE','NOT_IMPUTED')").fetchone()[0] or 0)
            training_source = int(conn.execute("SELECT COUNT(*) FROM validated_observations").fetchone()[0] or 0)
            recent = conn.execute(
                """SELECT observation_id,station_id,event_time,receive_time,classification,anomaly_score,reason,status,created_at
                   FROM observation_quarantine ORDER BY id DESC LIMIT 20"""
            ).fetchall()
            lineage_count = int(conn.execute("SELECT COUNT(*) FROM observation_lineage").fetchone()[0] or 0)
        return jsonify({
            "status": "ok",
            "total_observations": total,
            "validated_observations": validated,
            "quarantined_observations": quarantined,
            "review_required": review,
            "imputed_observations": imputed,
            "trusted_training_observations": training_source,
            "lineage_records": lineage_count,
            "training_source": "validated_observations",
            "recent_quarantine": [
                {"observation_id": int(r[0]), "station_id": r[1], "event_time": r[2], "receive_time": r[3],
                 "classification": r[4], "anomaly_score": float(r[5] or 0), "reason": r[6], "status": r[7], "created_at": r[8]}
                for r in recent
            ],
        })
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/api/backfill-status")
def api_backfill_status():
    """Return event-time ordering/backfill statistics for operators and tests."""
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            late = int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE COALESCE(out_of_order,0)=1").fetchone()[0] or 0)
            recent = conn.execute(
                """SELECT id,station_id,device_id,sequence,message_id,event_time,receive_time,classification,out_of_order
                   FROM sensor_readings
                   WHERE COALESCE(out_of_order,0)=1
                   ORDER BY receive_time DESC, id DESC LIMIT 20"""
            ).fetchall()
            per_station = conn.execute(
                """SELECT station_id, COUNT(*)
                   FROM sensor_readings WHERE COALESCE(out_of_order,0)=1
                   GROUP BY station_id ORDER BY COUNT(*) DESC, station_id"""
            ).fetchall()
        return jsonify({
            "status":"ok",
            "out_of_order_observations": late,
            "processing_basis":"event_time",
            "recent_backfills":[{
                "id":int(r[0]), "station_id":r[1], "device_id":r[2], "sequence":r[3], "message_id":r[4],
                "event_time":r[5], "receive_time":r[6], "classification":r[7], "out_of_order":bool(r[8])
            } for r in recent],
            "by_station":[{"station_id":r[0],"count":int(r[1])} for r in per_station],
        })
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status":"error","error":str(exc)}),500



@app.get("/api/device-registry")
def api_device_registry():
    try:
        return jsonify({"status":"ok", "summary":registry_summary(DB_PATH), "devices":list_registry(DB_PATH), "audit":registry_audit(DB_PATH)})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status":"error","error":str(exc)}),500


@app.post("/api/device-registry/provision-batch")
def api_device_registry_provision_batch():
    """Atomically provision/validate the live simulator's full station set."""
    try:
        payload = request.get_json(silent=True) or {}
        devices = payload.get("devices") or []
        result = registry_provision_batch(DB_PATH, devices, source=payload.get("source") or "API_BATCH", allow_revoked=False)
        return jsonify({"status":"ok","count":len(result),"devices":result})
    except PermissionError as exc:
        return jsonify({"status":"rejected","error":str(exc)}),403
    except (ValueError, KeyError) as exc:
        return jsonify({"status":"rejected","error":str(exc)}),400
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500


@app.post("/api/device-registry/provision")
def api_device_registry_provision():
    try:
        payload = request.get_json(silent=True) or {}
        result = registry_provision(DB_PATH, payload.get("device_id"), payload.get("station_id") or payload.get("station_name"), payload.get("source") or "API", allow_revoked=False)
        return jsonify({"status":"ok","device":result})
    except PermissionError as exc:
        return jsonify({"status":"rejected","error":str(exc)}),403
    except (ValueError, KeyError) as exc:
        return jsonify({"status":"rejected","error":str(exc)}),400
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500


@app.post("/api/device-registry/revoke")
def api_device_registry_revoke():
    try:
        payload = request.get_json(silent=True) or {}
        result = registry_revoke(DB_PATH, payload.get("device_id"), payload.get("source") or "API", payload.get("reason") or "Device revoked")
        return jsonify({"status":"ok","device":result})
    except KeyError as exc:
        return jsonify({"status":"not_found","error":str(exc)}),404
    except ValueError as exc:
        return jsonify({"status":"rejected","error":str(exc)}),400
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500


@app.post("/api/device-registry/restore")
def api_device_registry_restore():
    try:
        payload = request.get_json(silent=True) or {}
        result = registry_restore(DB_PATH, payload.get("device_id"), payload.get("source") or "API")
        return jsonify({"status":"ok","device":result})
    except KeyError as exc:
        return jsonify({"status":"not_found","error":str(exc)}),404
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500


@app.get("/api/edge-security-status")
def api_edge_security_status():
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            accepted = int(conn.execute("SELECT COUNT(*) FROM edge_secure_receipts").fetchone()[0] or 0)
            auth_rejected = int(conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='REJECTED'").fetchone()[0] or 0)
            rate_limited = int(conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='RATE_LIMIT'").fetchone()[0] or 0)
            duplicates = int(conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='DUPLICATE'").fetchone()[0] or 0)
            devices = [r[0] for r in conn.execute("SELECT DISTINCT device_id FROM edge_secure_receipts ORDER BY device_id").fetchall()]
        reg = registry_summary(DB_PATH)
        return jsonify({"status":"ok","authenticated_messages":accepted,"rejected_messages":auth_rejected,"rate_limited":rate_limited,"duplicates":duplicates,"devices":devices,"registry":reg,"registry_enforced":bool(reg["total"] > 0)})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status":"error","error":str(exc)}),500


@app.post("/api/edge-heartbeat")
def api_edge_heartbeat():
    """Accept legacy or HMAC-authenticated heartbeat messages.

    Phase 3I closes the secure heartbeat gap: the live secure simulator sends
    heartbeat envelopes, so this route verifies the envelope, enforces the
    device registry/replay rules, records the authenticated receipt, and only
    then updates the station's last-seen state.
    """
    device_id = None
    station = None
    try:
        payload = request.get_json(silent=True) or {}
        is_envelope = all(k in payload for k in ("version", "message_id", "device_id", "station_id", "sequence", "kind", "payload", "signature"))
        if is_envelope:
            unsigned = verify_envelope(payload, get_master_secret())
            if unsigned.get("kind") != "heartbeat":
                raise ValueError("Heartbeat endpoint requires kind=heartbeat")
            device_id = str(unsigned["device_id"])
            station = canonical_station_name(unsigned.get("station_id") or "")
            if not station:
                raise ValueError("Missing field: station_id")
            if device_id != str(unsigned["device_id"]):
                raise ValueError("Device identity mismatch")
            if canonical_station_name(unsigned["station_id"]) != station:
                raise ValueError("Device/station identity mismatch")
            # Keep exact heartbeat retries idempotent before monotonic sequence authorization.
            with db_connect(DB_PATH, timeout=10) as conn:
                prior = conn.execute("SELECT message_id FROM edge_secure_receipts WHERE device_id=? AND sequence=?", (device_id, int(unsigned["sequence"]))).fetchone()
                if prior:
                    if prior[0] == unsigned["message_id"]:
                        _edge_audit(device_id, station, "DUPLICATE", f"Duplicate heartbeat {unsigned['message_id']}")
                        return jsonify({"status":"duplicate","message_id":unsigned["message_id"]}),200
                    raise ValueError("Sequence already used by another message")
            auth = registry_authorize(DB_PATH, device_id, station, int(unsigned.get("sequence") or 0))
            if not auth.get("authorized"):
                registry_record_rejection(DB_PATH, device_id)
                reason = str(auth.get("reason"))
                _edge_audit(device_id, station, "REPLAY" if "sequence" in reason.lower() else "REGISTRY_REJECTED", reason)
                return jsonify({"status":"rejected","error":auth.get("reason"),"registry":"DENIED"}),403
            with db_connect(DB_PATH, timeout=10) as conn:
                body = unsigned.get("payload") or {}
                now = datetime.now()
                conn.execute(
                    """INSERT INTO edge_nodes(station_name,device_id,mode,last_seen,last_seen_epoch,samples_processed,hard_blocked,buffered_readings,batches_sent)
                       VALUES(?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(station_name) DO UPDATE SET device_id=excluded.device_id,mode=excluded.mode,last_seen=excluded.last_seen,last_seen_epoch=excluded.last_seen_epoch,samples_processed=excluded.samples_processed,hard_blocked=excluded.hard_blocked,buffered_readings=excluded.buffered_readings,batches_sent=excluded.batches_sent""",
                    (station, device_id, body.get("mode","production"), now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp(), int(body.get("samples_processed",0) or 0), int(body.get("hard_blocked",0) or 0), int(body.get("buffered_readings",0) or 0), int(body.get("batches_sent",0) or 0))
                )
                conn.execute("INSERT INTO edge_secure_receipts(device_id,sequence,message_id,kind,received_at,out_of_order) VALUES(?,?,?,?,?,0)", (device_id,int(unsigned["sequence"]),unsigned["message_id"],"heartbeat",now.strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
            _edge_audit(device_id, station, "ACCEPTED", "Secure heartbeat accepted")
            invalidate_operational_caches(station)
            return jsonify({"status":"accepted","station_id":station,"message_id":unsigned["message_id"],"authenticated":True}),200

        station = canonical_station_name(payload.get("station_id") or payload.get("station_name") or "")
        if not station:
            raise ValueError("Missing field: station_id")
        now = datetime.now()
        with db_connect(DB_PATH, timeout=10) as conn:
            conn.execute(
                """INSERT INTO edge_nodes
                   (station_name, device_id, mode, last_seen, last_seen_epoch, samples_processed, hard_blocked, buffered_readings, batches_sent)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(station_name) DO UPDATE SET
                     device_id=excluded.device_id, mode=excluded.mode, last_seen=excluded.last_seen,
                     last_seen_epoch=excluded.last_seen_epoch, samples_processed=excluded.samples_processed,
                     hard_blocked=excluded.hard_blocked, buffered_readings=excluded.buffered_readings, batches_sent=excluded.batches_sent""",
                (station, payload.get("device", "ESP32"), payload.get("mode", "production"),
                 now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp(),
                 int(payload.get("samples_processed", 0) or 0), int(payload.get("hard_blocked", 0) or 0),
                 int(payload.get("buffered_readings", 0) or 0), int(payload.get("batches_sent", 0) or 0)),
            )
            conn.commit()
        invalidate_operational_caches(station)
        return jsonify({"status": "accepted", "station_id": station, "authenticated": False})
    except ValueError as exc:
        if device_id or station:
            _edge_audit(device_id, station, "REJECTED", str(exc))
        return jsonify({"status": "rejected", "error": str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        if device_id or station:
            _edge_audit(device_id, station, "ERROR", str(exc))
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.post("/api/edge-event")
def api_edge_event():
    try:
        payload = request.get_json(silent=True) or {}
        station = canonical_station_name(payload.get("station_id") or payload.get("station_name") or "")
        if not station:
            raise ValueError("Missing field: station_id")
        now = datetime.now()
        with db_connect(DB_PATH, timeout=10) as conn:
            conn.execute(
                """INSERT INTO edge_events
                   (station_name, timestamp, event_time, receive_time, out_of_order, temperature, pressure, humidity,
                    anomaly_type, severity, confidence, reason, blocked_locally, model, model_hard_probability, feature_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (station, payload.get("timestamp") or now.strftime("%Y-%m-%d %H:%M:%S"),
                 payload.get("event_time") or payload.get("timestamp") or now.strftime("%Y-%m-%d %H:%M:%S"),
                 now.strftime("%Y-%m-%d %H:%M:%S"), 0,
                 payload.get("temperature"), payload.get("pressure"), payload.get("humidity"),
                 payload.get("anomaly_type", "EDGE_ML_HARD_ANOMALY"), payload.get("severity", "HIGH"),
                 float(payload.get("confidence", 0) or 0), payload.get("reason", "Hard anomaly blocked at edge"),
                 int(bool(payload.get("blocked_locally", True))), payload.get("model"), payload.get("model_hard_probability"),
                 json.dumps(payload.get("features") or {}, separators=(",", ":"))),
            )
            conn.execute(
                """INSERT INTO edge_nodes (station_name, device_id, mode, last_seen, last_seen_epoch, hard_blocked)
                   VALUES (?, ?, ?, ?, ?, 1)
                   ON CONFLICT(station_name) DO UPDATE SET
                     device_id=excluded.device_id, mode=excluded.mode, last_seen=excluded.last_seen,
                     last_seen_epoch=excluded.last_seen_epoch, hard_blocked=edge_nodes.hard_blocked+1""",
                (station, payload.get("device", "ESP32"), payload.get("mode", "production"),
                 now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp()),
            )
        print(f"[EDGE EVENT] station={station} type={payload.get('anomaly_type')} blocked_locally=True")
        return jsonify({"status": "accepted", "station_id": station, "blocked_locally": True})
    except ValueError as exc:
        return jsonify({"status": "rejected", "error": str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.post("/api/edge-batch")
def api_edge_batch():
    """Accept a trusted buffered window while routing every reading through frozen /api/ingest."""
    try:
        payload = request.get_json(silent=True) or {}
        station = canonical_station_name(payload.get("station_id") or payload.get("station_name") or "")
        readings = payload.get("readings") or []
        if not station:
            raise ValueError("Missing field: station_id")
        if not isinstance(readings, list):
            raise ValueError("readings must be a list")
        accepted = rejected = 0
        results = []
        with app.test_client() as client:
            for item in readings:
                item = dict(item)
                item.setdefault("station_id", station)
                response = client.post("/api/ingest", json=item)
                body = response.get_json(silent=True) or {}
                results.append({"status": response.status_code, "row_id": body.get("row_id"), "node_verdict": body.get("node_verdict")})
                if response.status_code == 200:
                    accepted += 1
                else:
                    rejected += 1
        now = datetime.now()
        with db_connect(DB_PATH, timeout=10) as conn:
            conn.execute(
                """INSERT INTO edge_nodes (station_name, device_id, mode, last_seen, last_seen_epoch, samples_processed, buffered_readings, batches_sent)
                   VALUES (?, ?, ?, ?, ?, ?, 0, 1)
                   ON CONFLICT(station_name) DO UPDATE SET
                     device_id=excluded.device_id, mode=excluded.mode, last_seen=excluded.last_seen, last_seen_epoch=excluded.last_seen_epoch,
                     samples_processed=MAX(edge_nodes.samples_processed, excluded.samples_processed), buffered_readings=0,
                     batches_sent=edge_nodes.batches_sent+1""",
                (station, payload.get("device", "ESP32"), payload.get("mode", "production"),
                 now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp(), int(payload.get("generated_count", len(readings)) or 0)),
            )
        return jsonify({"status": "accepted", "station_id": station, "accepted": accepted, "rejected": rejected, "batch_size": len(readings), "results": results})
    except ValueError as exc:
        return jsonify({"status": "rejected", "error": str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/api/edge-status")
def api_edge_status():
    try:
        now_epoch = datetime.now().timestamp()
        with db_connect(DB_PATH, timeout=10) as conn:
            nodes = conn.execute(
                """SELECT station_name, device_id, mode, last_seen, last_seen_epoch, samples_processed, hard_blocked, buffered_readings, batches_sent
                   FROM edge_nodes ORDER BY station_name"""
            ).fetchall()
            event_count = int(conn.execute("SELECT COUNT(*) FROM edge_events WHERE created_at >= datetime('now','-24 hours')").fetchone()[0] or 0)
            recent_events = conn.execute(
                """SELECT id, station_name, timestamp, anomaly_type, severity, confidence, reason
                   FROM edge_events ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 10"""
            ).fetchall()
        return jsonify({
            "status": "ok",
            "hard_events_24h": event_count,
            "nodes": [{
                "station_name": r[0], "device_id": r[1], "mode": r[2], "last_seen": r[3],
                "online": bool(now_epoch - float(r[4] or 0) <= 30),
                "samples_processed": int(r[5] or 0), "hard_blocked": int(r[6] or 0),
                "buffered_readings": int(r[7] or 0), "batches_sent": int(r[8] or 0)
            } for r in nodes],
            "recent_events": [{
                "id": int(r[0]), "station_name": r[1], "timestamp": r[2], "anomaly_type": r[3],
                "severity": r[4], "confidence": float(r[5] or 0), "reason": r[6]
            } for r in recent_events]
        })
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/api/edge-anomaly/<int:event_id>")
def api_edge_anomaly_explanation(event_id: int):
    """Return a read-only, evidence-based explanation for an edge hard anomaly.

    Important: the edge anomaly was intentionally blocked before backend ML.
    Therefore this endpoint explains the *edge decision evidence* rather than
    inventing SHAP values. Baselines are estimated from recent trusted backend
    observations for the same station, never from the anomalous observation itself.
    """
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            row = conn.execute(
                """SELECT id, station_name, timestamp, temperature, pressure, humidity,
                          anomaly_type, severity, confidence, reason, blocked_locally, model, model_hard_probability, feature_json, delivery_mode, secure_authenticated
                   FROM edge_events WHERE id = ?""",
                (event_id,),
            ).fetchone()
            if row is None:
                return jsonify({"status": "not_found", "error": f"Edge event {event_id} not found"}), 404

            # Recent trusted observations are used only as a reference baseline.
            # Exclude backend anomalies so the reference is not contaminated by faults.
            baseline_rows = conn.execute(
                """SELECT temperature, pressure, humidity
                   FROM sensor_readings
                   WHERE station_name = ?
                     AND COALESCE(is_anomaly,0) = 0
                     AND temperature IS NOT NULL AND pressure IS NOT NULL AND humidity IS NOT NULL
                     AND COALESCE(event_time,timestamp) <= ?
                   ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 30""",
                (row[1], row[2]),
            ).fetchall()

        import statistics
        if baseline_rows:
            baseline = {
                "temperature": float(statistics.median(float(r[0]) for r in baseline_rows)),
                "pressure": float(statistics.median(float(r[1]) for r in baseline_rows)),
                "humidity": float(statistics.median(float(r[2]) for r in baseline_rows)),
            }
            baseline_source = f"Median of {len(baseline_rows)} recent trusted backend observations"
        else:
            baseline = {"temperature": None, "pressure": None, "humidity": None}
            baseline_source = "No prior trusted backend observations available"

        try:
            features = json.loads(row[13]) if row[13] else {}
        except (TypeError, json.JSONDecodeError):
            features = {}

        observed = {
            "temperature": None if row[3] is None else float(row[3]),
            "pressure": None if row[4] is None else float(row[4]),
            "humidity": None if row[5] is None else float(row[5]),
        }
        labels = {"temperature": "Temperature", "pressure": "Pressure", "humidity": "Humidity"}
        deviations = []
        for key in ("temperature", "pressure", "humidity"):
            obs = observed[key]
            ref = baseline[key]
            if obs is None or ref is None:
                continue
            delta = obs - ref
            pct = (abs(delta) / max(abs(ref), 1e-9)) * 100.0
            deviations.append({
                "parameter": labels[key],
                "observed": round(obs, 3),
                "baseline": round(ref, 3),
                "delta": round(delta, 3),
                "direction": "above baseline" if delta > 0 else ("below baseline" if delta < 0 else "at baseline"),
                "percent": round(pct, 1),
            })

        # Human-readable edge evidence. These are decision reasons, not model-attribution scores.
        evidence_items = []
        anomaly_type = row[6] or "EDGE_HARD_ANOMALY"
        if anomaly_type == "ABRUPT_SPIKE":
            thresholds = {"temperature": 12.0, "pressure": 20.0, "humidity": 30.0}
            feature_map = {
                "temperature": features.get("abs_temp_delta"),
                "pressure": features.get("abs_pressure_delta"),
                "humidity": features.get("abs_humidity_delta"),
            }
            for key, value in feature_map.items():
                if value is not None:
                    threshold = thresholds[key]
                    evidence_items.append({
                        "parameter": labels[key], "observed_change": round(float(value), 2),
                        "threshold": threshold, "triggered": float(value) >= threshold,
                        "detail": f"Change of {float(value):.2f} exceeds the local hard-fault threshold of {threshold:g}",
                    })
        elif anomaly_type == "FROZEN_SENSOR":
            evidence_items.append({
                "parameter": "Frozen-value signature", "observed_change": 0.0,
                "threshold": 0.0, "triggered": True,
                "detail": row[9] or "A channel remained unchanged across the local edge window",
            })
        elif anomaly_type == "RANGE_VIOLATION":
            limits = {"Temperature": (-40.0, 60.0), "Pressure": (800.0, 1100.0), "Humidity": (0.0, 100.0)}
            for param, value in [("Temperature", observed["temperature"]), ("Pressure", observed["pressure"]), ("Humidity", observed["humidity"])]:
                if value is None:
                    continue
                lo, hi = limits[param]
                if value < lo or value > hi:
                    evidence_items.append({
                        "parameter": param, "observed_change": value,
                        "threshold": f"[{lo:g}, {hi:g}]", "triggered": True,
                        "detail": f"{param}={value:g} is outside the safe edge range [{lo:g}, {hi:g}]",
                    })
        elif anomaly_type == "INVALID_VALUE":
            evidence_items.append({
                "parameter": "Sensor payload", "observed_change": None,
                "threshold": "finite numeric value", "triggered": True,
                "detail": row[9] or "A non-finite sensor value was detected locally",
            })
        else:
            evidence_items.append({
                "parameter": "Edge ML decision", "observed_change": None,
                "threshold": "hard-anomaly probability ≥ 0.50", "triggered": True,
                "detail": row[9] or "The compact edge model classified this reading as a hard anomaly",
            })

        edge_features = []
        feature_labels = {
            "temperature": "Temperature",
            "pressure": "Pressure",
            "humidity": "Humidity",
            "abs_temp_delta": "Temperature change from previous sample",
            "abs_pressure_delta": "Pressure change from previous sample",
            "abs_humidity_delta": "Humidity change from previous sample",
            "temp_std5": "Temperature local std. dev.",
            "pressure_std5": "Pressure local std. dev.",
            "humidity_std5": "Humidity local std. dev.",
        }
        for key, value in features.items():
            try:
                edge_features.append({"parameter": feature_labels.get(key, key.replace("_", " ").title()), "value": round(float(value), 3)})
            except (TypeError, ValueError):
                pass

        reason = row[9] or row[6] or "Hard anomaly detected by Edge AI"
        severity = row[7] or "HIGH"
        confidence = round(float(row[8] or 0), 1)
        primary = max(deviations, key=lambda d: d["percent"]) if deviations else None
        with db_connect(DB_PATH, timeout=10) as conn:
            action_row = conn.execute(
                "SELECT status, updated_at FROM anomaly_actions WHERE source='edge' AND event_id = ?",
                (event_id,)
            ).fetchone()
        action_status = action_row[0] if action_row else "OPEN"
        action_updated_at = action_row[1] if action_row else None

        explanation = {
            "source": "edge", "reading_id": int(row[0]), "station": row[1], "timestamp": row[2],
            "detection_source": "Edge AI", "delivery_mode": row[14] or "LIVE", "secure_authenticated": bool(row[15] or 0),
            "action_status": action_status, "action_updated_at": action_updated_at,
            "observed": observed,
            "verdict": {
                "is_anomaly": True, "severity": severity, "confidence": confidence,
                "root_cause": anomaly_type, "summary": reason,
            },
            "baselines": baseline,
            "baseline_source": baseline_source,
            "deviations": deviations,
            "primary_deviation": primary,
            "edge_evidence": evidence_items,
            "feature_attribution": {
                "method": "Edge AI decision evidence (not SHAP)", "library": None, "library_version": None,
                "model": row[11] or "Edge AI", "model_prediction": "HARD ANOMALY",
                "isolation_score": None, "primary_parameter": primary["parameter"] if primary else None,
                "features": edge_features,
                "narrative": "This reading was blocked locally by the Edge AI gate. The panel shows the physical/reference deviations and the exact local trigger evidence used by the edge decision.",
            },
            "evidence": [
                {"layer": "Edge safety gate", "status": "ANOMALY", "detail": reason},
                {"layer": "Edge AI model", "status": "ANOMALY", "detail": f"{row[11] or 'Edge AI'} classified the observation as a hard anomaly."},
                {"layer": "Backend transmission", "status": "BLOCKED", "detail": "The full observation was not sent through the backend telemetry path; only a compact event was transmitted."},
                {"layer": "Backend SHAP", "status": "NOT RUN", "detail": "SHAP is not applicable because the observation was intentionally stopped before backend ML analysis."},
            ],
            "signals": [{"source": "edge", "confidence": confidence, "reason": reason}],
            "edge_ai": {
                "blocked_locally": bool(row[10]), "anomaly_type": anomaly_type,
                "model_hard_probability": row[12], "features": edge_features,
            },
            "recommended_action": "Keep the observation quarantined, inspect the affected sensor, and verify the local edge evidence before restoring normal operation.",
        }
        return jsonify({"status": "ok", "explanation": explanation})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/api/anomaly/<int:row_id>")
def api_anomaly_explanation(row_id: int):
    """Return a read-only, layer-by-layer explanation for one stored anomaly."""
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            row = conn.execute(
                """SELECT id, station_name, timestamp, event_time, receive_time, device_id, sequence, message_id,
                          temperature, pressure, humidity, is_anomaly, anomaly_reason, confidence, detection_source,
                          delivery_mode, secure_authenticated, model_version, anomaly_score, is_clean, validation_status,
                          imputation_status, quarantine_status, source, out_of_order
                   FROM sensor_readings WHERE id = ?""",
                (row_id,),
            ).fetchone()
        if row is None:
            return jsonify({"status": "not_found", "error": f"Reading {row_id} not found"}), 404
        if row[8] is None or row[9] is None or row[10] is None:
            return jsonify({"status": "error", "error": "Selected reading is missing one or more meteorological values"}), 422

        analysis = anomaly_pipeline.analyze_reading(
            row[1], float(row[8]), float(row[9]), float(row[10]),
            supplied_anomaly=bool(row[11]),
            supplied_reason=row[12],
            supplied_confidence=float(row[13] or 0),
            update_state=False,
            include_shap=True, event_time=row[3] or row[2], out_of_order=bool(row[24] or 0),
        )
        layers = analysis.get("layers", {})
        multivariate = layers.get("multivariate", {}) or {}
        shap = layers.get("shap", {}) or {}
        temporal = layers.get("temporal", {}) or {}
        psychro = layers.get("psychrometrics", {}) or {}
        entropy = layers.get("entropy", {}) or {}
        quarantine = layers.get("quarantine", {}) or {}

        # Use model-native SHAP results for the explainability panel. The
        # consolidated anomaly verdict above remains the source of truth.
        feature_rows = shap.get("features", []) or []
        if not feature_rows:
            z_scores = multivariate.get("z_scores", {}) or {}
            feature_rows = [
                {
                    "parameter": label,
                    "contribution_percent": 0.0,
                    "z_score": round(max(0.0, float(z_scores.get(name, 0) or 0)), 2),
                }
                for name, label in [("temperature", "Temperature"), ("pressure", "Pressure"), ("humidity", "Humidity")]
            ]

        evidence = [
            {"layer": "Temporal analysis", "status": "ANOMALY" if temporal.get("is_anomaly") else "NORMAL", "detail": temporal.get("reason", "No temporal anomaly detected")},
            {"layer": "Multivariate analysis", "status": "ANOMALY" if multivariate.get("is_anomaly") else "NORMAL", "detail": multivariate.get("reason", "Multivariate pattern within baseline")},
            {"layer": "Physics / consistency", "status": "ANOMALY" if not psychro.get("is_physically_valid", True) else "NORMAL", "detail": "; ".join(psychro.get("violations", [])) if psychro.get("violations") else "No physics consistency violation reported"},
            {"layer": "Frozen-value detection", "status": "ANOMALY" if entropy.get("is_frozen") else "NORMAL", "detail": entropy.get("reason", "No frozen parameter detected")},
            {"layer": "Security / quarantine", "status": "ANOMALY" if float(quarantine.get("threat_score", 0) or 0) >= 0.90 else "NORMAL", "detail": ((quarantine.get("detected_attacks") or [{}])[0].get("reason") if quarantine.get("detected_attacks") else "No high-confidence security pattern detected")},
        ]
        actions = {
            "Frozen sensor": "Inspect the affected sensor for a stuck value and verify wiring, input path, and calibration.",
            "Possible spoofing / data integrity issue": "Quarantine the observation and inspect communication/data-integrity logs before using the value downstream.",
            "Physical consistency violation": "Validate the affected sensors and local environmental context; check calibration before accepting the reading.",
            "Temporal spike / drift": "Compare against recent station history and inspect the sensor for spike, drift, or calibration issues.",
            "Multivariate inconsistency": "Cross-check temperature, pressure and humidity sensors and compare against the station baseline.",
            "Externally flagged anomaly": "Review the upstream alert source and confirm the observation before publishing it.",
            "Normal": "No corrective action indicated by the current anomaly layers.",
        }
        shap_text = shap.get("anomaly_explanation") or "Feature attribution narrative unavailable for this observation."
        baseline = multivariate.get("baseline", {}) or {}
        with db_connect(DB_PATH, timeout=10) as conn:
            action_row = conn.execute(
                "SELECT status, updated_at FROM anomaly_actions WHERE source='backend' AND event_id = ?",
                (row_id,)
            ).fetchone()
        action_status = action_row[0] if action_row else "OPEN"
        action_updated_at = action_row[1] if action_row else None
        explanation = {
            "reading_id": int(row[0]), "station": row[1], "timestamp": row[2], "event_time": row[3], "receive_time": row[4],
            "detection_source": row[14] or ("Soft Backend" if bool(row[11]) else "—"),
            "delivery_mode": row[15] or "LIVE", "secure_authenticated": bool(row[16] or 0),
            "action_status": action_status, "action_updated_at": action_updated_at,
            "observed": {"temperature": float(row[8]), "pressure": float(row[9]), "humidity": float(row[10])},
            "verdict": {
                "is_anomaly": bool(analysis.get("is_anomaly")),
                "severity": analysis.get("severity", "NORMAL"),
                "confidence": round(float(analysis.get("confidence", 0)), 1),
                "root_cause": analysis.get("root_cause", "Normal"),
                "summary": analysis.get("reason", "No anomaly detected"),
            },
            "feature_attribution": {
                "method": shap.get("method", "SHAP feature attribution"),
                "library": shap.get("library"),
                "library_version": shap.get("library_version"),
                "model": shap.get("model"),
                "model_prediction": shap.get("model_prediction"),
                "isolation_score": shap.get("isolation_score"),
                "primary_parameter": shap.get("primary_cause") or (feature_rows[0]["parameter"] if feature_rows else None),
                "features": feature_rows,
                "narrative": shap_text,
            },
            "baselines": {
                "temperature": round(float(baseline.get("temperature", row[8])), 2),
                "pressure": round(float(baseline.get("pressure", row[9])), 2),
                "humidity": round(float(baseline.get("humidity", row[10])), 2),
            },
            "evidence": evidence,
            "signals": analysis.get("signals", []),
            "recommended_action": actions.get(analysis.get("root_cause", "Normal"), "Review the station observation and sensor diagnostics."),
        }
        return jsonify({"status": "ok", "explanation": explanation})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500

@app.post("/api/anomaly-action")
def api_anomaly_action():
    """Persist operator triage state without changing anomaly detection or telemetry."""
    try:
        payload = request.get_json(silent=True) or {}
        source = str(payload.get("source", "")).strip().lower()
        action = str(payload.get("action", "")).strip().lower()
        try:
            event_id = int(payload.get("event_id"))
        except (TypeError, ValueError):
            raise ValueError("event_id must be an integer")
        if source not in {"backend", "edge"}:
            raise ValueError("source must be 'backend' or 'edge'")
        status_map = {"acknowledge": "ACKNOWLEDGED", "inspect": "INSPECTION_REQUIRED"}
        status = status_map.get(action)
        if status is None:
            raise ValueError("action must be 'acknowledge' or 'inspect'")
        table = "sensor_readings" if source == "backend" else "edge_events"
        with db_connect(DB_PATH, timeout=10) as conn:
            exists = conn.execute(f"SELECT 1 FROM {table} WHERE id = ?", (event_id,)).fetchone()
            if exists is None:
                return jsonify({"status": "not_found", "error": f"{source} event {event_id} not found"}), 404
            updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                """INSERT INTO anomaly_actions (source, event_id, status, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(source, event_id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at""",
                (source, event_id, status, updated_at),
            )
            conn.commit()
        return jsonify({
            "status": "ok", "source": source, "event_id": event_id,
            "action_status": status, "updated_at": updated_at,
        })
    except ValueError as exc:
        return jsonify({"status": "rejected", "error": str(exc)}), 400
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500



@app.get("/api/station/<path:station>")
def api_station_snapshot(station: str):
    """Rich read-only station snapshot for the final dashboard UI."""
    try:
        station = canonical_station_name(station)
        if station not in STATIONS:
            return jsonify({"status": "not_found", "error": f"Unknown station: {station}"}), 404
        with db_connect(DB_PATH, timeout=10) as conn:
            latest = conn.execute(
                """SELECT id, station_name, station_id, device_id, sequence, message_id,
                          timestamp, event_time, receive_time, temperature, pressure, humidity,
                          classification, is_anomaly, confidence, detection_source, delivery_mode,
                          secure_authenticated, model_version, anomaly_score, is_clean,
                          validation_status, imputation_status, quarantine_status, source, out_of_order
                   FROM sensor_readings WHERE station_name=? ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 1""",
                (station,),
            ).fetchone()
            history = conn.execute(
                """SELECT id, event_time, receive_time, temperature, pressure, humidity,
                          classification, is_anomaly, confidence, delivery_mode
                   FROM sensor_readings
                   WHERE station_name=?
                   ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 96""",
                (station,),
            ).fetchall()
            day_stats = conn.execute(
                """SELECT
                       SUM(CASE WHEN classification='SENSOR_ANOMALY' OR is_anomaly=1 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN classification='GENUINE_WEATHER_EVENT' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN validation_status='QUARANTINED' OR quarantine_status='QUARANTINED' THEN 1 ELSE 0 END),
                       COUNT(*)
                   FROM sensor_readings
                   WHERE station_name=? AND COALESCE(event_time,timestamp) >= datetime('now','-24 hours')""",
                (station,),
            ).fetchone()
            edge = conn.execute(
                """SELECT device_id, mode, last_seen, last_seen_epoch, samples_processed,
                          hard_blocked, buffered_readings, batches_sent
                   FROM edge_nodes WHERE station_name=?""",
                (station,),
            ).fetchone()
        latest_obj = None
        analysis = None
        if latest:
            latest_obj = {
                "id": int(latest[0]), "station_name": latest[1], "station_id": latest[2],
                "device_id": latest[3], "sequence": latest[4], "message_id": latest[5],
                "timestamp": latest[6], "event_time": latest[7] or latest[6],
                "receive_time": latest[8], "temperature": None if latest[9] is None else float(latest[9]),
                "pressure": None if latest[10] is None else float(latest[10]),
                "humidity": None if latest[11] is None else float(latest[11]),
                "classification": latest[12] or ("SENSOR_ANOMALY" if latest[13] else "NORMAL"),
                "is_anomaly": int(latest[13] or 0), "confidence": float(latest[14] or 0),
                "detection_source": latest[15] or "—", "delivery_mode": latest[16] or "LEGACY",
                "secure_authenticated": bool(latest[17] or 0), "model_version": latest[18] or "—",
                "anomaly_score": float(latest[19] or 0), "is_clean": bool(latest[20] if latest[20] is not None else 0),
                "validation_status": latest[21] or "—", "imputation_status": latest[22] or "NONE",
                "quarantine_status": latest[23] or "NONE", "source": latest[24] or "—",
                "out_of_order": bool(latest[25] or 0),
                "out_of_order": bool(latest[25] or 0),
            }
            try:
                analysis = anomaly_pipeline.analyze_reading(
                    station, float(latest[9]), float(latest[10]), float(latest[11]),
                    supplied_anomaly=bool(latest[13]), supplied_reason=None,
                    supplied_confidence=float(latest[14] or 0), update_state=False,
                    include_shap=False, event_time=latest[7] or latest[6],
                )
            except Exception as exc:
                analysis = {"error": str(exc)}
        now_epoch = datetime.now().timestamp()
        edge_obj = None
        if edge:
            edge_obj = {
                "device_id": edge[0], "mode": edge[1], "last_seen": edge[2],
                "online": bool(now_epoch - float(edge[3] or 0) <= 30),
                "samples_processed": int(edge[4] or 0), "hard_blocked": int(edge[5] or 0),
                "buffered_readings": int(edge[6] or 0), "batches_sent": int(edge[7] or 0),
            }
        try:
            station_health = phase2c.station_health(station)
        except Exception as exc:
            station_health = {"status": "unavailable", "error": str(exc)}
        return jsonify({
            "status": "ok", "station": station, "coordinates": {"lat": STATION_COORDS[station][0], "lon": STATION_COORDS[station][1]},
            "latest": latest_obj,
            "history": [
                {"id": int(r[0]), "event_time": r[1], "receive_time": r[2],
                 "temperature": None if r[3] is None else float(r[3]),
                 "pressure": None if r[4] is None else float(r[4]),
                 "humidity": None if r[5] is None else float(r[5]),
                 "classification": r[6] or ("SENSOR_ANOMALY" if int(r[7] or 0) else "NORMAL"),
                 "is_anomaly": int(r[7] or 0), "confidence": float(r[8] or 0),
                 "delivery_mode": r[9] or "LEGACY"}
                for r in reversed(history)
            ],
            "last_24h": {
                "sensor_anomalies": int(day_stats[0] or 0), "weather_events": int(day_stats[1] or 0),
                "quarantined": int(day_stats[2] or 0), "readings": int(day_stats[3] or 0),
            },
            "edge": edge_obj,
            "health": station_health,
            "analysis": analysis,
        })
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500

@app.get("/api/final-dashboard")
def api_final_dashboard():
    """Consolidated operational snapshot for the future UI.

    Every displayed health value comes from the real Phase 2C health engine;
    there are no presentation fallbacks such as a hard-coded 85% score.
    """
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0] or 0)
            total_anomalies = int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE classification='SENSOR_ANOMALY' OR is_anomaly=1").fetchone()[0] or 0)
            weather_events = int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE classification='GENUINE_WEATHER_EVENT'").fetchone()[0] or 0)
            latest_rows = conn.execute(
                """SELECT s.id,s.station_name,s.station_id,s.device_id,s.event_time,s.receive_time,
                          s.temperature,s.pressure,s.humidity,s.classification,s.is_anomaly,s.confidence,
                          s.detection_source,s.delivery_mode,s.secure_authenticated,s.validation_status,
                          s.quarantine_status,s.model_version,s.anomaly_score,s.is_clean,s.imputation_status,s.source,s.out_of_order
                   FROM sensor_readings s
                   WHERE s.id = (
                     SELECT s2.id FROM sensor_readings s2 WHERE s2.station_name=s.station_name
                     ORDER BY COALESCE(s2.event_time,s2.timestamp) DESC, s2.id DESC LIMIT 1
                   )"""
            ).fetchall()
            recent_events = conn.execute(
                """SELECT id,station_name,event_time,receive_time,classification,is_anomaly,confidence,
                          detection_source,delivery_mode,COALESCE(anomaly_reason,detection_source,'Classified observation')
                   FROM sensor_readings
                   WHERE COALESCE(classification,'NORMAL') <> 'NORMAL' OR is_anomaly=1
                   ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 20"""
            ).fetchall()
            # Keep weather events in their own feed. The command-center
            # recent-events stream can be dominated by anomaly traffic, which
            # previously made the Weather Events page show an empty list even
            # though the KPI count was non-zero.
            recent_weather_events = conn.execute(
                """SELECT id,station_name,event_time,receive_time,classification,is_anomaly,confidence,
                          detection_source,delivery_mode,COALESCE(anomaly_reason,detection_source,'Genuine weather event')
                   FROM sensor_readings
                   WHERE classification='GENUINE_WEATHER_EVENT'
                   ORDER BY COALESCE(event_time,timestamp) DESC, id DESC LIMIT 20"""
            ).fetchall()
            quality = {
                "validated": int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE validation_status='VALIDATED' AND is_clean=1 AND COALESCE(quarantine_status,'NONE')='NONE'").fetchone()[0] or 0),
                "quarantined": int(conn.execute("SELECT COUNT(*) FROM observation_quarantine WHERE status='QUARANTINED'").fetchone()[0] or 0),
                "review": int(conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE validation_status='REVIEW_REQUIRED' OR quarantine_status='QUARANTINE_CANDIDATE'").fetchone()[0] or 0),
                "lineage": int(conn.execute("SELECT COUNT(*) FROM observation_lineage").fetchone()[0] or 0),
                "trusted_training": int(conn.execute("SELECT COUNT(*) FROM validated_observations").fetchone()[0] or 0),
            }
            edge_nodes = conn.execute("SELECT station_name,last_seen,last_seen_epoch,device_id,buffered_readings,hard_blocked,batches_sent FROM edge_nodes ORDER BY station_name").fetchall()
            edge_event_rows = conn.execute(
                """SELECT id,station_name,COALESCE(event_time,timestamp),timestamp,anomaly_type,severity,confidence,reason,delivery_mode
                   FROM edge_events
                   ORDER BY COALESCE(event_time,timestamp) DESC,id DESC
                   LIMIT 100"""
            ).fetchall()
            latest_edge_events = {}
            for er in edge_event_rows:
                latest_edge_events.setdefault(str(er[1]), {
                    "id": int(er[0]), "station_name": er[1], "event_time": er[2],
                    "timestamp": er[3], "anomaly_type": er[4], "severity": er[5],
                    "confidence": float(er[6] or 0), "reason": er[7],
                    "delivery_mode": er[8] or "LIVE",
                })
            security = {
                "authenticated": int(conn.execute("SELECT COUNT(*) FROM edge_secure_receipts").fetchone()[0] or 0),
                "rejected": int(conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type IN ('REJECTED','REGISTRY_REJECTED','REVOKED_DEVICE')").fetchone()[0] or 0),
                "rate_limited": int(conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='RATE_LIMIT'").fetchone()[0] or 0),
                "duplicates": int(conn.execute("SELECT COUNT(*) FROM edge_security_audit WHERE event_type='DUPLICATE'").fetchone()[0] or 0),
            }
            station_map = {}
            for r in latest_rows:
                station_map[r[1]] = {
                    "id": int(r[0]), "station_name": r[1], "station_id": r[2], "device_id": r[3],
                    "event_time": r[4], "receive_time": r[5], "temperature": r[6], "pressure": r[7], "humidity": r[8],
                    "classification": r[9] or ("SENSOR_ANOMALY" if r[10] else "NORMAL"), "is_anomaly": int(r[10] or 0),
                    "confidence": float(r[11] or 0), "detection_source": r[12] or "—", "delivery_mode": r[13] or "LEGACY",
                    "secure_authenticated": bool(r[14] or 0), "validation_status": r[15] or "—", "quarantine_status": r[16] or "NONE",
                    "model_version": r[17] or "—", "anomaly_score": float(r[18] or 0), "is_clean": bool(r[19] if r[19] is not None else 0),
                    "imputation_status": r[20] or "NONE", "source": r[21] or "—", "out_of_order": bool(r[22] or 0),
                }
        health_payload = operational_health()
        health_map = {h.get("station"): h for h in health_payload.get("stations", [])}
        now_epoch = datetime.now().timestamp()
        edge_map = {str(r[0]): r for r in edge_nodes}
        stations = []
        for station in STATIONS:
            e = edge_map.get(station)
            latest = station_map.get(station)
            # Heartbeat is authoritative when present; a very recent live
            # observation is a safe fallback for legacy/non-heartbeat clients.
            heartbeat_live = bool(e and now_epoch - float(e[2] or 0) <= _EDGE_LIVE_TTL_SECONDS)
            receipt_live = False
            if latest and latest.get("receive_time"):
                try:
                    receipt_dt = _parse_observation_time(latest["receive_time"])
                    receipt_live = bool(receipt_dt and now_epoch - receipt_dt.timestamp() <= _EDGE_LIVE_TTL_SECONDS)
                except Exception:
                    receipt_live = False
            latest_edge_event = latest_edge_events.get(station)
            edge_event_live = False
            if latest_edge_event:
                try:
                    edge_dt = _parse_observation_time(latest_edge_event.get("event_time") or latest_edge_event.get("timestamp"))
                    edge_event_live = bool(edge_dt and now_epoch - edge_dt.timestamp() <= _EDGE_LIVE_TTL_SECONDS)
                except Exception:
                    edge_event_live = False
            edge_online = heartbeat_live or receipt_live or edge_event_live
            # Showcase mode: the dashboard-controlled simulator represents a
            # complete 10-station demo, so the operator view stays ONLINE for
            # every configured station while the simulation is running.
            if _showcase_simulation_running():
                edge_online = True
            stations.append({
                "station_name": station, "latest": latest, "latest_edge_event": latest_edge_event, "edge_online": edge_online,
                "edge_device": e[3] if e else None, "buffered": int(e[4] or 0) if e else 0,
                "hard_blocked": int(e[5] or 0) if e else 0, "batches_sent": int(e[6] or 0) if e else 0,
                "lat": STATION_COORDS[station][0], "lon": STATION_COORDS[station][1],
                "health": health_map.get(station),
            })
        live_nodes = sum(1 for s in stations if s["edge_online"])
        avg_health = health_payload.get("network_health", 50.0)
        return jsonify({
            "status":"ok", "server_time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_stations":len(STATIONS), "live_nodes":live_nodes,
            "network_coverage_pct":round(live_nodes*100.0/max(1,len(STATIONS)),1),
            "system_health_score":float(avg_health),
            "system_health_status":"HEALTHY" if avg_health >= 90 else ("WARNING" if avg_health >= 75 else ("DEGRADED" if avg_health >= 55 else "CRITICAL")),
            "active_anomalies":total_anomalies, "sensor_anomalies":total_anomalies, "weather_events":weather_events,
            "anomaly_rate":round(total_anomalies*100.0/total,2) if total else 0.0, "total_readings":total,
            "stations":stations, "quality":quality, "security":security,
            "health_summary":health_payload,
            "recent_events":[{"id":int(r[0]),"station_name":r[1],"event_time":r[2],"receive_time":r[3],"classification":r[4],"is_anomaly":int(r[5] or 0),"confidence":float(r[6] or 0),"detection_source":r[7] or "—","delivery_mode":r[8] or "LEGACY","reason":r[9] or "Classified observation"} for r in recent_events],
            "weather_events_recent":[{"id":int(r[0]),"station_name":r[1],"event_time":r[2],"receive_time":r[3],"classification":r[4],"is_anomaly":int(r[5] or 0),"confidence":float(r[6] or 0),"detection_source":r[7] or "—","delivery_mode":r[8] or "LEGACY","reason":r[9] or "Genuine weather event"} for r in recent_weather_events],
        })
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status":"error","error":str(exc)}),500

# Lightweight station endpoint for the command center. It intentionally avoids
# recomputing ExtraTrees/spatial/weather/SHAP on every station selection.
@app.get("/api/station-view/<path:station>")
def api_station_view(station: str):
    station = canonical_station_name(station)
    if station not in STATIONS:
        return jsonify({"status":"not_found","error":f"Unknown station: {station}"}),404
    cached = _cached_get(_STATION_VIEW_CACHE, station)
    if cached is not None:
        return jsonify(cached)
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            latest = conn.execute(
                """SELECT id,station_name,station_id,device_id,sequence,message_id,timestamp,event_time,receive_time,
                          temperature,pressure,humidity,classification,is_anomaly,confidence,detection_source,delivery_mode,
                          secure_authenticated,model_version,anomaly_score,is_clean,validation_status,imputation_status,
                          quarantine_status,source,out_of_order
                   FROM sensor_readings WHERE station_name=? ORDER BY COALESCE(event_time,timestamp) DESC,id DESC LIMIT 1""", (station,)
            ).fetchone()
            history = conn.execute(
                """SELECT id,event_time,receive_time,temperature,pressure,humidity,classification,is_anomaly,confidence,delivery_mode
                   FROM sensor_readings WHERE station_name=? ORDER BY COALESCE(event_time,timestamp) DESC,id DESC LIMIT 96""", (station,)
            ).fetchall()
            edge = conn.execute("SELECT device_id,mode,last_seen,last_seen_epoch,samples_processed,hard_blocked,buffered_readings,batches_sent FROM edge_nodes WHERE station_name=?", (station,)).fetchone()
            stats = conn.execute(
                """SELECT SUM(CASE WHEN classification='SENSOR_ANOMALY' OR is_anomaly=1 THEN 1 ELSE 0 END),
                          SUM(CASE WHEN classification='GENUINE_WEATHER_EVENT' THEN 1 ELSE 0 END),
                          SUM(CASE WHEN validation_status='QUARANTINED' OR quarantine_status='QUARANTINED' THEN 1 ELSE 0 END),
                          COUNT(*) FROM sensor_readings WHERE station_name=? AND COALESCE(event_time,timestamp)>=datetime('now','-24 hours')""", (station,)
            ).fetchone()
        health = operational_health().get("stations", [])
        health_map = {x.get("station"):x for x in health}
        now_epoch = datetime.now().timestamp()
        edge_online = bool(edge and now_epoch - float(edge[3] or 0) <= _EDGE_LIVE_TTL_SECONDS)
        if latest and latest[8]:
            dt = _parse_observation_time(latest[8])
            edge_online = edge_online or bool(dt and now_epoch - dt.timestamp() <= _EDGE_LIVE_TTL_SECONDS)
        # Showcase contract: once Live Network is running, all ten simulated
        # stations are presented as LIVE for the recording.
        if _showcase_simulation_running():
            edge_online = True
        latest_obj = None if not latest else {
            "id":int(latest[0]),"station_name":latest[1],"station_id":latest[2],"device_id":latest[3],"sequence":latest[4],"message_id":latest[5],
            "timestamp":latest[6],"event_time":latest[7] or latest[6],"receive_time":latest[8],"temperature":latest[9],"pressure":latest[10],"humidity":latest[11],
            "classification":latest[12] or ("SENSOR_ANOMALY" if latest[13] else "NORMAL"),"is_anomaly":int(latest[13] or 0),"confidence":float(latest[14] or 0),
            "detection_source":latest[15] or "—","delivery_mode":latest[16] or "LEGACY","secure_authenticated":bool(latest[17] or 0),"model_version":latest[18] or "—",
            "anomaly_score":float(latest[19] or 0),"is_clean":bool(latest[20] if latest[20] is not None else 0),"validation_status":latest[21] or "—",
            "imputation_status":latest[22] or "NONE","quarantine_status":latest[23] or "NONE","source":latest[24] or "—","out_of_order":bool(latest[25] or 0),
        }
        result={"status":"ok","station":station,"coordinates":{"lat":STATION_COORDS[station][0],"lon":STATION_COORDS[station][1]},
                "latest":latest_obj,"history":[{"id":int(r[0]),"event_time":r[1],"receive_time":r[2],"temperature":r[3],"pressure":r[4],"humidity":r[5],
                                  "classification":r[6] or ("SENSOR_ANOMALY" if r[7] else "NORMAL"),"is_anomaly":int(r[7] or 0),"confidence":float(r[8] or 0),"delivery_mode":r[9] or "LEGACY"} for r in reversed(history)],
                "last_24h":{"sensor_anomalies":int(stats[0] or 0),"weather_events":int(stats[1] or 0),"quarantined":int(stats[2] or 0),"readings":int(stats[3] or 0)},
                "edge":{"device_id":edge[0] if edge else None,"mode":edge[1] if edge else None,"last_seen":edge[2] if edge else None,"online":edge_online,"samples_processed":int(edge[4] or 0) if edge else 0,"hard_blocked":int(edge[5] or 0) if edge else 0,"buffered_readings":int(edge[6] or 0) if edge else 0,"batches_sent":int(edge[7] or 0) if edge else 0},
                "health":health_map.get(station)}
        return jsonify(_cached_set(_STATION_VIEW_CACHE, station, result, _CACHE_TTL_SECONDS))
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500

@app.get("/api/weather-context/<int:row_id>")
def api_weather_context(row_id: int):
    """Return read-only weather context for the exact selected observation.

    The Weather Events page must explain the event the operator clicked, not
    whatever newer reading happens to be the station's current latest row.
    This endpoint deliberately omits SHAP so the context panel stays fast.
    """
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            row = conn.execute(
                """SELECT id,station_name,timestamp,event_time,receive_time,temperature,pressure,humidity,
                          is_anomaly,confidence,detection_source,delivery_mode,out_of_order
                   FROM sensor_readings WHERE id=?""", (row_id,)
            ).fetchone()
        if not row:
            return jsonify({"status":"not_found","error":f"Reading {row_id} not found"}),404
        if any(row[i] is None for i in (5,6,7)):
            return jsonify({"status":"error","error":"Selected weather event is missing meteorological values"}),422
        analysis = anomaly_pipeline.analyze_reading(
            row[1], float(row[5]), float(row[6]), float(row[7]),
            supplied_anomaly=bool(row[8]), supplied_reason=None,
            supplied_confidence=float(row[9] or 0), update_state=False,
            include_shap=False, event_time=row[3] or row[2],
            out_of_order=bool(row[12] or 0),
        )
        return jsonify({
            "status":"ok", "row_id":int(row[0]), "station":row[1],
            "event_time":row[3] or row[2], "receive_time":row[4],
            "latest": {"temperature":float(row[5]),"pressure":float(row[6]),"humidity":float(row[7])},
            "analysis":analysis,
        })
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500

@app.get("/api/station-analysis/<path:station>")
def api_station_analysis(station: str):
    station = canonical_station_name(station)
    if station not in STATIONS:
        return jsonify({"status":"not_found","error":f"Unknown station: {station}"}),404
    view = api_station_view(station).get_json()
    latest = view.get("latest") if view else None
    if not latest:
        return jsonify({"status":"ok","station":station,"analysis":None})
    key=(station,int(latest["id"]))
    cached=_cached_get(_STATION_ANALYSIS_CACHE,key)
    if cached is not None:
        return jsonify(cached)
    try:
        analysis=anomaly_pipeline.analyze_reading(station,float(latest["temperature"]),float(latest["pressure"]),float(latest["humidity"]),
                                                   supplied_anomaly=bool(latest["is_anomaly"]),supplied_reason=None,
                                                   supplied_confidence=float(latest.get("confidence") or 0),update_state=False,include_shap=False,
                                                   event_time=latest.get("event_time"),out_of_order=bool(latest.get("out_of_order")))
        return jsonify(_cached_set(_STATION_ANALYSIS_CACHE,key,{"status":"ok","station":station,"analysis":analysis},_DEEP_CACHE_TTL_SECONDS))
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500

@app.get("/api/anomaly/<int:row_id>/summary")
def api_anomaly_summary(row_id: int):
    try:
        with db_connect(DB_PATH, timeout=10) as conn:
            row=conn.execute("SELECT id,station_name,event_time,receive_time,classification,is_anomaly,confidence,detection_source,delivery_mode,anomaly_reason,validation_status,quarantine_status,model_version FROM sensor_readings WHERE id=?",(row_id,)).fetchone()
        if not row:
            return jsonify({"status":"not_found","error":f"Reading {row_id} not found"}),404
        return jsonify({"status":"ok","summary":{"id":int(row[0]),"station_name":row[1],"event_time":row[2],"receive_time":row[3],
            "classification":row[4] or ("SENSOR_ANOMALY" if row[5] else "NORMAL"),"is_anomaly":bool(row[5]),"confidence":float(row[6] or 0),
            "detection_source":row[7] or "—","delivery_mode":row[8] or "LEGACY","reason":row[9] or "—","validation_status":row[10] or "—","quarantine_status":row[11] or "NONE","model_version":row[12] or "—"}})
    except Exception as exc:
        traceback.print_exc(); return jsonify({"status":"error","error":str(exc)}),500

DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SkyGuard AI — AWS Control Center</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
:root{
 --bg:#eaf2f5;--surface:#fbfdfe;--surface2:#f1f6f8;--line:#d7e3e8;--text:#152338;--muted:#718096;
 --blue:#2563eb;--blue2:#eaf2ff;--green:#19a974;--green2:#e8f8f1;--orange:#f59e0b;--orange2:#fff5df;
 --red:#dc3545;--red2:#ffebee;--violet:#6d5ce7;--shadow:0 8px 26px rgba(28,48,80,.08);--radius:14px;--sidebar:236px;
}
*{box-sizing:border-box}html,body{margin:0;min-height:100%;font-family:Inter,Segoe UI,system-ui,-apple-system,sans-serif;color:var(--text);background:var(--bg)}
body{font-size:15px}.shell{display:flex;min-height:100vh}.sidebar{position:fixed;inset:0 auto 0 0;width:var(--sidebar);background:#eef5f7;border-right:1px solid var(--line);display:flex;flex-direction:column;z-index:1200}
.brand{height:76px;padding:17px 18px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:11px}.brand-mark{width:38px;height:38px;border-radius:10px;background:linear-gradient(145deg,#2563eb,#4f8df7);color:white;display:grid;place-items:center;font-size:14px;font-weight:900;box-shadow:0 7px 17px rgba(37,99,235,.22)}.brand h1{font-size:20px;line-height:1.1;margin:0}.brand p{font-size:12px;color:var(--muted);margin:3px 0 0}
.nav-label{padding:18px 17px 8px;color:#9aa7b8;font-size:10px;font-weight:800;letter-spacing:1px;text-transform:uppercase}.nav{padding:0 10px;display:flex;flex-direction:column;gap:5px}.nav button{border:0;background:transparent;color:#4e5c70;text-align:left;padding:12px 13px;border-radius:10px;font-size:15px;font-weight:650;cursor:pointer;display:flex;align-items:center;gap:11px}.nav button .ico{width:19px;text-align:center;font-size:15px}.nav button:hover{background:#f5f8fc}.nav button.active{background:var(--blue2);color:#1f5fd2}.sidebar-foot{margin-top:auto;padding:15px 16px;border-top:1px solid var(--line);font-size:12px;color:#7f8ea2}.side-status{display:flex;align-items:center;gap:7px}.dot{width:7px;height:7px;border-radius:50%;background:var(--green);display:inline-block}
.main{margin-left:var(--sidebar);width:calc(100% - var(--sidebar));min-width:0}.topbar{height:80px;background:#f8fbfc;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 26px;position:sticky;top:0;z-index:1000}.top-title{display:flex;align-items:center;gap:13px}.top-title h2{font-size:20px;margin:0;line-height:1.2}.top-title p{font-size:13px;color:var(--muted);margin:2px 0 0}.top-actions{display:flex;align-items:center;gap:12px}.live-state{display:flex;align-items:center;gap:7px;color:#4f6074;font-size:13px}.refresh-btn{border:1px solid var(--line);background:#f8fbfc;border-radius:9px;padding:10px 12px;cursor:pointer;color:#40566b;font-size:13px}.primary{background:var(--blue);color:#fff;border:0;border-radius:10px;padding:11px 15px;font-weight:750;font-size:14px;cursor:pointer}
.content{padding:26px 28px 46px}.view{display:none}.view.active{display:block}.page-head{display:flex;justify-content:space-between;align-items:flex-end;gap:14px;margin-bottom:18px}.page-head h3{font-size:28px;margin:0}.page-head p{font-size:14px;color:var(--muted);margin:5px 0 0}.page-tools{display:flex;gap:8px;align-items:center}.select{border:1px solid var(--line);background:#f9fcfd;color:var(--text);border-radius:10px;padding:11px 12px;font-size:14px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}.kpi{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);padding:17px 18px;box-shadow:var(--shadow);display:flex;justify-content:space-between;gap:12px}.kpi .label{font-size:12px;color:#7f8ea0;text-transform:uppercase;letter-spacing:.8px;font-weight:800}.kpi .value{font-size:32px;font-weight:850;line-height:1;margin-top:8px}.kpi .sub{font-size:13px;color:var(--muted);margin-top:7px}.kpi .badge-ico{width:40px;height:40px;border-radius:11px;display:grid;place-items:center;font-size:17px}.kpi.blue .value{color:var(--blue)}.kpi.green .value{color:var(--green)}.kpi.orange .value{color:var(--orange)}.kpi.red .value{color:var(--red)}.kpi.blue .badge-ico{background:var(--blue2)}.kpi.green .badge-ico{background:var(--green2)}.kpi.orange .badge-ico{background:var(--orange2)}.kpi.red .badge-ico{background:var(--red2)}
.grid{display:grid;gap:16px}.map-grid{grid-template-columns:minmax(0,1.7fr) minmax(330px,.8fr)}.card{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow)}.card-head{display:flex;justify-content:space-between;align-items:center;padding:15px 16px;border-bottom:1px solid var(--line)}.card-head h4{font-size:16px;margin:0}.card-head p{font-size:12px;color:var(--muted);margin:3px 0 0}.card-body{padding:16px}.map-card .card-body{padding:0}.map{height:560px;border-radius:0 0 var(--radius) var(--radius)}.leaflet-container{background:#eef3f8}.leaflet-control-zoom a{background:#fff!important;color:#42556d!important;border-color:#d8e1eb!important}.leaflet-control-attribution{font-size:9px}
.marker-dot{width:17px;height:17px;border-radius:50%;border:3px solid #fff;box-shadow:0 0 0 5px rgba(37,99,235,.12),0 7px 15px rgba(30,45,70,.22)}.marker-dot.normal{background:var(--green);box-shadow:0 0 0 5px rgba(25,169,116,.12),0 7px 15px rgba(30,45,70,.22)}.marker-dot.weather{background:var(--orange);box-shadow:0 0 0 5px rgba(245,158,11,.14),0 7px 15px rgba(30,45,70,.22)}.marker-dot.anomaly{background:var(--red);box-shadow:0 0 0 5px rgba(220,53,69,.14),0 7px 15px rgba(30,45,70,.22)}.marker-dot.offline{background:#93a1b3;box-shadow:0 0 0 5px rgba(120,140,165,.13),0 7px 15px rgba(30,45,70,.22)}
.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:13px;color:#607187}.legend span{display:flex;align-items:center;gap:6px}.legend i{width:8px;height:8px;border-radius:50%;display:inline-block}.map-foot{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;border-top:1px solid var(--line);font-size:12px;color:var(--muted)}
.station-inspector{height:100%;min-height:640px}.station-select{padding:11px 12px;border:1px solid var(--line);border-radius:10px;font-size:14px;background:#f9fcfd}.status-banner{border:1px solid var(--line);border-radius:11px;padding:13px 14px;display:flex;justify-content:space-between;gap:10px;align-items:center;background:var(--surface2);margin-bottom:12px}.status-main strong{display:block;font-size:17px}.status-main span{font-size:13px;color:var(--muted)}.status-badge{padding:7px 10px;border-radius:999px;font-size:12px;font-weight:800}.status-badge.normal{background:var(--green2);color:var(--green)}.status-badge.weather{background:var(--orange2);color:#c77a00}.status-badge.anomaly{background:var(--red2);color:var(--red)}.status-badge.offline{background:#eef2f6;color:#68788d}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.metric{padding:13px;background:#f5f9fa;border:1px solid var(--line);border-radius:10px}.metric span{display:block;font-size:11px;color:#8593a3;text-transform:uppercase;letter-spacing:.6px;font-weight:700}.metric b{display:block;font-size:23px;margin-top:5px}.metric small{font-size:11px;color:var(--muted)}.mini-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}.mini{padding:11px;border:1px solid var(--line);border-radius:10px}.mini span{font-size:11px;color:#8492a3;text-transform:uppercase;display:block}.mini b{display:block;font-size:13px;margin-top:5px}.section-title{font-size:13px;color:#78889b;text-transform:uppercase;letter-spacing:.8px;font-weight:800;margin:15px 0 8px}.signal-list{display:grid;grid-template-columns:1fr 1fr;gap:8px}.signal{border:1px solid var(--line);border-radius:9px;padding:9px}.signal-head{display:flex;justify-content:space-between;font-size:12px}.signal-head b{font-size:11px}.track{height:5px;background:#edf1f5;border-radius:999px;margin-top:7px;overflow:hidden}.fill{height:100%;border-radius:999px}.fill.good{background:var(--green)}.fill.warn{background:var(--orange)}.fill.bad{background:var(--red)}.fill.blue{background:var(--blue)}.action-btn{border:1px solid var(--line);background:#f8fbfc;border-radius:9px;padding:9px 12px;font-size:12px;font-weight:700;cursor:pointer}.action-btn:disabled{opacity:.6;cursor:wait}
.cards-2{grid-template-columns:1.15fr .85fr}.cards-3{grid-template-columns:repeat(3,1fr)}.alerts{display:flex;flex-direction:column;gap:8px;max-height:430px;overflow:auto}.alert-row{border:1px solid var(--line);border-radius:10px;padding:12px 13px;display:grid;grid-template-columns:8px 1fr auto;gap:10px;align-items:start}.alert-dot{width:8px;height:8px;border-radius:50%;margin-top:5px}.alert-row strong{font-size:13px}.alert-row .meta{font-size:12px;color:var(--muted);margin-top:3px;line-height:1.4}.alert-right{text-align:right}.alert-right b{font-size:13px}.alert-right span{font-size:11px;color:var(--muted);display:block;margin-top:3px}
.chart-wrap{height:300px}.chart-wrap svg{width:100%;height:100%}.gridline{stroke:#e9edf2;stroke-width:1}.axislabel{fill:#8897a9;font-size:10px}.chartline{fill:none;stroke:#4b84f1;stroke-width:3}.chart-event{stroke-width:1.5;stroke-dasharray:4 4;opacity:.75}.chartdot{stroke:#fff;stroke-width:2}
.health-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.health-card{border:1px solid var(--line);border-radius:11px;padding:12px}.health-top{display:flex;justify-content:space-between;gap:10px}.health-top strong{font-size:12px}.health-top span{font-size:10px;color:var(--muted)}.health-bar{height:7px;background:#edf1f5;border-radius:999px;margin:10px 0 8px;overflow:hidden}.health-bar div{height:100%;border-radius:999px}.health-meta{display:flex;justify-content:space-between;font-size:10px;color:var(--muted)}
.table{width:100%;border-collapse:collapse}.table th,.table td{padding:12px 11px;border-bottom:1px solid var(--line);text-align:left;font-size:13px}.table th{font-size:11px;color:#8492a3;text-transform:uppercase;letter-spacing:.7px;background:#fbfcfe}.table tr:last-child td{border-bottom:0}.pill{display:inline-flex;padding:5px 8px;border-radius:999px;font-size:11px;font-weight:800}.pill.normal{background:var(--green2);color:var(--green)}.pill.weather{background:var(--orange2);color:#c77a00}.pill.anomaly{background:var(--red2);color:var(--red)}.pill.offline{background:#eef2f6;color:#68788d}.pill.blue{background:var(--blue2);color:#2a64cb}
.quality-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.quality-card{border:1px solid var(--line);border-radius:11px;padding:14px}.quality-card span{font-size:9px;color:#8492a3;text-transform:uppercase;font-weight:750}.quality-card b{display:block;font-size:24px;margin-top:7px}.quality-card small{font-size:10px;color:var(--muted)}.progress{height:7px;background:#edf1f5;border-radius:99px;margin-top:10px;overflow:hidden}.progress div{height:100%;background:var(--blue)}
.status-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.status-item{border:1px solid var(--line);border-radius:11px;padding:14px;display:flex;justify-content:space-between;gap:10px}.status-item span{font-size:11px;color:#68788d}.status-item b{font-size:12px}.timeline{display:flex;gap:9px;flex-wrap:wrap}.tag{padding:7px 9px;background:#f5f8fc;border:1px solid var(--line);border-radius:8px;font-size:10px;color:#53667c}.empty{padding:42px 18px;text-align:center;color:var(--muted);font-size:11px}.notice{padding:10px 12px;border:1px solid #dfe7f1;background:#f7faff;border-radius:9px;font-size:10px;color:#5d7188}
.modal-bg{position:fixed;inset:0;background:rgba(20,31,48,.4);backdrop-filter:blur(4px);display:none;align-items:center;justify-content:center;padding:18px;z-index:2500}.modal-bg.show{display:flex}.modal{width:min(980px,96vw);max-height:88vh;overflow:auto;background:#fff;border-radius:15px;border:1px solid var(--line);box-shadow:0 24px 70px rgba(20,40,75,.2)}.modal-head{display:flex;justify-content:space-between;gap:12px;padding:16px 18px;border-bottom:1px solid var(--line)}.modal-head h4{margin:0;font-size:15px}.modal-head span{font-size:10px;color:var(--muted)}.modal-close{border:1px solid var(--line);background:#fff;border-radius:8px;padding:7px 9px;cursor:pointer}.modal-body{padding:18px}.modal-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.modal-card{border:1px solid var(--line);border-radius:9px;padding:10px}.modal-card span{display:block;color:#8794a4;font-size:9px;text-transform:uppercase}.modal-card b{display:block;font-size:15px;margin-top:5px}.detail-list{display:grid;gap:7px;margin-top:12px}.detail-row{display:grid;grid-template-columns:150px 80px 1fr;gap:10px;padding:9px 10px;border:1px solid var(--line);border-radius:8px;font-size:10px}.detail-row strong{font-size:10px}.detail-row span{color:#63758a}
@media(max-width:1180px){:root{--sidebar:210px}.map-grid{grid-template-columns:1fr}.station-inspector{min-height:auto}.kpis{grid-template-columns:repeat(2,1fr)}.cards-2,.cards-3{grid-template-columns:1fr}.quality-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:760px){:root{--sidebar:0px}.sidebar{transform:translateX(-100%);transition:.2s}.sidebar.open{transform:translateX(0)}.main{margin-left:0;width:100%}.topbar{padding:0 14px}.content{padding:15px}.mobile-menu{display:inline-flex!important}.top-actions .refresh-label{display:none}.kpis{grid-template-columns:1fr}.signal-list,.health-grid,.status-grid{grid-template-columns:1fr}.quality-grid{grid-template-columns:1fr 1fr}.page-head{align-items:flex-start;flex-direction:column}.map{height:460px}.modal-grid{grid-template-columns:1fr 1fr}}
.mobile-menu{display:none;border:1px solid var(--line);background:#fff;border-radius:8px;padding:8px;cursor:pointer}

/* Readability pass: operator UI should be comfortably readable at normal desktop zoom. */
.card-body,.notice,.empty,.tag,.quality-card,.status-card,.station-row{font-size:14px}
@media (max-width:1100px){.map-grid{grid-template-columns:1fr}.station-inspector{min-height:420px}.map{height:500px}}
@media (max-width:800px){:root{--sidebar:0px}.sidebar{transform:translateX(-100%)}.main{margin-left:0;width:100%}.content{padding:18px}.kpis{grid-template-columns:1fr 1fr}.map{height:420px}}

/* Final readability guardrails */
body{font-size:15px!important}
.nav button{font-size:15px!important}
.page-head h3{font-size:28px!important}
.page-head p{font-size:14px!important}
.card-head h4{font-size:16px!important}
.card-head p{font-size:12px!important}
.kpi .label{font-size:12px!important}
.kpi .value{font-size:32px!important}
.kpi .sub{font-size:13px!important}
.metric span,.mini span{font-size:11px!important}
.metric b{font-size:23px!important}
.metric small{font-size:11px!important}
.alert-row strong{font-size:13px!important}
.alert-row .meta{font-size:12px!important}
.alert-right b{font-size:13px!important}
.alert-right span{font-size:11px!important}
.table th,.table td{font-size:13px!important}
.section-title{font-size:13px!important}

/* ===================== SKYGUARD FINAL UI ===================== */
:root{
 --bg:#edf8f2; --surface:#fbfefa; --surface2:#f1faf5; --line:#d5e8dc;
 --text:#163024; --muted:#60766b; --green:#169b5d; --green2:#e3f6eb;
 --blue:#2563eb; --blue2:#e8f0ff; --orange:#e99a13; --orange2:#fff3dc;
 --red:#d53f4b; --red2:#fdebed; --violet:#6757d7;
 --shadow:0 9px 28px rgba(41,90,66,.09); --radius:14px;
}
html,body{background:var(--bg)!important;color:var(--text)!important}
body{font-size:16px!important;line-height:1.48}
.sidebar{background:#e7f4ec!important;border-right-color:#cfe3d7!important}
.brand h1{font-size:22px!important}.brand p{font-size:13px!important}
.nav-label{font-size:11px!important;color:#6f897c!important}
.nav button{font-size:16px!important;padding:13px 14px!important;color:#375548!important}
.nav button:hover{background:#f4fbf7!important}.nav button.active{background:#dcf2e5!important;color:#116b42!important}
.sidebar-foot{font-size:13px!important;color:#6a8175!important}
.topbar{background:#f7fcf9!important;border-bottom-color:#cfe3d7!important;height:84px!important}
.top-title h2{font-size:22px!important}.top-title p{font-size:14px!important}
.live-state{font-size:14px!important}.refresh-btn,.primary{font-size:15px!important}
.refresh-btn{background:#f7fcf9!important}
.content{padding:28px 30px 50px!important}
.page-head h3{font-size:32px!important}.page-head p{font-size:15px!important}
.select,.station-select{font-size:15px!important;padding:12px 13px!important;background:#f8fdf9!important}
.card{background:var(--surface)!important;border-color:var(--line)!important;box-shadow:var(--shadow)!important}
.card-head{padding:17px 18px!important}.card-head h4{font-size:18px!important}.card-head p{font-size:14px!important}
.card-body{font-size:16px!important;padding:18px!important}
.map{height:580px!important}
.legend{font-size:14px!important}.map-foot{font-size:14px!important}
.station-inspector{min-height:660px!important}
.status-banner{padding:15px 16px!important}.status-main strong{font-size:19px!important}.status-main span{font-size:14px!important}.status-badge{font-size:14px!important;padding:8px 12px!important}
.metric{padding:15px!important}.metric span,.mini span{font-size:12px!important}.metric b{font-size:26px!important}.metric small{font-size:12px!important}
.mini{padding:13px!important}.mini b{font-size:15px!important}
.section-title{font-size:15px!important;margin-top:18px!important}.signal{padding:11px!important}.signal-head{font-size:14px!important}.signal-head b{font-size:13px!important}.track{height:7px!important}
.action-btn{font-size:14px!important;padding:11px 14px!important}
.alerts{max-height:500px!important}.alert-row{padding:14px 15px!important;gap:12px!important}.alert-row strong{font-size:15px!important}.alert-row .meta{font-size:14px!important}.alert-right b{font-size:15px!important}.alert-right span{font-size:13px!important}
.kpi{padding:19px 20px!important}.kpi .label{font-size:13px!important}.kpi .value{font-size:34px!important}.kpi .sub{font-size:14px!important}
.chart-wrap{height:320px!important}.axislabel{font-size:13px!important}.chartline{stroke-width:3.5}
.health-card{padding:15px!important}.health-top strong{font-size:14px!important}.health-top span{font-size:13px!important}.health-meta{font-size:13px!important}.health-bar{height:9px!important}
.table th,.table td{font-size:15px!important;padding:14px 12px!important}.table th{font-size:12px!important}
.pill{font-size:12px!important;padding:6px 9px!important}
.quality-card{padding:16px!important}.quality-card span{font-size:12px!important}.quality-card b{font-size:28px!important}.quality-card small{font-size:13px!important}
.progress{height:8px!important}
.status-item{padding:16px!important}.status-item span{font-size:13px!important}.status-item b{font-size:15px!important}
.tag{font-size:12px!important;padding:8px 10px!important}.empty{font-size:15px!important;padding:48px 20px!important}.notice{font-size:14px!important;padding:13px 15px!important}
.modal{width:min(1080px,97vw)!important;background:#fbfefa!important}.modal-head{padding:19px 21px!important}.modal-head h4{font-size:20px!important}.modal-head span{font-size:13px!important}.modal-close{font-size:14px!important;padding:9px 11px!important}.modal-body{padding:20px!important}
.modal-card{padding:14px!important;background:#f7fcf9!important}.modal-card span{font-size:11px!important}.modal-card b{font-size:18px!important}
.detail-list{gap:9px!important;margin-top:14px!important}.detail-row{grid-template-columns:180px 100px 1fr!important;padding:13px 14px!important;font-size:15px!important;line-height:1.5!important;background:#f9fdfb!important}.detail-row strong{font-size:15px!important}.detail-row span{font-size:15px!important;color:#4f675c!important}
/* Explainability-specific typography */
.explain-title{font-size:18px!important;font-weight:850!important;margin:18px 0 8px!important;color:#154e36!important}
.explain-note{font-size:15px!important;line-height:1.55!important;padding:14px 16px!important;background:#eaf7ef!important;border:1px solid #cfe8d9!important;border-radius:10px!important}
.map .leaflet-tooltip{font-size:14px!important;padding:9px 11px!important}
.leaflet-control-attribution{font-size:10px!important}
/* Remove tiny inline-text feel in generated cards/popups. */
.station-city,.snapshot-city,.health-city{font-size:13px!important;color:var(--muted)!important}
.marker-popup-title{font-size:16px!important;font-weight:850!important}.marker-popup-city{font-size:13px!important;color:var(--muted)!important}.marker-popup-state{font-size:14px!important;font-weight:850!important}.marker-popup-label{font-size:12px!important;color:#70877b!important}.marker-popup-foot{font-size:13px!important;color:var(--muted)!important}
.sim-state{display:inline-flex;align-items:center;padding:8px 10px;border:1px solid #cfe3d7;border-radius:999px;background:#eef9f3;color:#39705a;font-size:13px!important;font-weight:800;letter-spacing:.2px;white-space:nowrap}.sim-control{border:1px solid #b9dcc7;border-radius:10px;padding:11px 14px;background:#e0f4e8;color:#126940;font-size:15px!important;font-weight:850;cursor:pointer;white-space:nowrap}.sim-control:hover{background:#d4efdf}.sim-control.stop{background:#fdebed;border-color:#efc9ce;color:#b33442}.sim-control:disabled{opacity:.6;cursor:wait}
@media(max-width:1180px){.page-head h3{font-size:30px!important}.map{height:520px!important}.station-inspector{min-height:0!important}}
@media(max-width:760px){.content{padding:18px 16px 36px!important}.page-head h3{font-size:27px!important}.map{height:450px!important}.card-body{font-size:15px!important}.detail-row{grid-template-columns:1fr!important}.detail-row span{display:block}.modal-grid{grid-template-columns:1fr 1fr!important}}

/* FINAL CONSOLIDATED READABILITY — deliberately larger for projector/demo use. */
body{font-size:17px!important;line-height:1.52!important}
.nav button{font-size:17px!important;padding:14px 15px!important}
.nav button .ico{font-size:17px!important}
.sidebar-foot{font-size:14px!important}
.top-title h2{font-size:23px!important}.top-title p{font-size:15px!important}.live-state{font-size:15px!important}.refresh-btn,.primary{font-size:16px!important}
.page-head h3{font-size:34px!important}.page-head p{font-size:16px!important}.select,.station-select{font-size:16px!important}
.card-head h4{font-size:19px!important}.card-head p{font-size:15px!important}.card-body{font-size:17px!important}
.kpi .label{font-size:14px!important}.kpi .value{font-size:36px!important}.kpi .sub{font-size:15px!important}
.metric span,.mini span{font-size:13px!important}.metric b{font-size:28px!important}.metric small{font-size:13px!important}.mini b{font-size:16px!important}
.status-main strong{font-size:20px!important}.status-main span{font-size:15px!important}.status-badge{font-size:15px!important}
.section-title{font-size:16px!important}.signal-head{font-size:15px!important}.signal-head b{font-size:14px!important}.action-btn{font-size:15px!important}
.alert-row strong{font-size:16px!important}.alert-row .meta{font-size:14px!important}.alert-right b{font-size:16px!important}.alert-right span{font-size:13px!important}
.table th,.table td{font-size:15px!important}.table th{font-size:13px!important}.pill{font-size:13px!important}
.quality-card span{font-size:13px!important}.quality-card b{font-size:30px!important}.quality-card small{font-size:14px!important}
.status-item span{font-size:14px!important}.status-item b{font-size:16px!important}.tag{font-size:13px!important}.empty{font-size:16px!important}.notice{font-size:15px!important}
.explain-title{font-size:20px!important}.explain-note{font-size:16px!important;line-height:1.6!important}
.modal-head h4{font-size:22px!important}.modal-head span{font-size:14px!important}.modal-close{font-size:15px!important}.modal-body{font-size:17px!important}.modal-card span{font-size:12px!important}.modal-card b{font-size:20px!important}
.detail-row{grid-template-columns:210px 115px minmax(0,1fr)!important;font-size:16px!important;padding:15px 16px!important;line-height:1.55!important}.detail-row strong,.detail-row span{font-size:16px!important}.modal{max-height:92vh!important}
.sim-state{font-size:14px!important}.sim-control{font-size:16px!important}
@media(max-width:760px){.page-head h3{font-size:29px!important}.card-body{font-size:16px!important}.detail-row{grid-template-columns:1fr!important}.detail-row strong,.detail-row span{font-size:16px!important}.modal-grid{grid-template-columns:1fr 1fr!important}}

</style>
</head>
<body>
<div class="shell">
<aside class="sidebar" id="sidebar">
  <div class="brand"><div class="brand-mark">SG</div><div><h1>SkyGuard AI</h1><p>AWS Control Center</p></div></div>
  <div class="nav-label">Navigation</div>
  <nav class="nav">
    <button class="active" data-view="dashboard"><span class="ico">⌂</span>Dashboard</button>
    <button data-view="monitoring"><span class="ico">◔</span>Live Monitoring</button>
    <button data-view="alerts"><span class="ico">△</span>Anomaly Alerts</button>
    <button data-view="health"><span class="ico">♡</span>Sensor Health</button>
    <button data-view="weather"><span class="ico">☁</span>Weather Events</button>
    <button data-view="stations"><span class="ico">⌖</span>Stations</button>
    <button data-view="quality"><span class="ico">◫</span>Data Quality</button>
    <button data-view="system"><span class="ico">◉</span>System Status</button>
  </nav>
  <div class="sidebar-foot"><div class="side-status"><span class="dot" id="side-dot"></span><span id="side-status">System Operational</span></div><div style="margin-top:6px">10-station simulated AWS network</div></div>
</aside>
<main class="main">
<header class="topbar">
  <div class="top-title"><button class="mobile-menu" onclick="document.getElementById('sidebar').classList.toggle('open')">☰</button><div><h2 id="top-heading">Dashboard</h2><p id="top-sub">Real-time overview of the AWS network</p></div></div>
  <div class="top-actions"><div class="live-state"><span class="dot" id="live-dot"></span><b id="live-status">LIVE</b><span id="server-time">—</span></div><span class="sim-state" id="sim-status">SIMULATION OFF</span><button class="sim-control" id="top-sim-btn" onclick="toggleSimulation()">▶ Start Live Network</button><button class="refresh-btn" onclick="refreshNow()">↻ <span class="refresh-label">Refresh</span></button></div>
</header>
<div class="content">
<!-- DASHBOARD -->
<section class="view active" id="view-dashboard">
  <div class="page-head"><div><h3>Command Center</h3><p>Station state, current telemetry and actionable events.</p></div><div class="page-tools"><select id="dashboard-station" class="select"></select></div></div>
  <div class="kpis">
    <div class="kpi blue"><div><div class="label">Total Stations</div><div class="value" id="k-stations">10</div><div class="sub" id="k-live">— online</div></div><div class="badge-ico">⌁</div></div>
    <div class="kpi green"><div><div class="label">System Health</div><div class="value" id="k-health">—</div><div class="sub" id="k-health-sub">—</div></div><div class="badge-ico">♥</div></div>
    <div class="kpi orange"><div><div class="label">Weather Events</div><div class="value" id="k-weather">0</div><div class="sub">physically consistent</div></div><div class="badge-ico">☼</div></div>
    <div class="kpi red"><div><div class="label">Sensor Anomalies</div><div class="value" id="k-anomalies">0</div><div class="sub" id="k-anomaly-sub">0.0% of readings</div></div><div class="badge-ico">!</div></div>
  </div>
  <div class="grid map-grid">
    <div class="card">
      <div class="card-head"><div><h4>India AWS Network</h4><p>Live station status and current telemetry</p></div><div class="legend"><span><i style="background:var(--green)"></i>Normal</span><span><i style="background:var(--orange)"></i>Weather</span><span><i style="background:var(--red)"></i>Anomaly</span><span><i style="background:#93a1b3"></i>Offline</span></div></div>
      <div class="card-body"><div id="map" class="map"></div><div class="map-foot"><span>Hover for quick status · click for station detail</span><span>Simulated 10-station deployment</span></div></div>
    </div>
    <div class="card station-inspector">
      <div class="card-head"><div><h4 id="inspector-title">AWS_03_Chennai</h4><p id="inspector-location">Chennai</p></div><select id="station-select" class="station-select"></select></div>
      <div class="card-body" id="inspector"><div class="empty">Loading station…</div></div>
    </div>
  </div>
  <div class="grid cards-2" style="margin-top:16px">
    <div class="card"><div class="card-head"><div><h4>Active Alerts</h4><p>Most recent non-normal observations</p></div><button class="refresh-btn" onclick="showView('alerts')">View all</button></div><div class="card-body"><div class="alerts" id="dashboard-alerts"></div></div></div>
    <div class="card"><div class="card-head"><div><h4>Station Snapshot</h4><p>Current state across the network</p></div><button class="refresh-btn" onclick="showView('stations')">Open stations</button></div><div class="card-body" id="dashboard-stations"></div></div>
  </div>
</section>

<!-- LIVE MONITORING -->
<section class="view" id="view-monitoring">
  <div class="page-head"><div><h3>Live Monitoring</h3><p>Current telemetry and station event-time history.</p></div><div class="page-tools"><select id="monitor-station" class="select"></select></div></div>
  <div class="grid cards-3">
    <div class="card"><div class="card-head"><div><h4>Temperature</h4><p>°C · event-time series</p></div></div><div class="card-body"><div class="chart-wrap" id="chart-temp"></div></div></div>
    <div class="card"><div class="card-head"><div><h4>Pressure</h4><p>hPa · event-time series</p></div></div><div class="card-body"><div class="chart-wrap" id="chart-pressure"></div></div></div>
    <div class="card"><div class="card-head"><div><h4>Humidity</h4><p>% RH · event-time series</p></div></div><div class="card-body"><div class="chart-wrap" id="chart-humidity"></div></div></div>
  </div>
  <div class="grid cards-2" style="margin-top:16px"><div class="card"><div class="card-head"><div><h4>Selected Station</h4><p>Live state and current observation</p></div></div><div class="card-body" id="monitor-station-card"></div></div><div class="card"><div class="card-head"><div><h4>Delivery</h4><p>Event time and backend receipt</p></div></div><div class="card-body" id="monitor-delivery"></div></div></div>
</section>

<!-- ALERTS -->
<section class="view" id="view-alerts">
  <div class="page-head"><div><h3>Anomaly Alerts</h3><p>Sensor anomalies and other non-normal classified observations.</p></div><div class="page-tools"><span class="tag" id="alerts-count">0 alerts</span></div></div>
  <div class="grid cards-2"><div class="card"><div class="card-head"><div><h4>Recent Alerts</h4><p>Click an alert for explanation and evidence.</p></div></div><div class="card-body"><div class="alerts" id="alerts-list"></div></div></div><div class="card"><div class="card-head"><div><h4>Current Classification</h4><p>Network distribution</p></div></div><div class="card-body" id="classification-summary"></div></div></div>
</section>

<!-- HEALTH -->
<section class="view" id="view-health">
  <div class="page-head"><div><h3>Sensor Health</h3><p>Condition, degradation and maintenance signals for every station.</p></div></div>
  <div class="card"><div class="card-body"><div class="health-grid" id="health-grid"></div></div></div>
</section>

<!-- WEATHER -->
<section class="view" id="view-weather">
  <div class="page-head"><div><h3>Weather Events</h3><p>Unusual but physically consistent atmospheric transitions.</p></div><div class="page-tools"><span class="tag" id="weather-count">0 events</span></div></div>
  <div class="grid cards-2"><div class="card"><div class="card-head"><div><h4>Recent Weather Events</h4><p>Current network observations classified as genuine weather</p></div></div><div class="card-body"><div class="alerts" id="weather-list"></div></div></div><div class="card"><div class="card-head"><div><h4>Selected Event Context</h4><p>Thermodynamic and change context</p></div></div><div class="card-body" id="weather-context-panel"><div class="empty">Select a weather event to inspect the station.</div></div></div></div>
</section>

<!-- STATIONS -->
<section class="view" id="view-stations">
  <div class="page-head"><div><h3>Stations</h3><p>All simulated AWS nodes and their current state.</p></div></div>
  <div class="card"><div class="card-body" style="padding:0;overflow:auto"><table class="table"><thead><tr><th>Station</th><th>State</th><th>Temperature</th><th>Pressure</th><th>Humidity</th><th>Health</th><th>Delivery</th><th>Event Time</th></tr></thead><tbody id="stations-table"></tbody></table></div></div>
</section>

<!-- QUALITY -->
<section class="view" id="view-quality">
  <div class="page-head"><div><h3>Data Quality</h3><p>Validation, quarantine and lineage status.</p></div></div>
  <div class="quality-grid" id="quality-cards"></div>
  <div class="grid cards-2" style="margin-top:16px"><div class="card"><div class="card-head"><div><h4>Quality State</h4><p>Trusted training source and review queue</p></div></div><div class="card-body" id="quality-state"></div></div><div class="card"><div class="card-head"><div><h4>Recent Offline → Synced</h4><p>Backfilled observations retain event time</p></div></div><div class="card-body"><div class="alerts" id="offline-list"></div></div></div></div>
</section>

<!-- SYSTEM -->
<section class="view" id="view-system">
  <div class="page-head"><div><h3>System Status</h3><p>Security, edge connectivity and pipeline health.</p></div><div class="page-tools"><span class="sim-state" id="sim-status-side">SIMULATION OFF</span><button class="sim-control" id="side-sim-btn" onclick="toggleSimulation()">▶ Start Live Network</button></div></div>
  <div class="status-grid" id="status-grid"></div>
  <div class="grid cards-2" style="margin-top:16px"><div class="card"><div class="card-head"><div><h4>Security</h4><p>Authenticated edge communication</p></div></div><div class="card-body" id="security-panel"></div></div><div class="card"><div class="card-head"><div><h4>Pipeline</h4><p>Current platform state</p></div></div><div class="card-body"><div class="timeline"><span class="tag">Edge AI</span><span class="tag">Temporal</span><span class="tag">Multivariate</span><span class="tag">ExtraTrees</span><span class="tag">Spatial</span><span class="tag">Weather Context</span><span class="tag">Decision Fusion</span><span class="tag">Quality Gate</span><span class="tag">SHAP</span><span class="tag">Store & Forward</span></div></div></div></div>
</section>
</div></main></div>

<div class="modal-bg" id="modal"><div class="modal"><div class="modal-head"><div><h4 id="modal-title">Alert Details</h4><span id="modal-subtitle">—</span></div><button class="modal-close" onclick="closeModal()">Close</button></div><div class="modal-body" id="modal-body"></div></div></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const stations=[
{id:'AWS_01_Delhi',city:'Delhi',lat:28.70,lon:77.20},{id:'AWS_02_Mumbai',city:'Mumbai',lat:19.08,lon:72.88},{id:'AWS_03_Chennai',city:'Chennai',lat:13.08,lon:80.27},{id:'AWS_04_Himachal',city:'Himachal',lat:32.10,lon:77.20},{id:'AWS_05_Kolkata',city:'Kolkata',lat:22.57,lon:88.36},{id:'AWS_06_Bangalore',city:'Bangalore',lat:12.97,lon:77.59},{id:'AWS_07_Kerala',city:'Kerala',lat:10.00,lon:76.40},{id:'AWS_08_Rajasthan',city:'Rajasthan',lat:26.91,lon:75.79},{id:'AWS_09_NorthEast',city:'NorthEast',lat:25.58,lon:91.89},{id:'AWS_10_Gujarat',city:'Gujarat',lat:23.03,lon:72.58}];
const stationMeta=Object.fromEntries(stations.map(s=>[s.id,s]));
let appState=null, selectedStation='AWS_03_Chennai', selectedSnapshot=null, map=null, markers={}, refreshBusy=false;
const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const num=(v,d=1)=>Number.isFinite(Number(v))?Number(v).toFixed(d):'—';
const cls=c=>c==='GENUINE_WEATHER_EVENT'?'weather':c==='SENSOR_ANOMALY'?'anomaly':'normal';
const clsLabel=c=>c==='GENUINE_WEATHER_EVENT'?'GENUINE WEATHER EVENT':c==='SENSOR_ANOMALY'?'SENSOR ANOMALY':'NORMAL';
const delivery=d=>String(d||'').toUpperCase()==='SECURE_OFFLINE_SYNC'?'OFFLINE → SYNCED':String(d||'').toUpperCase()==='SECURE_LIVE'?'SECURE • LIVE':'LIVE';
function safeStationRows(){return appState?.stations||[]}
function currentStationRow(){return safeStationRows().find(s=>s.station_name===selectedStation)||null}
function iconFor(type){return L.divIcon({className:'',html:`<div class="marker-dot ${type}"></div>`,iconSize:[17,17],iconAnchor:[8.5,8.5]})}
function markerType(st){
  const latest=st?.latest||null, edge=st?.latest_edge_event||null;
  const latestType=latest?cls(latest.classification):'offline';
  if(edge){
    const edgeTime=Date.parse(String(edge.event_time||edge.timestamp||'').replace(' ','T'));
    const now=Date.now();
    if(Number.isFinite(edgeTime) && edgeTime>0 && now-edgeTime<=45000){
      const latestTime=latest?Date.parse(String(latest.event_time||latest.receive_time||'').replace(' ','T')):NaN;
      if(!Number.isFinite(latestTime) || edgeTime>=latestTime) return 'anomaly';
    }
  }
  return latestType;
}
function markerPopup(st){const r=st?.latest||{},m=stationMeta[st?.station_name]||{};const type=markerType(st);return `<div style="min-width:220px;font-family:Segoe UI,Arial"><b style="font-size:16px">${esc(st?.station_name||'Station')}</b><div style="font-size:13px;color:#718096;margin:3px 0 9px">${esc(m.city||'')}</div><div style="font-weight:800;color:${type==='anomaly'?'#dc3545':type==='weather'?'#c77a00':type==='normal'?'#19a974':'#68788d'};font-size:14px">${esc(r.classification?clsLabel(r.classification):'NO DATA')}</div><div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:7px;margin-top:8px"><div><span style="font-size:12px;color:#72857a">TEMP</span><br><b>${num(r.temperature)}°C</b></div><div><span style="font-size:12px;color:#72857a">PRESS</span><br><b>${num(r.pressure)}</b></div><div><span style="font-size:12px;color:#72857a">RH</span><br><b>${num(r.humidity)}%</b></div></div><div style="font-size:13px;color:#718096;margin-top:9px">Health ${esc(healthLabel(st))} · ${esc(delivery(r.delivery_mode))}</div></div>`}
function healthScore(st){return Number(st?.health?.health_score??st?.health_score??st?.health?.score??st?.score??85)}
function healthLabel(st){const score=healthScore(st);if(score>=80)return'HEALTHY';if(score>=60)return'WARNING';if(score>=40)return'DEGRADING';return'FAULT SUSPECTED'}
function initMap(){map=L.map('map',{zoomControl:true,scrollWheelZoom:true}).setView([22.4,79.2],4.7);L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:10,attribution:'&copy; OpenStreetMap contributors'}).addTo(map);stations.forEach(s=>{const m=L.marker([s.lat,s.lon],{icon:iconFor('offline')}).addTo(map);m.bindTooltip(s.id,{direction:'top',offset:[0,-8]});m.on('mouseover',()=>{const st=currentStationLive(s.id);if(st)m.setTooltipContent(markerPopup(st))});m.on('click',()=>selectStation(s.id,true));markers[s.id]=m});}
function currentStationLive(id){return safeStationRows().find(x=>x.station_name===id)||null}
function refreshMarkers(){if(!map)return;safeStationRows().forEach(st=>{const m=markers[st.station_name];if(!m)return;m.setIcon(iconFor(markerType(st)));m.setTooltipContent(markerPopup(st))})}
function setupStationSelectors(){['station-select','dashboard-station','monitor-station'].forEach(id=>{const el=document.getElementById(id);el.innerHTML=stations.map(s=>`<option value="${esc(s.id)}">${esc(s.id)} · ${esc(s.city)}</option>`).join('');el.value=selectedStation;el.addEventListener('change',()=>selectStation(el.value,false))})}
function renderWeatherContextForStation(p){const r=p.latest||{},q=p.last_24h||{},rh=Number(r.humidity),temp=Number(r.temperature);let dew=null;if(Number.isFinite(temp)&&Number.isFinite(rh)&&rh>0&&rh<=100){const g=(17.27*temp/(237.7+temp))+Math.log(rh/100);const d=237.7*g/(17.27-g);dew=Number.isFinite(d)?d:null}const wc=(p.analysis?.layers?.weather_context)||null;const weather=wc?.weather_event_candidate===true;document.getElementById('inspector').insertAdjacentHTML('beforeend',`<div class="section-title">Atmospheric context</div><div class="mini-grid"><div class="mini"><span>Dew point</span><b>${dew==null?'—':dew.toFixed(1)+'°C'}</b></div><div class="mini"><span>24h weather events</span><b>${Number(q.weather_events||0)}</b></div><div class="mini"><span>24h sensor anomalies</span><b>${Number(q.sensor_anomalies||0)}</b></div><div class="mini"><span>24h quarantined</span><b>${Number(q.quarantined||0)}</b></div></div>${wc?`<div class="notice" style="margin-top:10px">Weather context: <strong>${weather?'Candidate':'No candidate'}</strong> · transition ${num(wc.weather_transition_score,0)} · confidence ${num(wc.confidence,0)}%</div>`:''}<div style="display:flex;gap:8px;align-items:center;justify-content:space-between;margin-top:10px"><span class="tag">Fast station view · deep analysis on demand</span>${r.id?`<button class="action-btn" onclick="loadDeepStationAnalysis()">Inspect full evidence</button>`:''}</div>`)}
async function loadDeepStationAnalysis(){if(!selectedStation)return;const btns=document.querySelectorAll('#inspector .action-btn');btns.forEach(b=>{b.disabled=true;b.textContent='Analyzing…'});try{const res=await fetch('/api/station-analysis/'+encodeURIComponent(selectedStation)+'?t='+Date.now(),{cache:'no-store'});const p=await res.json();if(!res.ok||p.status!=='ok')throw new Error(p.error||'Analysis unavailable');if(selectedSnapshot){selectedSnapshot.analysis=p.analysis||null;renderInspector(selectedSnapshot);renderWeatherContextForStation(selectedSnapshot)}}catch(e){document.getElementById('inspector').insertAdjacentHTML('beforeend',`<div class="notice" style="margin-top:10px">${esc(e.message||e)}</div>`)}finally{btns.forEach(b=>{b.disabled=false;b.textContent='Inspect full evidence'})}}
async function selectStation(station,fromMap){selectedStation=station;['station-select','dashboard-station','monitor-station'].forEach(id=>{const el=document.getElementById(id);if(el)el.value=station});const meta=stationMeta[station]||{};document.getElementById('inspector-title').textContent=station;document.getElementById('inspector-location').textContent=meta.city||'AWS station';try{const res=await fetch('/api/station-view/'+encodeURIComponent(station)+'?t='+Date.now(),{cache:'no-store'});const p=await res.json();if(!res.ok||p.status!=='ok')throw new Error(p.error||'Station API error');selectedSnapshot=p;renderInspector(p);renderStationCharts(p.history||[]);renderMonitor(p);renderWeatherContextForStation(p);renderOffline();if(fromMap&&map)map.setView([meta.lat,meta.lon],6,{animate:true})}catch(e){document.getElementById('inspector').innerHTML=`<div class="empty">Unable to load station: ${esc(e.message)}</div>`}}
function renderInspector(p){const r=p.latest||{},a=p.analysis||{},layers=a.layers||{},conf=Number(a.confidence??r.confidence??0),c=r.classification||a.classification||'NORMAL',h=p.health||{},score=healthScore(p),edge=p.edge||{};const signals=[['Edge AI',Boolean(edge.hard_blocked),Boolean(edge.hard_blocked)?90:10],['Temporal',Boolean(layers.temporal?.is_anomaly),Number(layers.temporal?.score||0)],['Multivariate',Boolean(layers.multivariate?.is_anomaly),Number(layers.multivariate?.score||0)],['ExtraTrees',Boolean(layers.ml_evidence?.anomaly_candidate),Number(layers.ml_evidence?.confidence||0)],['Spatial',Boolean(layers.spatial_evidence?.anomaly_candidate),Number(layers.spatial_evidence?.confidence||0)],['Weather',Boolean(layers.weather_context?.weather_event_candidate),Number(layers.weather_context?.confidence||0)],['Physics',layers.psychrometrics?.is_physically_valid===false,100],['Security',Number(layers.quarantine?.threat_score||0)>=.9,Math.min(100,Number(layers.quarantine?.threat_score||0)*100)]];document.getElementById('inspector').innerHTML=`<div class="status-banner"><div class="status-main"><strong>${esc(clsLabel(c))}</strong><span>${esc(a.root_cause||r.detection_source||'Multi-evidence classification')}</span></div><span class="status-badge ${cls(c)}">${num(conf,0)}% confidence</span></div><div class="metrics"><div class="metric"><span>Temperature</span><b>${num(r.temperature)}°C</b><small>${esc(r.event_time||'—')}</small></div><div class="metric"><span>Pressure</span><b>${num(r.pressure)}</b><small>hPa</small></div><div class="metric"><span>Humidity</span><b>${num(r.humidity)}%</b><small>RH</small></div></div><div class="mini-grid"><div class="mini"><span>Sensor health</span><b>${score.toFixed(0)}% · ${healthLabel(p)}</b></div><div class="mini"><span>Connectivity</span><b>${(edge.online||edge.edge_online)?'LIVE':'OFFLINE'}</b></div><div class="mini"><span>Delivery</span><b>${esc(delivery(r.delivery_mode))}</b></div><div class="mini"><span>Receive time</span><b>${esc(r.receive_time||'—')}</b></div></div><div class="section-title">Decision signals</div><div class="signal-list">${signals.map(([name,on,val])=>`<div class="signal"><div class="signal-head"><span>${esc(name)}</span><b style="color:${on?(name==='Weather'?'var(--orange)':'var(--red)'):'#7f8ea0'}">${on?'SIGNAL':'CLEAR'}</b></div><div class="track"><div class="fill ${on?(name==='Weather'?'warn':'bad'):'blue'}" style="width:${Math.max(7,Math.min(100,Number(val)||0))}%"></div></div></div>`).join('')}</div><div class="section-title">Observation lineage</div><div class="notice">${esc(r.event_time||'—')} → received ${esc(r.receive_time||'—')} · validation ${esc(r.validation_status||'—')} · ${esc(r.quarantine_status||'NONE')}</div><div style="display:flex;gap:7px;flex-wrap:wrap;margin-top:10px"><span class="tag">Model ${esc(r.model_version||'—')}</span><span class="tag">Source ${esc(r.detection_source||'—')}</span><span class="tag">Sequence ${esc(r.sequence??'—')}</span></div>`}
function drawChart(id,history,key,unit){const box=document.getElementById(id);if(!history?.length){box.innerHTML='<div class="empty">No telemetry history available.</div>';return}const vals=history.map(x=>Number(x[key])).filter(Number.isFinite);if(!vals.length){box.innerHTML='<div class="empty">No telemetry values available.</div>';return}const W=760,H=280,pad={l:48,r:18,t:15,b:30},iw=W-pad.l-pad.r,ih=H-pad.t-pad.b,mn=Math.min(...vals),mx=Math.max(...vals),lo=mn===mx?mn-1:mn,hi=mn===mx?mx+1:mx,x=i=>pad.l+(history.length===1?iw/2:iw*i/(history.length-1)),y=v=>pad.t+(1-(v-lo)/(hi-lo))*ih;let svg=`<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">`;[0,.5,1].forEach(t=>{const yy=pad.t+t*ih;svg+=`<line class="gridline" x1="${pad.l}" y1="${yy}" x2="${W-pad.r}" y2="${yy}"/>`});svg+=`<text class="axislabel" x="4" y="${pad.t+7}">${num(hi,1)} ${unit}</text><text class="axislabel" x="4" y="${H-pad.b}">${num(lo,1)} ${unit}</text>`;const path=history.map((r,i)=>`${i?'L':'M'}${x(i).toFixed(1)} ${y(Number.isFinite(Number(r[key]))?Number(r[key]):lo).toFixed(1)}`).join(' ');svg+=`<path class="chartline" d="${path}"/>`;history.forEach((r,i)=>{const c=r.classification;if(c&&c!=='NORMAL'){const cc=c==='SENSOR_ANOMALY'?'#dc3545':'#f59e0b';const xx=x(i),vv=Number(r[key]);svg+=`<line class="chart-event" stroke="${cc}" x1="${xx}" y1="${pad.t}" x2="${xx}" y2="${H-pad.b}"/><circle class="chartdot" fill="${cc}" cx="${xx}" cy="${y(Number.isFinite(vv)?vv:lo)}" r="5"/>`}});svg+='</svg>';box.innerHTML=svg}
function renderStationCharts(h){drawChart('chart-temp',h,'temperature','°C');drawChart('chart-pressure',h,'pressure','hPa');drawChart('chart-humidity',h,'humidity','%');}
function renderMonitor(p){const r=p.latest||{},h=p.health||{},score=healthScore(p);document.getElementById('monitor-station-card').innerHTML=`<div class="status-banner"><div class="status-main"><strong>${esc(selectedStation)}</strong><span>${esc(stationMeta[selectedStation]?.city||'')}</span></div><span class="status-badge ${cls(r.classification||'NORMAL')}">${esc(clsLabel(r.classification||'NORMAL'))}</span></div><div class="metrics"><div class="metric"><span>Temperature</span><b>${num(r.temperature)}°C</b></div><div class="metric"><span>Pressure</span><b>${num(r.pressure)}</b></div><div class="metric"><span>Humidity</span><b>${num(r.humidity)}%</b></div></div><div class="mini-grid"><div class="mini"><span>Health</span><b>${score.toFixed(0)}%</b></div><div class="mini"><span>Status</span><b>${edgeStatus(p)}</b></div></div>`;document.getElementById('monitor-delivery').innerHTML=`<div class="mini-grid"><div class="mini"><span>Event time</span><b>${esc(r.event_time||'—')}</b></div><div class="mini"><span>Receive time</span><b>${esc(r.receive_time||'—')}</b></div><div class="mini"><span>Delivery</span><b>${esc(delivery(r.delivery_mode))}</b></div><div class="mini"><span>Secure</span><b>${r.secure_authenticated?'AUTHENTICATED':'—'}</b></div></div>`}
function edgeStatus(p){return p?.edge?.edge_online?'LIVE':'OFFLINE'}
function renderDashboardStations(){const rows=safeStationRows();document.getElementById('dashboard-stations').innerHTML=rows.slice(0,10).map(st=>{const r=st.latest||{};return `<div style="display:flex;justify-content:space-between;gap:10px;padding:9px 0;border-bottom:1px solid var(--line)"><div><strong style="font-size:15px">${esc(st.station_name)}</strong><div class="station-city">${esc(stationMeta[st.station_name]?.city||'')}</div></div><div style="text-align:right"><div><span class="pill ${cls(r.classification||'NORMAL')}">${esc(clsLabel(r.classification||'NORMAL'))}</span></div><div style="font-size:13px;color:var(--muted);margin-top:5px">${num(r.temperature)}°C · ${num(r.humidity)}%</div></div></div>`}).join('')}
function eventRows(limit=12,filter){let rows=(appState?.recent_events||[]);if(filter)rows=rows.filter(x=>x.classification===filter);return rows.slice(0,limit)}
function renderAlertList(targetId,rows){const el=document.getElementById(targetId);if(!rows.length){el.innerHTML='<div class="empty">No events.</div>';return}el.innerHTML=rows.map(e=>{const t=cls(e.classification);return `<button style="all:unset;cursor:pointer" onclick="openAlert(${Number(e.id||0)})"><div class="alert-row"><span class="alert-dot" style="background:${t==='anomaly'?'var(--red)':t==='weather'?'var(--orange)':'var(--green)'}"></span><div><strong>${esc(e.station_name)} · ${esc(clsLabel(e.classification))}</strong><div class="meta">${esc(e.reason||'Classified observation')} · ${esc(delivery(e.delivery_mode))}</div></div><div class="alert-right"><b>${num(e.confidence,0)}%</b><span>${esc(e.event_time||'—')}</span></div></div></button>`}).join('')}
function renderAlerts(){const all=eventRows(20).filter(e=>e.classification!=='NORMAL');renderAlertList('dashboard-alerts',all.slice(0,5));renderAlertList('alerts-list',all);document.getElementById('alerts-count').textContent=`${all.length} alerts`;const normal=safeStationRows().filter(s=>(s.latest?.classification||'NORMAL')==='NORMAL').length,weather=safeStationRows().filter(s=>s.latest?.classification==='GENUINE_WEATHER_EVENT').length,an=safeStationRows().filter(s=>s.latest?.classification==='SENSOR_ANOMALY').length;document.getElementById('classification-summary').innerHTML=`<div class="kpi green" style="box-shadow:none;margin-bottom:8px"><div><div class="label">NORMAL</div><div class="value">${normal}</div><div class="sub">stations currently clear</div></div></div><div class="kpi orange" style="box-shadow:none;margin-bottom:8px"><div><div class="label">WEATHER EVENTS</div><div class="value">${weather}</div><div class="sub">stations with weather state</div></div></div><div class="kpi red" style="box-shadow:none"><div><div class="label">SENSOR ANOMALIES</div><div class="value">${an}</div><div class="sub">stations requiring attention</div></div></div>`}
function renderHealth(){document.getElementById('health-grid').innerHTML=safeStationRows().map(st=>{const score=healthScore(st), label=healthLabel(st), color=score>=80?'var(--green)':score>=60?'var(--orange)':'var(--red)';return `<div class="health-card" onclick="selectStation('${st.station_name.replace(/'/g,"\\'")}',false)"><div class="health-top"><div><strong>${esc(st.station_name)}</strong><div class="snapshot-city">${esc(stationMeta[st.station_name]?.city||'')}</div></div><span>${esc(label)}</span></div><div class="health-bar"><div style="width:${Math.max(0,Math.min(100,score))}%;background:${color}"></div></div><div class="health-meta"><span>${score.toFixed(0)}% health</span><span>${st.edge_online?'LIVE':'OFFLINE'}</span></div></div>`}).join('')}
function renderWeather(){const rows=(appState?.weather_events_recent||eventRows(20,'GENUINE_WEATHER_EVENT')).slice(0,20);document.getElementById('weather-count').textContent=`${rows.length} events`;renderAlertList('weather-list',rows);if(rows.length)inspectWeather(rows[0]);}
async function inspectWeather(e){const station=e.station_name;try{const res=await fetch('/api/weather-context/'+Number(e.id)+'?t='+Date.now(),{cache:'no-store'});const p=await res.json();if(!res.ok||p.status!=='ok')throw new Error(p.error||'Weather context unavailable');const wc=p.analysis?.layers?.weather_context||{};document.getElementById('weather-context-panel').innerHTML=`<div class="status-banner"><div class="status-main"><strong>${esc(station)}</strong><span>${esc(stationMeta[station]?.city||'')}</span></div><span class="status-badge weather">WEATHER EVENT</span></div><div class="metrics"><div class="metric"><span>Transition score</span><b>${num(wc.weather_transition_score,0)}</b></div><div class="metric"><span>Confidence</span><b>${num(wc.confidence,0)}%</b></div><div class="metric"><span>Dew point</span><b>${num(wc.dew_point_c)}°C</b></div></div><div class="mini-grid"><div class="mini"><span>Temperature Δ</span><b>${num(wc.changes?.temperature_c)}°C</b></div><div class="mini"><span>Humidity Δ</span><b>${num(wc.changes?.humidity_percent)}%</b></div><div class="mini"><span>Pressure Δ</span><b>${num(wc.changes?.pressure_hpa)} hPa</b></div><div class="mini"><span>Physical validation</span><b>${wc.physical_validation?.is_physically_valid?'PASS':'FAIL'}</b></div></div><div class="notice" style="margin-top:10px">${esc(wc.reason||'Weather-context evidence loaded for the selected observation.')}</div>`}catch(err){document.getElementById('weather-context-panel').innerHTML=`<div class="empty">Unable to load weather context: ${esc(err.message||err)}</div>`}}
function renderStationsTable(){document.getElementById('stations-table').innerHTML=safeStationRows().map(st=>{const r=st.latest||{},type=cls(r.classification||'NORMAL');return `<tr><td><strong>${esc(st.station_name)}</strong><div class="station-city">${esc(stationMeta[st.station_name]?.city||'')}</div></td><td><span class="pill ${type}">${esc(clsLabel(r.classification||'NORMAL'))}</span></td><td>${num(r.temperature)}°C</td><td>${num(r.pressure)}</td><td>${num(r.humidity)}%</td><td>${healthScore(st).toFixed(0)}%</td><td>${esc(delivery(r.delivery_mode))}</td><td>${esc(r.event_time||'—')}</td></tr>`}).join('')}
function renderOffline(){const el=document.getElementById('offline-list');if(!el)return;const source=Array.isArray(selectedSnapshot?.history)?selectedSnapshot.history:[];const items=source.filter(x=>String(x.delivery_mode||'').toUpperCase()==='SECURE_OFFLINE_SYNC'&&String(x.classification||'NORMAL').toUpperCase()!=='NORMAL').slice(-10).reverse();el.innerHTML=items.length?items.map(x=>`<div class="alert-row"><span class="alert-dot" style="background:var(--orange)"></span><div><strong>${esc(selectedStation||x.station_name||'Station')} · ${esc(clsLabel(x.classification||'Genuine Weather Event'))}</strong><div class="meta">Event ${esc(x.event_time||'—')} → received ${esc(x.receive_time||'—')}</div></div><div class="alert-right"><b>${num(x.confidence,0)}%</b><span>OFFLINE → SYNCED</span></div></div>`).join(''):'<div class="empty">No offline-synced anomalies for the selected station.</div>'}
function renderQuality(){const q=appState?.quality||{},total=Math.max(1,appState?.total_readings||0),rate=Math.min(100,(q.validated||0)*100/total);document.getElementById('quality-cards').innerHTML=[['Validated',q.validated||0,'clean observations'],['Quarantined',q.quarantined||0,'isolated observations'],['Review required',q.review||0,'operator queue'],['Lineage records',q.lineage||0,'audit records']].map(x=>`<div class="quality-card"><span>${x[0]}</span><b>${Number(x[1]).toLocaleString()}</b><small>${x[2]}</small>${x[0]==='Validated'?`<div class="progress"><div style="width:${rate}%"></div></div>`:''}</div>`).join('');document.getElementById('quality-state').innerHTML=`<div class="mini-grid"><div class="mini"><span>Trusted training source</span><b>validated_observations</b></div><div class="mini"><span>Trusted rows</span><b>${Number(q.trusted_training||0).toLocaleString()}</b></div></div><div class="notice" style="margin-top:10px">Suspicious, imputed and untrusted observations remain outside the trusted training source.</div>`;const items=selectedSnapshot?.history?.filter(x=>String(x.delivery_mode).toUpperCase()==='SECURE_OFFLINE_SYNC'&&x.classification!=='NORMAL')||[];document.getElementById('offline-list').innerHTML=items.length?items.slice(-10).map(x=>`<div class="alert-row"><span class="alert-dot" style="background:var(--orange)"></span><div><strong>${esc(selectedStation)} · ${esc(clsLabel(x.classification))}</strong><div class="meta">Event ${esc(x.event_time||'—')} → received ${esc(x.receive_time||'—')}</div></div><div class="alert-right"><b>${num(x.confidence,0)}%</b><span>OFFLINE → SYNCED</span></div></div>`).join(''):'<div class="empty">No offline-synced anomalies for the selected station.</div>'}
function renderSystem(){const q=appState?.quality||{},s=appState?.security||{};document.getElementById('status-grid').innerHTML=[['API / Dashboard','OPERATIONAL'],['Edge nodes',`${appState?.live_nodes??0}/${appState?.total_stations??0} LIVE`],['Training source',`VALIDATED · ${(q.trusted_training||0).toLocaleString()}`],['Data lineage',(q.lineage||0).toLocaleString()],['Quarantine',(q.quarantined||0).toLocaleString()],['Review queue',(q.review||0).toLocaleString()]].map(x=>`<div class="status-item"><span>${x[0]}</span><b>${x[1]}</b></div>`).join('');document.getElementById('security-panel').innerHTML=`<div class="status-grid"><div class="status-item"><span>Authenticated</span><b>${s.authenticated??0}</b></div><div class="status-item"><span>Rejected</span><b>${s.rejected??0}</b></div><div class="status-item"><span>Rate limited</span><b>${s.rate_limited??0}</b></div><div class="status-item"><span>Duplicates</span><b>${s.duplicates??0}</b></div></div>`}
async function syncSimulationStatus(){try{const res=await fetch('/api/simulation/status?t='+Date.now(),{cache:'no-store'});const p=await res.json();const running=Boolean(p.running);const failed=!running&&p.unexpected_stop;const label=running?'SIMULATION RUNNING':(failed?'SIMULATION ERROR':'SIMULATION OFF');['sim-status','sim-status-side'].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent=label});['top-sim-btn','side-sim-btn'].forEach(id=>{const b=document.getElementById(id);if(!b)return;b.textContent=running?'■ Stop Live Network':'▶ Start Live Network';b.classList.toggle('stop',running);b.title=running?'Stop the 10-station secure demo simulator':'Start the 10-station secure demo simulator'});}catch(e){console.error(e)}}
async function toggleSimulation(){const status=await fetch('/api/simulation/status?t='+Date.now(),{cache:'no-store'}).then(r=>r.json()).catch(()=>({running:false}));const action=status.running?'/api/simulation/stop':'/api/simulation/start';const buttons=['top-sim-btn','side-sim-btn'].map(id=>document.getElementById(id)).filter(Boolean);buttons.forEach(b=>{b.disabled=true});try{const res=await fetch(action,{method:'POST'});const p=await res.json();if(!res.ok||p.status!=='ok')throw new Error(p.error||'Simulation control failed');}catch(e){alert(e.message)}finally{buttons.forEach(b=>{b.disabled=false});await syncSimulationStatus();await refresh(true)}}
async function refreshNow(){await refresh(true)}
async function refresh(force=false){if(refreshBusy&&!force)return;refreshBusy=true;try{const res=await fetch('/api/final-dashboard?t='+Date.now(),{cache:'no-store'});const p=await res.json();if(!res.ok||p.status!=='ok')throw new Error(p.error||'Dashboard API error');appState=p;document.getElementById('server-time').textContent=p.server_time||'—';document.getElementById('k-stations').textContent=p.total_stations??10;document.getElementById('k-live').textContent=`${p.live_nodes??0} online`;document.getElementById('k-health').textContent=(p.system_health_score!=null?`${Math.round(p.system_health_score)}%`:'—');document.getElementById('k-health-sub').textContent=`${p.live_nodes||0}/${p.total_stations||0} edge nodes live`;;document.getElementById('k-weather').textContent=(p.weather_events||0).toLocaleString();document.getElementById('k-anomalies').textContent=(p.active_anomalies||0).toLocaleString();document.getElementById('k-anomaly-sub').textContent=`${num(p.anomaly_rate,1)}% of readings`;document.getElementById('side-status').textContent='System Operational';document.getElementById('side-dot').style.background='var(--green)';document.getElementById('live-status').textContent='LIVE';document.getElementById('live-dot').style.background='var(--green)';refreshMarkers();renderDashboardStations();renderAlerts();renderHealth();renderWeather();renderStationsTable();renderQuality();renderSystem();if(selectedStation)await selectStation(selectedStation,false)}catch(e){document.getElementById('live-status').textContent='OFFLINE';document.getElementById('live-dot').style.background='var(--red)';document.getElementById('side-status').textContent='Connection issue';document.getElementById('side-dot').style.background='var(--red)';console.error(e)}finally{refreshBusy=false}}
async function openAlert(id){if(!id)return;document.getElementById('modal').classList.add('show');document.getElementById('modal-title').textContent='Alert Details';document.getElementById('modal-subtitle').textContent='Loading evidence…';document.getElementById('modal-body').innerHTML='<div class="empty">Loading…</div>';try{const res=await fetch('/api/anomaly/'+id);const p=await res.json();if(!res.ok||p.status!=='ok')throw new Error(p.error||'Unable to load alert');const x=p.explanation||{},v=x.verdict||{},fa=x.feature_attribution||{};document.getElementById('modal-title').textContent=`${x.station||'Station'} · ${v.severity||'Alert'}`;document.getElementById('modal-subtitle').textContent=`${delivery(x.delivery_mode)} · ${x.timestamp||''}`;const features=(fa.features||[]).map(f=>`<div class="detail-row"><strong>${esc(f.parameter||'Feature')}</strong><span>${num(f.contribution_percent,1)}%</span><span>SHAP ${num(f.shap_value,3)} · ${esc(f.direction||'')}</span></div>`).join('');const evidence=(x.evidence||[]).map(e=>`<div class="detail-row"><strong>${esc(e.layer)}</strong><span>${esc(e.status)}</span><span>${esc(e.detail||'')}</span></div>`).join('');document.getElementById('modal-body').innerHTML=`<div class="modal-grid"><div class="modal-card"><span>Verdict</span><b>${esc(v.severity||'—')}</b></div><div class="modal-card"><span>Confidence</span><b>${num(v.confidence,0)}%</b></div><div class="modal-card"><span>Primary factor</span><b>${esc(fa.primary_parameter||'—')}</b></div><div class="modal-card"><span>Delivery</span><b>${esc(delivery(x.delivery_mode))}</b></div></div><div class="explain-title">Explainability — SHAP feature attribution</div><div class="explain-note">The feature rows below show which observed parameters contributed to the current model explanation. Positive and negative SHAP direction indicate the direction of the contribution; they are evidence for the displayed verdict, not a replacement for the full decision pipeline.</div><div class="detail-list">${features||'<div class="empty">No SHAP feature rows returned.</div>'}</div><div class="explain-title">Decision-layer evidence</div><div class="detail-list">${evidence||'<div class="empty">No evidence rows returned.</div>'}</div>`}catch(e){document.getElementById('modal-body').innerHTML=`<div class="empty">${esc(e.message||e)}</div>`}}
function closeModal(){document.getElementById('modal').classList.remove('show')}
document.getElementById('modal').addEventListener('click',e=>{if(e.target.id==='modal')closeModal()});
function showView(name){document.querySelectorAll('.view').forEach(v=>v.classList.remove('active'));document.getElementById('view-'+name)?.classList.add('active');document.querySelectorAll('.nav button').forEach(b=>b.classList.toggle('active',b.dataset.view===name));const meta={dashboard:['Dashboard','Real-time overview of the AWS network'],monitoring:['Live Monitoring','Current telemetry and event-time history'],alerts:['Anomaly Alerts','Sensor anomalies and non-normal observations'],health:['Sensor Health','Condition and degradation across stations'],weather:['Weather Events','Physically consistent atmospheric events'],stations:['Stations','All AWS nodes and their current state'],quality:['Data Quality','Validation, quarantine and lineage status'],system:['System Status','Security, edge connectivity and pipeline health']}[name]||['Dashboard',''];document.getElementById('top-heading').textContent=meta[0];document.getElementById('top-sub').textContent=meta[1];document.getElementById('sidebar').classList.remove('open');if(name==='dashboard'&&map)setTimeout(()=>map.invalidateSize(),80)}
document.querySelectorAll('.nav button').forEach(b=>b.addEventListener('click',()=>showView(b.dataset.view)));
setupStationSelectors();initMap();syncSimulationStatus();refresh();setInterval(refresh,5000);setInterval(syncSimulationStatus,2000);setInterval(()=>{if(selectedStation&&!document.hidden)selectStation(selectedStation,false)},9000);
</script>
</body>
</html>
"""


@app.get("/api/simulation/status")
def api_simulation_status():
    try:
        return jsonify({"status": "ok", **_simulation_status_payload()})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.post("/api/simulation/start")
def api_simulation_start():
    try:
        payload, started = _simulation_start()
        return jsonify({"status": "ok", "started": started, **payload}), 200
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.post("/api/simulation/stop")
def api_simulation_stop():
    try:
        payload = _simulation_stop()
        return jsonify({"status": "ok", "stopped": True, **payload}), 200
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.get("/")
def dashboard_home():
    """Serve the final SkyGuard command-center dashboard."""
    return Response(DASHBOARD_HTML, mimetype="text/html")


def _open_browser() -> None:
    try:
        import webbrowser
        webbrowser.open("http://127.0.0.1:8050/", new=2)
    except Exception:
        pass


if __name__ == "__main__":
    initialize_backend()
    import threading
    threading.Timer(1.2, _open_browser).start()
    print("[START] SkyGuard AI Command Center: http://127.0.0.1:8050/")
    app.run(host="127.0.0.1", port=8050, debug=False, use_reloader=False, threaded=True)

