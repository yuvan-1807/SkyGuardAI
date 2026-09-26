from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import date, timedelta

from decision_fusion import DecisionFusionEngine
from anomaly_pipeline import AnomalyPipeline


def base_layers():
    return {
        "psychrometrics": {"is_physically_valid": True, "confidence_score": 100, "violations": []},
        "entropy": {"is_frozen": False, "confidence": 0},
        "temporal": {"is_anomaly": False, "score": 0},
        "multivariate": {"is_anomaly": False, "score": 0},
        "weather_context": {"weather_event_candidate": False, "weather_transition_score": 0, "confidence": 0},
        "ml_evidence": {"available": True, "anomaly_candidate": False, "confidence": 0, "parameters": {}},
        "spatial_evidence": {"available": True, "anomaly_candidate": False, "confidence": 0, "parameters": {}},
        "quarantine": {"threat_score": 0, "threat_level": "Low"},
    }


def ml_candidate(conf):
    return {
        "available": True,
        "anomaly_candidate": True,
        "confidence": conf,
        "strongest_parameter": "temperature",
        "parameters": {
            "temperature": {"anomaly_candidate": True, "confidence": conf}
        },
    }


def spatial_candidate(conf):
    return {
        "available": True,
        "anomaly_candidate": True,
        "confidence": conf,
        "strongest_parameter": "temperature",
        "parameters": {
            "temperature": {"anomaly_candidate": True, "confidence": conf}
        },
    }


def make_pipeline_db(path: str):
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
            detection_source TEXT,
            delivery_mode TEXT,
            secure_authenticated INTEGER DEFAULT 0,
            station_id TEXT,
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
    start = date(2024, 1, 1)
    stations = {
        "AWS_10_Gujarat": (30.0, 1010.0, 60.0),
        "AWS_02_Mumbai": (29.0, 1009.0, 65.0),
        "AWS_08_Rajasthan": (32.0, 1007.0, 50.0),
    }
    rows = []
    for i in range(120):
        ts = f"{start + timedelta(days=i)} 12:00:00"
        for station, (t, p, h) in stations.items():
            rows.append((station, ts, ts, ts, t, p, h, station))
    conn.executemany(
        "INSERT INTO sensor_readings(station_name,timestamp,event_time,receive_time,temperature,pressure,humidity,station_id) VALUES(?,?,?,?,?,?,?,?)",
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
    e = DecisionFusionEngine()

    normal = e.fuse(layers=base_layers())
    assert normal["classification"] == "NORMAL", normal

    weather = base_layers()
    weather["weather_context"] = {
        "weather_event_candidate": True,
        "weather_transition_score": 90,
        "confidence": 88,
        "reason": "Coordinated atmospheric transition",
    }
    result = e.fuse(layers=weather)
    assert result["classification"] == "GENUINE_WEATHER_EVENT", result

    sensor = base_layers()
    sensor["temporal"] = {"is_anomaly": True, "score": 92}
    sensor["ml_evidence"] = ml_candidate(88)
    sensor["spatial_evidence"] = spatial_candidate(86)
    result = e.fuse(layers=sensor)
    assert result["classification"] == "SENSOR_ANOMALY", result

    hard = base_layers()
    hard["psychrometrics"] = {"is_physically_valid": False, "confidence_score": 10, "violations": ["Humidity impossible"]}
    hard["weather_context"] = {"weather_event_candidate": True, "weather_transition_score": 100, "confidence": 100}
    result = e.fuse(layers=hard)
    assert result["classification"] == "SENSOR_ANOMALY", result
    assert result["hard_fault"] is True

    conflict = base_layers()
    conflict["weather_context"] = {"weather_event_candidate": True, "weather_transition_score": 84, "confidence": 82, "reason": "Strong coordinated transition"}
    conflict["ml_evidence"] = ml_candidate(82)
    result = e.fuse(layers=conflict)
    assert result["classification"] == "GENUINE_WEATHER_EVENT", result
    assert result["review_required"] is True

    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "fusion.db")
        make_pipeline_db(db)
        p = AnomalyPipeline(db)
        result = p.analyze_reading("AWS_10_Gujarat", 30.0, 1010.0, 60.0, event_time="2024-04-15 12:00:00")
        assert "decision_fusion" in result["layers"], result
        assert result["classification"] in {"NORMAL", "GENUINE_WEATHER_EVENT", "SENSOR_ANOMALY"}, result
        assert result["model_version"] == "phase3e-decision-fusion-v1", result

    print("PHASE 3E DECISION FUSION TESTS: PASS")
    print("Normal classification: NORMAL")
    print("Coordinated weather classification: GENUINE_WEATHER_EVENT")
    print("Multi-signal sensor classification: SENSOR_ANOMALY")
    print("Hard physical fault veto: SENSOR_ANOMALY")
    print("Weather/sensor conflict review: True")
    print("Pipeline integration: PASS")


if __name__ == "__main__":
    run()
