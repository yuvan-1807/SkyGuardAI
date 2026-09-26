import math
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta

from extra_trees_evidence import ExtraTreesEvidence, MODEL_VERSION
from anomaly_pipeline import AnomalyPipeline


def make_db(path: str):
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_name TEXT,
            station_id TEXT,
            timestamp TEXT,
            event_time TEXT,
            receive_time TEXT,
            temperature REAL,
            pressure REAL,
            humidity REAL,
            rainfall REAL,
            label INTEGER,
            is_anomaly INTEGER DEFAULT 0,
            anomaly_reason TEXT,
            confidence REAL,
            detection_source TEXT,
            delivery_mode TEXT,
            secure_authenticated INTEGER,
            device_id TEXT,
            sequence INTEGER,
            message_id TEXT,
            classification TEXT,
            source TEXT,
            model_version TEXT,
            anomaly_score REAL,
            is_clean INTEGER DEFAULT 1,
            validation_status TEXT DEFAULT 'VALIDATED',
            imputation_status TEXT DEFAULT 'NONE',
            quarantine_status TEXT DEFAULT 'NONE',
            created_at TEXT
        )"""
    )
    start = datetime(2025, 1, 1)
    for i in range(240):
        dt = start + timedelta(days=i)
        # Smooth synthetic annual cycle with small deterministic variation.
        temp = 28.0 + 5.0 * math.sin(2 * math.pi * i / 120.0) + 0.15 * math.sin(2 * math.pi * i / 7.0)
        pressure = 1012.0 - 3.0 * math.sin(2 * math.pi * i / 90.0) + 0.1 * math.cos(2 * math.pi * i / 11.0)
        humidity = 68.0 - 8.0 * math.sin(2 * math.pi * i / 120.0) + 0.4 * math.cos(2 * math.pi * i / 8.0)
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """INSERT INTO sensor_readings
            (station_name,station_id,timestamp,event_time,receive_time,temperature,pressure,humidity,
             is_anomaly,is_clean,validation_status,quarantine_status,source,created_at)
            VALUES (?,?,?,?,?,?,?,?,0,1,'VALIDATED','NONE','TEST',?)""",
            ('AWS_TEST', 'AWS_TEST', ts, ts, ts, temp, pressure, humidity, ts),
        )
    conn.execute("""CREATE VIEW validated_observations AS
      SELECT id, station_name, station_id, COALESCE(event_time,timestamp) AS event_time, receive_time,
             device_id, sequence, message_id, temperature, pressure, humidity, rainfall,
             classification, source, model_version, anomaly_score, is_clean, validation_status,
             imputation_status, quarantine_status
      FROM sensor_readings
      WHERE temperature IS NOT NULL AND pressure IS NOT NULL AND humidity IS NOT NULL
        AND COALESCE(is_clean,0)=1
        AND COALESCE(validation_status,'')='VALIDATED'
        AND COALESCE(imputation_status,'NONE') IN ('NONE','ORIGINAL','NOT_IMPUTED')
        AND COALESCE(quarantine_status,'NONE')='NONE'""")
    conn.commit(); conn.close()


def main():
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, 'test.db')
        make_db(db)
        engine = ExtraTreesEvidence(db, min_samples=90, n_estimators=100)
        normal = engine.analyze('AWS_TEST', '2025-09-01 00:00:00', 27.2, 1010.0, 69.0)
        assert normal['available'] is True, normal
        assert normal['model_version'] == MODEL_VERSION
        assert set(normal['parameters']) == {'temperature', 'pressure', 'humidity'}
        assert all('expected' in v and 'residual' in v for v in normal['parameters'].values()), normal
        assert normal['anomaly_candidate'] is False, normal

        anomaly = engine.analyze('AWS_TEST', '2025-09-01 00:00:00', 55.0, 1010.0, 69.0, targets=['temperature'])
        t = anomaly['parameters']['temperature']
        assert t['available'] is True, t
        assert t['anomaly_candidate'] is True, t
        assert t['residual'] > 10.0, t

        invalid = engine.analyze('AWS_TEST', '2025-09-01 00:00:00', 55.0, 1010.0, 120.0, targets=['temperature'])
        assert invalid['parameters']['temperature']['available'] is False, invalid

        too_small = ExtraTreesEvidence(db, min_samples=500).analyze('AWS_TEST', '2025-09-01 00:00:00', 30.0, 1010.0, 70.0)
        assert too_small['available'] is False, too_small

        pipeline = AnomalyPipeline(db_path=db)
        piped = pipeline.analyze_reading('AWS_TEST', 55.0, 1010.0, 69.0, event_time='2025-09-01 00:00:00')
        ml = piped['layers']['ml_evidence']
        assert ml['available'] is True, ml
        assert ml['event_time'] == '2025-09-01 00:00:00', ml
        assert ml['parameters']['temperature']['anomaly_candidate'] is True, ml

    print('PHASE 3C EXTRATREES TESTS: PASS')
    print('Normal ML candidate:', normal['anomaly_candidate'])
    print('Temperature anomaly candidate:', anomaly['parameters']['temperature']['anomaly_candidate'])
    print('Invalid companion handling:', invalid['parameters']['temperature']['available'])
    print('Pipeline integration:', ml['available'], ml['parameters']['temperature']['anomaly_candidate'])


if __name__ == '__main__':
    main()
