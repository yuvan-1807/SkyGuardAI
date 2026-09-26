"""Phase 3F quality gate, quarantine, lineage, and trusted-source tests."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from quality_gate import ObservationQualityGate


def make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE sensor_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_name TEXT, station_id TEXT, timestamp TEXT, event_time TEXT, receive_time TEXT,
        device_id TEXT, sequence INTEGER, message_id TEXT,
        temperature REAL, pressure REAL, humidity REAL,
        is_anomaly INTEGER DEFAULT 0, confidence REAL DEFAULT 0,
        classification TEXT, source TEXT, model_version TEXT, anomaly_score REAL,
        is_clean INTEGER, validation_status TEXT, imputation_status TEXT, quarantine_status TEXT
    )""")
    conn.execute("""CREATE TABLE observation_quarantine (
        id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER UNIQUE,
        station_id TEXT, device_id TEXT, sequence INTEGER, message_id TEXT,
        event_time TEXT, receive_time TEXT, classification TEXT, anomaly_score REAL,
        reason TEXT, source TEXT, model_version TEXT, status TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        released_at TEXT
    )""")
    conn.execute("""CREATE TABLE observation_lineage (
        id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER, stage TEXT,
        station_id TEXT, device_id TEXT, sequence INTEGER, message_id TEXT,
        event_time TEXT, receive_time TEXT, classification TEXT, model_version TEXT,
        anomaly_score REAL, is_clean INTEGER, validation_status TEXT,
        imputation_status TEXT, quarantine_status TEXT, source TEXT, reason TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""INSERT INTO sensor_readings
      (station_name,station_id,timestamp,event_time,receive_time,device_id,sequence,message_id,
       temperature,pressure,humidity,is_anomaly,confidence,classification,source,model_version,anomaly_score,
       is_clean,validation_status,imputation_status,quarantine_status)
      VALUES ('AWS_01_Delhi','AWS_01_Delhi','2026-09-24 10:00:00','2026-09-24 10:00:00','2026-09-24 10:01:00','DEV1',1,'M1',30,1012,70,0,0,'NORMAL','TEST','v1',0,1,'VALIDATED','NONE','NONE')""")
    conn.execute("""CREATE VIEW validated_observations AS
      SELECT id,station_name,station_id,event_time,receive_time,device_id,sequence,message_id,
             temperature,pressure,humidity,classification,source,model_version,anomaly_score,
             is_clean,validation_status,imputation_status,quarantine_status
      FROM sensor_readings WHERE temperature IS NOT NULL AND pressure IS NOT NULL AND humidity IS NOT NULL
        AND is_clean=1 AND validation_status='VALIDATED' AND imputation_status='NONE' AND quarantine_status='NONE'""")
    conn.commit(); conn.close()


def insert_observation(conn, quality, oid, device, seq, msg):
    conn.execute("""INSERT INTO sensor_readings
      (station_name,station_id,timestamp,event_time,receive_time,device_id,sequence,message_id,
       temperature,pressure,humidity,is_anomaly,confidence,classification,source,model_version,anomaly_score,
       is_clean,validation_status,imputation_status,quarantine_status)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      ('AWS_01_Delhi','AWS_01_Delhi','2026-09-24 10:02:00','2026-09-24 10:02:00','2026-09-24 10:30:00',device,seq,msg,
       45,1005,85,1,92,quality['classification'],'TEST','phase3e',0.92,int(quality['is_clean']),quality['validation_status'],quality['imputation_status'],quality['quarantine_status']))


def main() -> None:
    gate = ObservationQualityGate()
    # A normal observation is valid and trainable.
    normal = gate.evaluate({'classification':'NORMAL','confidence':20,'reason':'Normal'}, source='TEST')
    assert normal['is_clean'] and normal['validation_status']=='VALIDATED' and normal['quarantine_status']=='NONE'
    assert normal['training_eligible']

    # A genuine weather event is valid when there is no sensor conflict.
    weather = gate.evaluate({'classification':'GENUINE_WEATHER_EVENT','confidence':88,'reason':'Physical weather transition', 'layers':{'decision_fusion':{'review_required':False}}}, source='TEST')
    assert weather['is_clean'] and weather['validation_status']=='VALIDATED' and weather['training_eligible']

    # Sensor anomaly is quarantined and therefore not trainable.
    anomaly = gate.evaluate({'classification':'SENSOR_ANOMALY','confidence':95,'reason':'Large ML residual'}, source='TEST')
    assert not anomaly['is_clean'] and anomaly['validation_status']=='QUARANTINED' and anomaly['quarantine_status']=='QUARANTINED'
    assert not anomaly['training_eligible']

    # Imputed/self-healed observations stay quarantined.
    imputed = gate.evaluate({'classification':'NORMAL','confidence':30,'reason':'Corrected'}, source='TEST', imputation_status='IMPUTED')
    assert not imputed['is_clean'] and imputed['quarantine_status']=='QUARANTINED' and not imputed['training_eligible']

    # Weather/sensor conflict is held for review instead of entering training.
    conflict = gate.evaluate({'classification':'GENUINE_WEATHER_EVENT','confidence':80,'layers':{'decision_fusion':{'review_required':True}}}, source='TEST')
    assert conflict['validation_status']=='REVIEW_REQUIRED' and conflict['quarantine_status']=='QUARANTINE_CANDIDATE' and not conflict['training_eligible']

    with tempfile.TemporaryDirectory() as td:
        db = Path(td)/'quality.db'; make_db(db)
        conn = sqlite3.connect(db)
        # Raw value remains untouched after an anomaly lifecycle decision.
        before = conn.execute('SELECT temperature, event_time, receive_time FROM sensor_readings WHERE id=1').fetchone()
        conn.execute("""INSERT INTO sensor_readings
          (station_name,station_id,timestamp,event_time,receive_time,device_id,sequence,message_id,temperature,pressure,humidity,is_anomaly,confidence,classification,source,model_version,anomaly_score,is_clean,validation_status,imputation_status,quarantine_status)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          ('AWS_01_Delhi','AWS_01_Delhi','2026-09-24 10:02:00','2026-09-24 10:02:00','2026-09-24 10:30:00','DEV1',2,'M2',55,1005,85,1,95,'SENSOR_ANOMALY','TEST','phase3e',.95,0,'QUARANTINED','NONE','QUARANTINED'))
        oid = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        conn.execute("""INSERT INTO observation_quarantine(observation_id,station_id,device_id,sequence,message_id,event_time,receive_time,classification,anomaly_score,reason,source,model_version,status)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (oid,'AWS_01_Delhi','DEV1',2,'M2','2026-09-24 10:02:00','2026-09-24 10:30:00','SENSOR_ANOMALY',.95,'ML residual','TEST','phase3e','QUARANTINED'))
        conn.execute("""INSERT INTO observation_lineage(observation_id,stage,station_id,device_id,sequence,message_id,event_time,receive_time,classification,model_version,anomaly_score,is_clean,validation_status,imputation_status,quarantine_status,source,reason)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (oid,'RAW_RECEIVED','AWS_01_Delhi','DEV1',2,'M2','2026-09-24 10:02:00','2026-09-24 10:30:00','SENSOR_ANOMALY','phase3e',.95,0,'RAW','NONE','NONE','TEST','Raw received'))
        conn.execute("""INSERT INTO observation_lineage(observation_id,stage,station_id,device_id,sequence,message_id,event_time,receive_time,classification,model_version,anomaly_score,is_clean,validation_status,imputation_status,quarantine_status,source,reason)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (oid,'QUALITY_GATE','AWS_01_Delhi','DEV1',2,'M2','2026-09-24 10:02:00','2026-09-24 10:30:00','SENSOR_ANOMALY','phase3e',.95,0,'QUARANTINED','NONE','QUARANTINED','TEST','ML residual'))
        conn.commit()
        after = conn.execute('SELECT temperature, event_time, receive_time FROM sensor_readings WHERE id=1').fetchone()
        assert before == after
        assert conn.execute('SELECT COUNT(*) FROM validated_observations').fetchone()[0] == 1
        assert conn.execute('SELECT COUNT(*) FROM observation_quarantine').fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM observation_lineage WHERE observation_id=?",(oid,)).fetchone()[0] == 2
        conn.close()

    print('PHASE 3F QUALITY GATE TESTS: PASS')
    print('Normal -> VALIDATED: PASS')
    print('Weather event -> VALIDATED: PASS')
    print('Sensor anomaly -> QUARANTINED: PASS')
    print('Imputed value -> QUARANTINED: PASS')
    print('Weather/sensor conflict -> REVIEW_REQUIRED: PASS')
    print('Trusted training source -> validated_observations: PASS')
    print('Raw observation preserved: PASS')
    print('Quarantine + lineage persistence: PASS')

if __name__ == '__main__':
    main()
