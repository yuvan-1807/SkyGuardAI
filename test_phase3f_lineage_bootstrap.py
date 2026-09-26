import sqlite3
from pathlib import Path

def run():
    db=Path("lineage_bootstrap_test.db")
    if db.exists(): db.unlink()
    c=sqlite3.connect(db)
    c.executescript("""
    CREATE TABLE sensor_readings (id INTEGER PRIMARY KEY, station_name TEXT, station_id TEXT, timestamp TEXT, event_time TEXT, receive_time TEXT, device_id TEXT, sequence INTEGER, message_id TEXT, classification TEXT, model_version TEXT, anomaly_score REAL, is_clean INTEGER, validation_status TEXT, imputation_status TEXT, quarantine_status TEXT, source TEXT);
    CREATE TABLE observation_lineage (id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER, stage TEXT, station_id TEXT, device_id TEXT, sequence INTEGER, message_id TEXT, event_time TEXT, receive_time TEXT, classification TEXT, model_version TEXT, anomaly_score REAL, is_clean INTEGER, validation_status TEXT, imputation_status TEXT, quarantine_status TEXT, source TEXT, reason TEXT);
    """)
    c.execute("INSERT INTO sensor_readings VALUES (1,'AWS_01_Delhi','AWS_01_Delhi','2023-01-01 00:00:00','2023-01-01 00:00:00','2026-09-24 10:00:00',NULL,NULL,NULL,'NORMAL','legacy-validated',0,1,'VALIDATED','NONE','NONE','CSV_BOOTSTRAP')")
    c.commit(); c.close()
    c=sqlite3.connect(db)
    c.execute("""INSERT INTO observation_lineage (observation_id,stage,station_id,device_id,sequence,message_id,event_time,receive_time,classification,model_version,anomaly_score,is_clean,validation_status,imputation_status,quarantine_status,source,reason) SELECT s.id,'MIGRATED',COALESCE(s.station_id,s.station_name),s.device_id,s.sequence,s.message_id,COALESCE(s.event_time,s.timestamp),s.receive_time,s.classification,s.model_version,s.anomaly_score,s.is_clean,s.validation_status,COALESCE(s.imputation_status,'NONE'),COALESCE(s.quarantine_status,'NONE'),COALESCE(s.source,'LEGACY'),'Phase 3F lineage backfill for pre-existing observation' FROM sensor_readings s WHERE NOT EXISTS (SELECT 1 FROM observation_lineage l WHERE l.observation_id=s.id)""")
    c.commit()
    first=c.execute('SELECT COUNT(*) FROM observation_lineage').fetchone()[0]
    c.execute("""INSERT INTO observation_lineage (observation_id,stage,station_id,device_id,sequence,message_id,event_time,receive_time,classification,model_version,anomaly_score,is_clean,validation_status,imputation_status,quarantine_status,source,reason) SELECT s.id,'MIGRATED',COALESCE(s.station_id,s.station_name),s.device_id,s.sequence,s.message_id,COALESCE(s.event_time,s.timestamp),s.receive_time,s.classification,s.model_version,s.anomaly_score,s.is_clean,s.validation_status,COALESCE(s.imputation_status,'NONE'),COALESCE(s.quarantine_status,'NONE'),COALESCE(s.source,'LEGACY'),'Phase 3F lineage backfill for pre-existing observation' FROM sensor_readings s WHERE NOT EXISTS (SELECT 1 FROM observation_lineage l WHERE l.observation_id=s.id)""")
    c.commit()
    second=c.execute('SELECT COUNT(*) FROM observation_lineage').fetchone()[0]
    c.close(); db.unlink()
    assert first==1 and second==1
    print('PHASE 3F FIX1 LINEAGE BACKFILL TEST: PASS')
    print('Initial lineage created: PASS')
    print('Idempotent backfill: PASS')
if __name__=='__main__': run()
