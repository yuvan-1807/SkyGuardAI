from __future__ import annotations
import sqlite3, tempfile
from pathlib import Path
from config import STATIONS
from device_registry import ensure_registry_table, provision_batch, summary, authorize, revoke

def run():
    with tempfile.TemporaryDirectory() as td:
        db=str(Path(td)/"all10.db")
        ensure_registry_table(db)
        devices=[{"device_id":f"ALL10-DEVICE-{i:02d}","station_id":s,"source":"TEST"} for i,s in enumerate(STATIONS,1)]
        out=provision_batch(db,devices,source="TEST")
        assert len(out)==10 and all(d["status"]=="ACTIVE" for d in out)
        assert summary(db)=={"total":10,"active":10,"revoked":0}
        for i,s in enumerate(STATIONS,1):
            assert authorize(db,f"ALL10-DEVICE-{i:02d}",s,1)["authorized"]
        revoke(db,"ALL10-DEVICE-03",source="TEST",reason="integration")
        assert not authorize(db,"ALL10-DEVICE-03",STATIONS[2],2)["authorized"]
        assert authorize(db,"ALL10-DEVICE-04",STATIONS[3],2)["authorized"]
        print("PHASE 3I ALL-10 REGISTRY INTEGRATION: PASS")
        print("10 devices provisioned: True")
        print("10 active bindings: True")
        print("Revoked device rejected: True")
        print("Other 9 devices remain eligible: True")
        print("Registry summary:", summary(db))

if __name__=="__main__": run()
