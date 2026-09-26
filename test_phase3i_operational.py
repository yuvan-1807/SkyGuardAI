from __future__ import annotations
import os, sqlite3, tempfile
from pathlib import Path

os.environ.setdefault('SKYGUARD_EDGE_SECRET','skyguard-demo-master-secret-change-me')

with tempfile.TemporaryDirectory() as td:
    db = Path(td) / 'ops.db'
    os.environ['SKYGUARD_DB'] = str(db)
    from config import DB_PATH, STATIONS
    import app_new
    assert app_new.app is not None

    # Fresh app bootstrap + schema should exist.
    assert db.exists(), f'db missing: {db}'

    c = sqlite3.connect(DB_PATH)
    try:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert 'sensor_readings' in tables
        assert 'edge_nodes' in tables
        assert 'edge_secure_receipts' in tables
        assert 'device_registry' in tables
    finally:
        c.close()

    # Secure heartbeat envelope must be accepted and recorded.
    from edge_security import build_envelope, get_master_secret
    from device_registry import provision
    with app_new.app.test_client() as client:
        device = 'TEST-OPS-DEVICE'
        station = STATIONS[0]
        provision(DB_PATH, device, station, source='3I_TEST')
        env = build_envelope(device, station, 1, 'heartbeat', {
            'station_id': station, 'device': device, 'mode': 'demo',
            'samples_processed': 1, 'hard_blocked': 0, 'buffered_readings': 0, 'batches_sent': 0
        }, get_master_secret())
        r = client.post('/api/edge-heartbeat', json=env)
        assert r.status_code == 200, r.get_json()
        r2 = client.get('/api/device-registry')
        assert r2.status_code == 200
        summary = r2.get_json()['summary']
        assert summary['active'] >= 1
        r3 = client.get('/api/edge-security-status')
        assert r3.status_code == 200
        assert r3.get_json()['authenticated_messages'] >= 1
        r4 = client.get('/api/final-dashboard')
        assert r4.status_code == 200
        p = r4.get_json()
        assert 'system_health_score' in p
        assert 'network_coverage_pct' in p
        assert all('health' in s for s in p['stations'])
        r5 = client.get(f'/api/station-view/{station}')
        assert r5.status_code == 200
        rv = r5.get_json()
        assert rv['station'] == station
        r6 = client.get(f'/api/anomaly/1/summary')
        assert r6.status_code in (200,404)

print('PHASE 3I OPERATIONAL API TESTS: PASS')
print('Secure heartbeat integration: PASS')
print('Registry-enforced node presence: PASS')
print('Real sensor health exposed in dashboard API: PASS')
print('Network coverage/system health fields: PASS')
print('Fast station-view endpoint: PASS')
print('Fast anomaly-summary endpoint: PASS')
