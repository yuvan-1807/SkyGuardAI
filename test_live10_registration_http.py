from __future__ import annotations
import json
import threading
import tempfile
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import live_network_simulator as sim
from config import STATIONS

calls = []
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_POST(self):
        n = int(self.headers.get('Content-Length','0'))
        body = json.loads(self.rfile.read(n) or b'{}')
        calls.append((self.path, body))
        if self.path == '/api/device-registry/provision-batch':
            payload = body.get('devices', [])
            # Simulate an actual API response for all stations.
            out=[]
            for d in payload:
                out.append({"device_id":d["device_id"],"station_id":d["station_id"],"status":"ACTIVE"})
            raw=json.dumps({"status":"ok","count":len(out),"devices":out}).encode()
            self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
        else:
            self.send_response(404); self.end_headers()

with tempfile.TemporaryDirectory() as td:
    srv=HTTPServer(('127.0.0.1',0), Handler)
    th=threading.Thread(target=srv.serve_forever,daemon=True); th.start()
    port=srv.server_address[1]
    sim.DEVICE_PROVISION_BATCH_URL=f'http://127.0.0.1:{port}/api/device-registry/provision-batch'
    sim.DEVICE_PROVISION_URL=f'http://127.0.0.1:{port}/api/device-registry/provision'
    s=sim.LiveNetworkSimulator(secure=True, queue_dir=str(Path(td)/'.edge_outbox'), demo=True)
    s.register_secure_devices()
    assert len(calls)==1, calls
    assert calls[0][0].endswith('/device-registry/provision-batch')
    devices=calls[0][1]['devices']
    assert len(devices)==10
    assert {d['station_id'] for d in devices}==set(STATIONS)
    assert len({d['device_id'] for d in devices})==10
    srv.shutdown()
print('LIVE 10-STATION REGISTRATION HTTP TEST: PASS')
print('Single batch enrollment request: True')
print('10 unique device identities submitted: True')
print('All 10 stations included: True')
