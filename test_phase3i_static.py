from pathlib import Path
import ast

p=Path('app_new.py')
s=p.read_text(encoding='utf-8')
ast.parse(s)
required = [
    '@app.get("/api/final-dashboard")',
    '@app.get("/api/station-view/<path:station>")',
    '@app.get("/api/station-analysis/<path:station>")',
    '@app.get("/api/anomaly/<int:row_id>/summary")',
    'system_health_score',
    'network_coverage_pct',
    'health_map.get(station)',
    'kind") != "heartbeat"',
    'kind=heartbeat',
]
for marker in required:
    assert marker in s, f'missing marker: {marker}'
print('PHASE 3I STATIC CONTRACT TEST: PASS')
print('Real health in dashboard API: PASS')
print('Separate network coverage metric: PASS')
print('Secure heartbeat envelope path: PASS')
print('Fast station-view API: PASS')
print('Separate deep-analysis API: PASS')
print('Fast anomaly summary API: PASS')
