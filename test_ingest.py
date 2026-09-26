"""Quick Phase-1 integration test.
Run app_new.py first, then run: python test_ingest.py
"""
import requests

URL = "http://127.0.0.1:8050/api/ingest"

payload = {
    "station_id": "AWS_03_Chennai",
    "temperature": 55.0,
    "pressure": 980.2,
    "humidity": 97.0,
    "is_anomaly": True,
    "anomaly_reason": "Manual Phase-1 test injection",
    "confidence": 97.0,
}

r = requests.post(URL, json=payload, timeout=5)
print("HTTP:", r.status_code)
print(r.text)
r.raise_for_status()
