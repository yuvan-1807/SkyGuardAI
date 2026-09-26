import sqlite3
import requests
from datetime import datetime

DB = "aws_data.db"
API = "http://127.0.0.1:8050/api/ingest"

# Use a station that already has historical data
STATION = "AWS_03_Chennai"

# Read the latest observation so the event is relative to the
# station's actual current conditions.
conn = sqlite3.connect(DB)
row = conn.execute("""
    SELECT temperature, pressure, humidity
    FROM sensor_readings
    WHERE station_name = ?
    ORDER BY event_time DESC, id DESC
    LIMIT 1
""", (STATION,)).fetchone()
conn.close()

if not row:
    print("❌ No historical data found for", STATION)
    input("Press Enter to close...")
    raise SystemExit

old_t, old_p, old_h = map(float, row)

# Coordinated atmospheric transition:
# temperature ↓
# pressure ↓
# humidity ↑
new_t = old_t - 3.0
new_p = old_p - 5.0
new_h = min(95.0, old_h + 12.0)

payload = {
    "station_id": STATION,
    "temperature": round(new_t, 2),
    "pressure": round(new_p, 2),
    "humidity": round(new_h, 2),
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "source": "WEATHER_EVENT_DEMO",
    "detection_source": "Weather Context Engine",
    "delivery_mode": "LIVE"
}

print("\n🌦️ Injecting genuine weather event...")
print(f"Station     : {STATION}")
print(f"Temperature : {old_t:.2f} → {new_t:.2f} °C")
print(f"Pressure    : {old_p:.2f} → {new_p:.2f} hPa")
print(f"Humidity    : {old_h:.2f} → {new_h:.2f} %")

try:
    r = requests.post(API, json=payload, timeout=10)

    print("\nHTTP:", r.status_code)
    print(r.text)

    if r.ok:
        data = r.json()

        if data.get("classification") == "GENUINE_WEATHER_EVENT":
            print("\n✅ WEATHER EVENT INJECTED SUCCESSFULLY!")
            print("   Open Dashboard → Weather Events")
        else:
            print("\n⚠️ Reading accepted, but classification was:")
            print("   ", data.get("classification"))

except Exception as e:
    print("\n❌ Could not connect to SkyGuard:")
    print(e)

input("\nPress Enter to close...")
