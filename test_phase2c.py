"""Small offline smoke test for Phase 2C modules using the bundled CSV."""
from pathlib import Path
import os
import sqlite3
import tempfile
import pandas as pd

from sensor_health_v2 import SensorHealthEngine
from self_healing_v2 import SelfHealingAdvisor
from config import CSV_PATH, STATIONS


def build_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE sensor_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_name TEXT, timestamp TEXT, temperature REAL, pressure REAL,
        humidity REAL, rainfall REAL, label INTEGER DEFAULT 0,
        is_anomaly INTEGER DEFAULT 0, anomaly_reason TEXT,
        confidence REAL DEFAULT 0.0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE edge_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_name TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    df = pd.read_csv(CSV_PATH)
    parsed = pd.to_datetime(df["date"], format="mixed", dayfirst=True, errors="coerce")
    for _, row in df.assign(timestamp=parsed.dt.strftime("%Y-%m-%d %H:%M:%S")).iterrows():
        label = int(row.get("label", 0) or 0)
        conn.execute(
            """INSERT INTO sensor_readings(station_name,timestamp,temperature,pressure,humidity,label,is_anomaly)
               VALUES(?,?,?,?,?,?,?)""",
            (str(row["station"]), str(row["timestamp"]), float(row["temperature"]),
             float(row["pressure"]), float(row["humidity"]), label, label),
        )
    conn.commit()
    conn.close()


def main() -> None:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        build_db(path)
        engine = SensorHealthEngine(path)
        network = engine.all_stations()
        assert len(network["stations"]) == 10
        assert 0 <= network["network_health"] <= 100
        station = engine.station_health(STATIONS[2])
        assert "parameter_health" in station and len(station["parameter_health"]) == 3

        conn = sqlite3.connect(path)
        try:
            latest = conn.execute(
                "SELECT station_name,timestamp,temperature,pressure,humidity FROM sensor_readings ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        advisor = SelfHealingAdvisor(path)
        healing = advisor.preview(
            latest[0], latest[1],
            {"temperature": latest[2], "pressure": latest[3], "humidity": latest[4]},
        )
        assert healing["read_only"] is True
        assert healing["raw_observation_preserved"] is True
        print("Phase 2C smoke test: PASS")
        print(f"Stations: {len(network['stations'])}  Network health: {network['network_health']}%")
        print(f"Sample station: {station['station']} -> {station['health_score']}% / {station['status']}")
        print(f"Healing parameters returned: {len(healing['parameters'])}")
    finally:
        Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
