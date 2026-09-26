"""Smoke tests for Phase 2C improved self-healing."""
from pathlib import Path
import sqlite3
import tempfile

import pandas as pd

from self_healing_v3 import SelfHealingAdvisorV3


def build_db(csv_path: str, db_path: str):
    df = pd.read_csv(csv_path)
    parsed = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    with sqlite3.connect(db_path) as conn:
        conn.execute("""CREATE TABLE sensor_readings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_name TEXT, timestamp TEXT,
            temperature REAL, pressure REAL, humidity REAL,
            is_anomaly INTEGER DEFAULT 0)""")
        for _, r in df.iterrows():
            conn.execute(
                "INSERT INTO sensor_readings(station_name,timestamp,temperature,pressure,humidity,is_anomaly) VALUES (?,?,?,?,?,0)",
                (r["station"], parsed.loc[r.name].strftime("%Y-%m-%d %H:%M:%S"), float(r["temperature"]), float(r["pressure"]), float(r["humidity"]))
            )
        conn.commit()


def main():
    root = Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "test.db")
        build_db(str(root / "10_AWS_stations_combined.csv"), db)
        advisor = SelfHealingAdvisorV3(db)
        result = advisor.preview(
            "AWS_03_Chennai",
            "2023-12-31 00:00:00",
            {"temperature": 58.0, "pressure": 1070.0, "humidity": 15.0},
        )
        assert result["raw_observation_preserved"] is True
        assert set(result["parameters"]) == {"temperature", "pressure", "humidity"}
        for p, item in result["parameters"].items():
            assert item["corrected_value"] is not None, p
            lo, hi = item["guardrail"]["bounds"]
            assert lo <= item["corrected_value"] <= hi
        print("Phase 2C improved self-healing smoke test: PASS")
        for p, item in result["parameters"].items():
            print(f"{p}: observed={item['observed']}, suggested={item['corrected_value']}, confidence={item['confidence']}%, method={item['method']}")


if __name__ == "__main__":
    main()
