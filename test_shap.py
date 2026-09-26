"""Isolated smoke test for the real SHAP explanation layer.

The test builds a temporary SQLite database from the bundled AWS dataset, so it
never depends on or mutates the user's live aws_data.db.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd

from shap_explainer import SHAPExplainer


def main() -> None:
    root = Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory(prefix="skyguard_shap_") as td:
        db = str(Path(td) / "shap.db")
        df = pd.read_csv(root / "10_AWS_stations_combined.csv")
        # Keep the smoke test compact and deterministic while preserving >50
        # normal observations per station, which is enough for the explainer.
        df = df.groupby("station", sort=False).head(120).reset_index(drop=True)
        parsed = pd.to_datetime(df["date"], format="mixed", dayfirst=True, errors="coerce")
        if parsed.isna().any():
            raise AssertionError("Bundled CSV has invalid date values")
        conn = sqlite3.connect(db)
        try:
            conn.execute("""CREATE TABLE sensor_readings(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_name TEXT, timestamp TEXT,
                temperature REAL, pressure REAL, humidity REAL,
                is_anomaly INTEGER DEFAULT 0
            )""")
            rows = [
                (
                    str(r["station"]),
                    str(parsed.loc[idx].strftime("%Y-%m-%d %H:%M:%S")),
                    float(r["temperature"]), float(r["pressure"]), float(r["humidity"]),
                    int(r.get("label", 0) or 0),
                )
                for idx, r in df.iterrows()
            ]
            conn.executemany(
                "INSERT INTO sensor_readings(station_name,timestamp,temperature,pressure,humidity,is_anomaly) VALUES(?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
            row = conn.execute(
                "SELECT station_name,temperature,pressure,humidity FROM sensor_readings ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        station, temp, pressure, humidity = row
        result = SHAPExplainer(db).calculate_shap_values(station, float(temp), float(pressure), float(humidity))
        assert result["method"].startswith("SHAP TreeExplainer")
        assert isinstance(result["shap_values"], dict) and set(result["shap_values"]) == {"temperature", "pressure", "humidity"}
        assert isinstance(result["contribution_percent"], dict)
        print("SHAP smoke test: PASS")
        print("Library:", result["library_version"])
        print("Model verdict:", result["model_prediction"])
        print("Primary feature:", result.get("primary_cause"))
        print("Contributions (%):", result["contribution_percent"])


if __name__ == "__main__":
    main()
