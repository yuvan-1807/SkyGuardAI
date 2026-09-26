"""SkyGuard Phase 2C - Read-only self-healing suggestions.

The engine never overwrites raw observations. It produces a proposed value
from trusted temporal history and, when available, spatial neighbour readings.
"""
from __future__ import annotations

from db_utils import db_connect

from datetime import datetime
import math
import sqlite3
from typing import Any, Dict, Optional, Tuple

import numpy as np

from config import DB_PATH

PARAMETERS = ("temperature", "pressure", "humidity")
BOUNDS = {
    "temperature": (-60.0, 70.0),
    "pressure": (870.0, 1100.0),
    "humidity": (0.0, 100.0),
}
STATION_COORDS = {
    'AWS_01_Delhi': (28.7, 77.2), 'AWS_02_Mumbai': (19.1, 72.9), 'AWS_03_Chennai': (13.0, 80.3),
    'AWS_04_Himachal': (32.2, 77.0), 'AWS_05_Kolkata': (22.6, 88.4), 'AWS_06_Bangalore': (12.9, 77.6),
    'AWS_07_Kerala': (10.0, 76.3), 'AWS_08_Rajasthan': (26.9, 75.8), 'AWS_09_NorthEast': (25.6, 91.8),
    'AWS_10_Gujarat': (23.0, 72.6),
}


class SelfHealingAdvisor:
    def __init__(self, db_path: str = DB_PATH, max_neighbors: int = 3):
        self.db_path = db_path
        self.max_neighbors = max_neighbors

    def _nearest(self, station: str):
        if station not in STATION_COORDS:
            return []
        lat, lon = STATION_COORDS[station]
        items = []
        for name, (nlat, nlon) in STATION_COORDS.items():
            if name == station:
                continue
            d = math.hypot(lat - nlat, lon - nlon)
            items.append((name, max(d, 1e-6)))
        items.sort(key=lambda x: x[1])
        return items[: self.max_neighbors]

    def _trusted_temporal(self, station: str, parameter: str, limit: int = 40):
        with db_connect(self.db_path, timeout=10) as conn:
            rows = conn.execute(
                f"""SELECT {parameter} FROM sensor_readings
                    WHERE station_name = ? AND COALESCE(is_anomaly,0)=0 AND {parameter} IS NOT NULL
                    ORDER BY id DESC LIMIT ?""",
                (station, limit),
            ).fetchall()
        return [float(r[0]) for r in rows if r[0] is not None and math.isfinite(float(r[0]))]

    def _neighbor_value(self, station: str, timestamp: str, parameter: str, distance: float) -> Optional[float]:
        try:
            with db_connect(self.db_path, timeout=10) as conn:
                row = conn.execute(
                    f"""SELECT {parameter}, timestamp FROM sensor_readings
                        WHERE station_name = ? AND COALESCE(is_anomaly,0)=0 AND {parameter} IS NOT NULL
                          AND ABS(julianday(timestamp) - julianday(?)) <= 0.25
                        ORDER BY ABS(julianday(timestamp) - julianday(?)) ASC, id DESC LIMIT 1""",
                    (station, timestamp, timestamp),
                ).fetchone()
        except sqlite3.Error:
            row = None
        if not row:
            return None
        try:
            value = float(row[0])
            return value if math.isfinite(value) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _median(values):
        if not values:
            return None
        return float(np.median(np.asarray(values, dtype=float)))

    def estimate_parameter(self, station: str, timestamp: str, parameter: str, observed: float) -> Dict[str, Any]:
        temporal = self._trusted_temporal(station, parameter)
        temporal_est = self._median(temporal)
        neighbors = {}
        for name, distance in self._nearest(station):
            v = self._neighbor_value(name, timestamp, parameter, distance)
            if v is not None:
                neighbors[name] = {"value": v, "distance": distance}

        spatial_est = None
        if neighbors:
            num = 0.0
            den = 0.0
            for item in neighbors.values():
                w = 1.0 / (item["distance"] ** 2)
                num += w * item["value"]
                den += w
            spatial_est = num / den if den else None

        if temporal_est is not None and spatial_est is not None:
            corrected = 0.6 * temporal_est + 0.4 * spatial_est
            method = "Temporal median + spatial IDW"
        elif temporal_est is not None:
            corrected = temporal_est
            method = "Trusted temporal median"
        elif spatial_est is not None:
            corrected = spatial_est
            method = "Spatial IDW"
        else:
            corrected = None
            method = "No trusted reference available"

        confidence = 35.0
        if len(temporal) >= 20:
            confidence += 30.0
        elif len(temporal) >= 10:
            confidence += 15.0
        if len(neighbors) >= 2:
            confidence += 20.0
        elif len(neighbors) == 1:
            confidence += 8.0
        if temporal_est is not None and spatial_est is not None:
            scale = max(abs(temporal_est) * 0.05, 1.0)
            agreement = max(0.0, 1.0 - abs(temporal_est - spatial_est) / scale)
            confidence += 15.0 * agreement
        confidence = min(95.0, round(confidence, 1))

        if corrected is not None:
            lo, hi = BOUNDS[parameter]
            corrected = min(hi, max(lo, corrected))
            corrected = round(float(corrected), 2)

        return {
            "parameter": parameter,
            "observed": round(float(observed), 2),
            "corrected_value": corrected,
            "method": method,
            "confidence": confidence if corrected is not None else 0.0,
            "trusted_temporal_samples": len(temporal),
            "neighbor_count": len(neighbors),
            "neighboring_values": {k: round(v["value"], 2) for k, v in neighbors.items()},
            "temporal_reference": None if temporal_est is None else round(temporal_est, 2),
            "spatial_reference": None if spatial_est is None else round(spatial_est, 2),
        }

    def preview(self, station: str, timestamp: str, values: Dict[str, float], parameters=None) -> Dict[str, Any]:
        parameters = tuple(parameters or PARAMETERS)
        results = {}
        for p in parameters:
            if p not in PARAMETERS or p not in values:
                continue
            results[p] = self.estimate_parameter(station, timestamp, p, float(values[p]))
        usable = [r for r in results.values() if r.get("corrected_value") is not None]
        return {
            "station": station,
            "timestamp": timestamp,
            "read_only": True,
            "raw_observation_preserved": True,
            "parameters": results,
            "overall_confidence": round(float(np.mean([r["confidence"] for r in usable])) if usable else 0.0, 1),
            "note": "This is a correction suggestion only. The raw sensor observation is never overwritten.",
        }
