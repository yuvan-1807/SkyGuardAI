"""SkyGuard AI - Phase 3D spatial consistency evidence.

Compares an observation with trusted observations from geographically nearby
AWS stations.  The layer is advisory: it produces spatial residual evidence
for the later Decision Fusion engine and does not independently change the
legacy anomaly verdict.

Design:
- fixed prototype station coordinates from the SIH 10-station simulator;
- nearest-neighbour selection by haversine distance;
- event-time aware trusted-observation lookup (never arrival/timestamp order);
- climatology-adjusted spatial transfer so station climate differences are not
  treated as faults;
- robust residual calibration and neighbour-consensus confidence;
- insufficient spatial coverage returns "available=False" rather than inventing
  evidence.
"""
from __future__ import annotations

import math
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

PARAMETERS: Tuple[str, ...] = ("temperature", "pressure", "humidity")
BOUNDS = {
    "temperature": (-60.0, 70.0),
    "pressure": (870.0, 1100.0),
    "humidity": (0.0, 100.0),
}
SCALE_FLOORS = {"temperature": 1.0, "pressure": 0.8, "humidity": 3.0}
STATION_COORDS = {
    "AWS_01_Delhi": (28.7, 77.2),
    "AWS_02_Mumbai": (19.1, 72.9),
    "AWS_03_Chennai": (13.0, 80.3),
    "AWS_04_Himachal": (32.2, 77.0),
    "AWS_05_Kolkata": (22.6, 88.4),
    "AWS_06_Bangalore": (12.9, 77.6),
    "AWS_07_Kerala": (10.0, 76.3),
    "AWS_08_Rajasthan": (26.9, 75.8),
    "AWS_09_NorthEast": (25.6, 91.8),
    "AWS_10_Gujarat": (23.0, 72.6),
}
MODEL_VERSION = "phase3d-spatial-v1"
DEFAULT_MAX_NEIGHBORS = 3
DEFAULT_TIME_TOLERANCE_HOURS = 3.0


class SpatialConsistencyEvidence:
    """Station-to-station residual evidence engine for the prototype."""

    def __init__(
        self,
        db_path: str,
        max_neighbors: int = DEFAULT_MAX_NEIGHBORS,
        time_tolerance_hours: float = DEFAULT_TIME_TOLERANCE_HOURS,
    ):
        self.db_path = db_path
        self.max_neighbors = max(1, int(max_neighbors))
        self.time_tolerance_hours = max(0.05, float(time_tolerance_hours))

    @staticmethod
    def _parse_time(value: Any) -> datetime:
        text = str(value or "").strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            else:
                return datetime.now()
        return dt.replace(tzinfo=None)

    @staticmethod
    def _finite(value: Any) -> Optional[float]:
        try:
            x = float(value)
            return x if math.isfinite(x) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
        lat1, lon1 = map(math.radians, a)
        lat2, lon2 = map(math.radians, b)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        h = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
        return 6371.0088 * 2.0 * math.asin(math.sqrt(min(1.0, h)))

    @staticmethod
    def _robust_scale(values: Iterable[float], floor: float) -> float:
        vals = [float(v) for v in values if v is not None and math.isfinite(float(v))]
        if len(vals) < 2:
            return float(floor)
        arr = np.asarray(vals, dtype=float)
        med = float(np.median(arr))
        mad = float(np.median(np.abs(arr - med)))
        return max(1.4826 * mad, float(np.std(arr)), float(floor))

    def nearest_stations(self, station: str) -> List[Tuple[str, float]]:
        if station not in STATION_COORDS:
            return []
        origin = STATION_COORDS[station]
        ranked = []
        for name, coords in STATION_COORDS.items():
            if name == station:
                continue
            ranked.append((name, self._haversine_km(origin, coords)))
        ranked.sort(key=lambda item: item[1])
        return ranked[: self.max_neighbors]

    def _trusted_row_near_time(self, station: str, event_time: Any, parameter: str) -> Optional[Dict[str, Any]]:
        if parameter not in PARAMETERS:
            raise ValueError(f"Unsupported parameter: {parameter}")
        tolerance_days = self.time_tolerance_hours / 24.0
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            row = conn.execute(
                """SELECT id, event_time, temperature, pressure, humidity
                   FROM validated_observations
                   WHERE station_name = ?
                     AND ABS(julianday(event_time) - julianday(?)) <= ?
                   ORDER BY ABS(julianday(event_time) - julianday(?)), id DESC
                   LIMIT 1""",
                (station, str(event_time), tolerance_days, str(event_time)),
            ).fetchone()
        except sqlite3.Error:
            return None
        finally:
            if conn is not None:
                conn.close()
        if not row:
            return None
        values = {p: self._finite(row[2 + i]) for i, p in enumerate(PARAMETERS)}
        if any(values[p] is None for p in PARAMETERS):
            return None
        for p, value in values.items():
            lo, hi = BOUNDS[p]
            if not (lo <= value <= hi):
                return None
        return {"id": int(row[0]), "event_time": str(row[1]), **values}

    def _station_context_baseline(self, station: str, parameter: str, event_time: Any) -> Optional[float]:
        target_dt = self._parse_time(event_time)
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            rows = conn.execute(
                f"""SELECT {parameter}, event_time
                    FROM validated_observations
                    WHERE station_name = ? AND event_time < ?
                    ORDER BY event_time DESC
                    LIMIT 1000""",
                (station, str(event_time)),
            ).fetchall()
        except sqlite3.Error:
            return None
        finally:
            if conn is not None:
                conn.close()
        values = []
        for value, ts in rows:
            v = self._finite(value)
            if v is None:
                continue
            dt = self._parse_time(ts)
            if dt.month == target_dt.month:
                values.append(v)
        return float(np.median(values)) if values else None

    def _fallback_scale(self, station: str, parameter: str, event_time: Any) -> float:
        target_dt = self._parse_time(event_time)
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            rows = conn.execute(
                f"""SELECT {parameter}, event_time
                    FROM validated_observations
                    WHERE station_name = ? AND event_time < ?
                    ORDER BY event_time DESC LIMIT 500""",
                (station, str(event_time)),
            ).fetchall()
        except sqlite3.Error:
            return SCALE_FLOORS[parameter]
        finally:
            if conn is not None:
                conn.close()
        vals = [self._finite(v) for v, ts in rows if self._parse_time(ts).month == target_dt.month]
        return self._robust_scale(vals, SCALE_FLOORS[parameter])

    def analyze(self, station: str, event_time: Any, temperature: float, pressure: float, humidity: float) -> Dict[str, Any]:
        values = {"temperature": self._finite(temperature), "pressure": self._finite(pressure), "humidity": self._finite(humidity)}
        if station not in STATION_COORDS:
            return {
                "available": False,
                "station_id": station,
                "event_time": str(event_time),
                "model_version": MODEL_VERSION,
                "anomaly_candidate": False,
                "confidence": 0.0,
                "reason": "Spatial evidence unavailable: station coordinates are not registered",
                "neighbor_count": 0,
                "neighbors": [],
            }
        invalid = [p for p, v in values.items() if v is None]
        invalid += [p for p, v in values.items() if v is not None and not (BOUNDS[p][0] <= v <= BOUNDS[p][1])]
        if invalid:
            return {
                "available": False,
                "station_id": station,
                "event_time": str(event_time),
                "model_version": MODEL_VERSION,
                "anomaly_candidate": False,
                "confidence": 0.0,
                "reason": f"Spatial evidence unavailable: invalid {', '.join(dict.fromkeys(invalid))} input",
                "neighbor_count": 0,
                "neighbors": [],
            }

        neighbor_details = []
        for neighbor, distance_km in self.nearest_stations(station):
            row = self._trusted_row_near_time(neighbor, event_time, "temperature")
            if row is None:
                continue
            detail = {
                "station_id": neighbor,
                "distance_km": round(distance_km, 2),
                "event_time": row["event_time"],
                "values": {p: round(float(row[p]), 4) for p in PARAMETERS},
                "context_baseline": {},
            }
            for p in PARAMETERS:
                detail["context_baseline"][p] = self._station_context_baseline(neighbor, p, event_time)
            if any(detail["context_baseline"][p] is None for p in PARAMETERS):
                continue
            neighbor_details.append(detail)

        if not neighbor_details:
            return {
                "available": False,
                "station_id": station,
                "event_time": str(event_time),
                "model_version": MODEL_VERSION,
                "anomaly_candidate": False,
                "confidence": 0.0,
                "reason": "No trusted nearby observations available within event-time tolerance",
                "neighbor_count": 0,
                "neighbors": [],
            }

        parameter_results = {}
        strongest = None
        for p in PARAMETERS:
            target_baseline = self._station_context_baseline(station, p, event_time)
            if target_baseline is None:
                continue
            weighted_sum = 0.0
            weight_sum = 0.0
            adjusted_estimates = []
            neighbour_weights = []
            for detail in neighbor_details:
                nbase = detail["context_baseline"][p]
                observed = detail["values"][p]
                adjusted = target_baseline + (observed - nbase)
                weight = 1.0 / max(detail["distance_km"], 1.0) ** 2
                weighted_sum += weight * adjusted
                weight_sum += weight
                adjusted_estimates.append(adjusted)
                neighbour_weights.append(weight)
            if weight_sum <= 0:
                continue
            expected = weighted_sum / weight_sum
            observed = float(values[p])
            residual = observed - expected
            spatial_spread = self._robust_scale(adjusted_estimates, SCALE_FLOORS[p] / 2.0)
            baseline_scale = self._fallback_scale(station, p, event_time)
            scale = max(spatial_spread, 0.35 * baseline_scale, SCALE_FLOORS[p])
            normalized = abs(residual) / max(scale, 1e-6)
            candidate = bool(len(neighbor_details) >= 2 and normalized >= 4.0)
            consensus = 1.0 - min(1.0, float(np.std(adjusted_estimates)) / max(scale * 2.0, 1e-6))
            support = min(1.0, len(neighbor_details) / 3.0)
            confidence = 0.0
            if candidate:
                confidence = min(100.0, (58.0 + max(0.0, normalized - 4.0) * 9.0) * (0.65 + 0.35 * support) * (0.75 + 0.25 * consensus))
            parameter_results[p] = {
                "available": True,
                "parameter": p,
                "observed": round(observed, 4),
                "expected_spatial": round(expected, 4),
                "residual": round(residual, 4),
                "absolute_residual": round(abs(residual), 4),
                "normalized_residual": round(float(normalized), 4),
                "residual_scale": round(float(scale), 4),
                "anomaly_threshold_sigma": 4.0,
                "anomaly_candidate": candidate,
                "confidence": round(float(confidence), 2),
                "neighbor_count": len(neighbor_details),
                "neighbor_consensus": round(float(consensus), 4),
                "method": "Climatology-adjusted inverse-distance weighted spatial residual",
                "reason": (
                    f"{p.title()} differs from nearby-station spatial expectation by {residual:+.3f}"
                    if candidate else
                    f"{p.title()} is consistent with nearby-station spatial context"
                ),
            }
            if candidate and (strongest is None or confidence > strongest[1]):
                strongest = (p, confidence)

        candidate = bool(strongest)
        confidence = float(strongest[1]) if strongest else 0.0
        reason = (
            f"Isolated spatial deviation dominated by {strongest[0]} across trusted nearby stations"
            if strongest else
            "Observed values are consistent with trusted nearby-station spatial context"
        )
        return {
            "available": True,
            "station_id": station,
            "event_time": str(event_time),
            "model_version": MODEL_VERSION,
            "anomaly_candidate": candidate,
            "confidence": round(confidence, 2),
            "strongest_parameter": strongest[0] if strongest else None,
            "reason": reason,
            "neighbor_count": len(neighbor_details),
            "neighbors": neighbor_details,
            "parameters": parameter_results,
            "time_tolerance_hours": self.time_tolerance_hours,
            "method": "Multi-station spatial consistency evidence (Phase 3D)",
        }


__all__ = ["SpatialConsistencyEvidence", "STATION_COORDS"]
