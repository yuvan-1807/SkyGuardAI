"""SkyGuard Phase 2C - Sensor health and degradation analysis.

This module is additive: it reads telemetry and edge-event history and computes
an explainable health snapshot without modifying the ingestion path or raw
sensor records.
"""
from __future__ import annotations

from db_utils import db_connect

from dataclasses import dataclass
from datetime import datetime, timedelta
import math
import sqlite3
from typing import Any, Dict, Iterable, List, Optional

import numpy as np

from config import DB_PATH, STATIONS

PARAMETERS = ("temperature", "pressure", "humidity")
PARAM_LABELS = {
    "temperature": "Temperature",
    "pressure": "Pressure",
    "humidity": "Humidity",
}


@dataclass
class WindowMetrics:
    total: int
    anomalies: int
    anomaly_rate: float
    drift: Dict[str, float]
    freeze: Dict[str, bool]
    invalid: Dict[str, int]
    gap_rate: float


class SensorHealthEngine:
    """Compute transparent sensor-health and degradation indicators."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def _fetch_rows(self, station: str, limit: int = 240, offset: int = 0) -> List[tuple]:
        with db_connect(self.db_path, timeout=10) as conn:
            return conn.execute(
                """SELECT id, timestamp, created_at, temperature, pressure, humidity, is_anomaly
                   FROM sensor_readings
                   WHERE station_name = ?
                   ORDER BY id DESC
                   LIMIT ? OFFSET ?""",
                (station, int(limit), int(offset)),
            ).fetchall()

    @staticmethod
    def _robust_drift(values: Iterable[float]) -> float:
        arr = np.asarray(list(values), dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size < 20:
            return 0.0
        mid = arr.size // 2
        a = arr[:mid]
        b = arr[mid:]
        median = float(np.median(arr))
        mad = float(np.median(np.abs(arr - median)))
        scale = max(1.4826 * mad, float(np.std(arr)), 1e-3)
        return abs(float(np.median(b)) - float(np.median(a))) / scale

    @staticmethod
    def _freeze(values: Iterable[float], window: int = 8, tolerance: float = 1e-5) -> bool:
        arr = [float(v) for v in values if v is not None and math.isfinite(float(v))]
        if len(arr) < window:
            return False
        tail = np.asarray(arr[-window:], dtype=float)
        return float(np.var(tail)) <= tolerance

    @staticmethod
    def _invalid_counts(rows: List[tuple]) -> Dict[str, int]:
        counts = {p: 0 for p in PARAMETERS}
        bounds = {
            "temperature": (-60.0, 70.0),
            "pressure": (870.0, 1100.0),
            "humidity": (0.0, 100.0),
        }
        idx = {"temperature": 3, "pressure": 4, "humidity": 5}
        for row in rows:
            for p in PARAMETERS:
                value = row[idx[p]]
                try:
                    value_f = float(value)
                except (TypeError, ValueError):
                    counts[p] += 1
                    continue
                lo, hi = bounds[p]
                if not math.isfinite(value_f) or value_f < lo or value_f > hi:
                    counts[p] += 1
        return counts

    @staticmethod
    def _gap_rate(rows: List[tuple]) -> float:
        # Use ingestion creation times because the supplied historical dataset
        # and demo stream can have very different sensor timestamps.
        parsed = []
        for r in reversed(rows):
            try:
                parsed.append(datetime.fromisoformat(str(r[2]).replace("Z", "+00:00")).replace(tzinfo=None))
            except Exception:
                continue
        if len(parsed) < 8:
            return 0.0
        intervals = np.asarray([(b - a).total_seconds() for a, b in zip(parsed, parsed[1:])], dtype=float)
        intervals = intervals[intervals > 0]
        if intervals.size < 6:
            return 0.0
        baseline = float(np.median(intervals))
        if baseline <= 0:
            return 0.0
        return float(np.mean(intervals > max(60.0, baseline * 6.0)))

    def _window_metrics(self, rows: List[tuple]) -> WindowMetrics:
        if not rows:
            return WindowMetrics(0, 0, 0.0, {p: 0.0 for p in PARAMETERS}, {p: False for p in PARAMETERS}, {p: 0 for p in PARAMETERS}, 0.0)
        drift = {}
        freeze = {}
        for p, idx in (("temperature", 3), ("pressure", 4), ("humidity", 5)):
            values = [r[idx] for r in reversed(rows) if r[idx] is not None]
            drift[p] = self._robust_drift(values)
            freeze[p] = self._freeze(values)
        anomalies = sum(int(r[6] or 0) for r in rows)
        return WindowMetrics(
            total=len(rows),
            anomalies=anomalies,
            anomaly_rate=(anomalies / len(rows)) if rows else 0.0,
            drift=drift,
            freeze=freeze,
            invalid=self._invalid_counts(rows),
            gap_rate=self._gap_rate(rows),
        )

    @staticmethod
    def _status(score: float) -> str:
        if score >= 90:
            return "HEALTHY"
        if score >= 75:
            return "GOOD"
        if score >= 55:
            return "DEGRADED"
        if score >= 35:
            return "POOR"
        return "CRITICAL"

    @staticmethod
    def _trend_label(delta: float) -> str:
        if delta <= -8:
            return "DEGRADING"
        if delta <= -3:
            return "WATCH"
        if delta >= 6:
            return "IMPROVING"
        return "STABLE"

    def _score_window(self, rows: List[tuple], hard_events: int = 0) -> Dict[str, Any]:
        m = self._window_metrics(rows)
        if not rows:
            return {
                "score": 50.0, "status": "UNKNOWN", "metrics": m,
                "penalties": {}, "parameter_health": {},
            }

        anomaly_penalty = min(35.0, m.anomaly_rate * 70.0)
        hard_penalty = min(20.0, hard_events * 4.0)
        drift_penalty = min(20.0, max(m.drift.values()) * 4.0)
        freeze_penalty = 25.0 if any(m.freeze.values()) else 0.0
        gap_penalty = min(12.0, m.gap_rate * 30.0)
        invalid_total = sum(m.invalid.values())
        invalid_penalty = min(10.0, (invalid_total / max(1, m.total)) * 100.0)

        score = max(0.0, min(100.0, 100.0 - anomaly_penalty - hard_penalty - drift_penalty - freeze_penalty - gap_penalty - invalid_penalty))

        param_health: Dict[str, float] = {}
        for p in PARAMETERS:
            p_score = 100.0
            p_score -= min(35.0, m.drift[p] * 7.0)
            if m.freeze[p]:
                p_score -= 45.0
            p_score -= min(20.0, (m.invalid[p] / max(1, m.total)) * 100.0)
            # Overall anomaly pressure is deliberately modest at the individual
            # parameter level because the database stores a joint verdict.
            p_score -= min(15.0, m.anomaly_rate * 30.0)
            param_health[p] = max(0.0, min(100.0, p_score))

        return {
            "score": round(score, 1),
            "status": self._status(score),
            "metrics": m,
            "penalties": {
                "anomaly_rate": round(anomaly_penalty, 2),
                "hard_edge_events": round(hard_penalty, 2),
                "drift": round(drift_penalty, 2),
                "freeze": round(freeze_penalty, 2),
                "communication_gaps": round(gap_penalty, 2),
                "invalid_values": round(invalid_penalty, 2),
            },
            "parameter_health": {p: round(v, 1) for p, v in param_health.items()},
        }

    def _hard_events(self, station: str, hours: int = 24) -> int:
        try:
            with db_connect(self.db_path, timeout=10) as conn:
                return int(conn.execute(
                    "SELECT COUNT(*) FROM edge_events WHERE station_name = ? AND datetime(created_at) >= datetime('now', ?)",
                    (station, f"-{int(hours)} hours"),
                ).fetchone()[0] or 0)
        except sqlite3.Error:
            return 0

    def station_health(self, station: str, window: int = 160) -> Dict[str, Any]:
        rows = self._fetch_rows(station, limit=window * 2)
        if not rows:
            return {
                "station": station,
                "health_score": 50.0,
                "status": "UNKNOWN",
                "trend": "INSUFFICIENT DATA",
                "trend_delta": 0.0,
                "degradation_risk": "UNKNOWN",
                "parameter_health": {p: None for p in PARAMETERS},
                "maintenance": {"urgency": "DATA NEEDED", "recommendation": "Wait for telemetry from this station."},
                "evidence": [],
            }

        recent = rows[:window]
        previous = rows[window: window * 2]
        hard_recent = self._hard_events(station, 24)
        current = self._score_window(recent, hard_recent)
        previous_score = self._score_window(previous, 0)["score"] if previous else current["score"]
        delta = round(current["score"] - previous_score, 1)
        trend = self._trend_label(delta)

        if current["score"] < 45 or delta <= -15 or current["metrics"].freeze.get("temperature") or current["metrics"].freeze.get("pressure") or current["metrics"].freeze.get("humidity"):
            risk = "HIGH"
        elif current["score"] < 70 or delta <= -6:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        if current["score"] < 45:
            maintenance = {"urgency": "URGENT", "recommendation": "Inspect the station and affected sensor before trusting new observations."}
        elif risk == "HIGH":
            maintenance = {"urgency": "INSPECTION", "recommendation": "Inspect/calibrate the most affected sensor and review recent anomaly history."}
        elif risk == "MEDIUM":
            maintenance = {"urgency": "CALIBRATION WATCH", "recommendation": "Schedule calibration and continue monitoring the health trend."}
        else:
            maintenance = {"urgency": "ROUTINE", "recommendation": "Continue routine preventive maintenance."}

        m = current["metrics"]
        parameter = current["parameter_health"]
        weakest = min(parameter, key=parameter.get)
        evidence = [
            f"Anomaly rate: {m.anomaly_rate * 100:.1f}% over the recent window",
            f"Largest drift index: {max(m.drift.values()):.2f}",
            f"Edge hard events in last 24h: {hard_recent}",
            f"Communication gap rate: {m.gap_rate * 100:.1f}%",
        ]
        if any(m.freeze.values()):
            evidence.append("Frozen-value pattern detected in recent telemetry")
        evidence.append(f"Most affected parameter: {PARAM_LABELS[weakest]}")

        return {
            "station": station,
            "health_score": current["score"],
            "status": current["status"],
            "trend": trend,
            "trend_delta": delta,
            "degradation_risk": risk,
            "parameter_health": parameter,
            "maintenance": maintenance,
            "window_readings": current["metrics"].total,
            "anomaly_rate": round(m.anomaly_rate * 100.0, 2),
            "hard_events_24h": hard_recent,
            "drift_index": {p: round(v, 3) for p, v in m.drift.items()},
            "frozen": dict(m.freeze),
            "communication_gap_rate": round(m.gap_rate * 100.0, 2),
            "invalid_values": dict(m.invalid),
            "previous_health_score": round(previous_score, 1),
            "evidence": evidence,
        }

    def all_stations(self) -> Dict[str, Any]:
        stations = [self.station_health(s) for s in STATIONS]
        stations.sort(key=lambda x: (x["health_score"] if x["status"] != "UNKNOWN" else 50.0, x["station"]))
        scores = [x["health_score"] for x in stations if x["status"] != "UNKNOWN"]
        at_risk = sum(1 for x in stations if x["degradation_risk"] in ("MEDIUM", "HIGH"))
        inspection = sum(1 for x in stations if x["maintenance"]["urgency"] in ("URGENT", "INSPECTION", "CALIBRATION WATCH"))
        return {
            "stations": stations,
            "network_health": round(float(np.mean(scores)) if scores else 50.0, 1),
            "at_risk": at_risk,
            "maintenance_queue": inspection,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
