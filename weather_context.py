"""
SkyGuard AI - Phase 3B
Thermodynamic + contextual weather-event evidence.

Purpose:
    Provide a weather-context layer that distinguishes a coordinated,
    physically plausible atmospheric transition from an isolated sensor jump.

This module DOES NOT make the final NORMAL / WEATHER_EVENT / SENSOR_ANOMALY
verdict. Decision Fusion owns that later. Phase 3B only produces structured
contextual evidence for the fusion engine.
"""
from __future__ import annotations

import math
from statistics import median, pstdev
from typing import Any, Dict, Iterable, Optional

from psychrometrics_layer import PsychrometricsLayer


class WeatherContextEngine:
    """Generate explainable evidence for potential genuine weather events."""

    def __init__(self, min_history: int = 5):
        self.min_history = int(min_history)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            x = float(value)
            return x if math.isfinite(x) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _robust_change(current: float, previous: Optional[float]) -> float:
        if previous is None:
            return 0.0
        return float(current - previous)

    @staticmethod
    def _robust_z(value: float, history: Iterable[float]) -> float:
        vals = [float(v) for v in history if v is not None and math.isfinite(float(v))]
        if len(vals) < 3:
            return 0.0
        center = median(vals)
        scale = pstdev(vals)
        if scale < 1e-6:
            return 0.0
        return abs(float(value) - center) / scale

    @staticmethod
    def _coordinated_score(d_temp: float, d_rh: float, d_pressure: float,
                           d_dew: float) -> float:
        """Score coordinated movement rather than absolute extremeness.

        This is intentionally evidence-oriented. It rewards multiple variables
        moving together and a dew-point response that remains consistent with
        temperature/RH movement.
        """
        # Meaningful movement bands chosen for AWS demo data, not a meteorological
        # forecast threshold. Final classification remains in Decision Fusion.
        t = min(abs(d_temp) / 3.0, 1.0)
        rh = min(abs(d_rh) / 12.0, 1.0)
        p = min(abs(d_pressure) / 4.0, 1.0)
        dew = min(abs(d_dew) / 2.5, 1.0)

        movement_count = sum(x >= 0.35 for x in (t, rh, p))
        coordination = min(1.0, movement_count / 3.0)

        # A dew-point response should broadly move with moisture/temperature
        # context. Penalize only obvious contradiction, not normal noise.
        dew_alignment = 1.0
        if abs(d_rh) >= 5.0 and abs(d_dew) >= 0.5:
            if (d_rh > 0 and d_dew < -0.3) or (d_rh < 0 and d_dew > 0.8):
                dew_alignment = 0.35

        raw = 0.45 * coordination + 0.20 * t + 0.15 * rh + 0.10 * p + 0.10 * dew
        return max(0.0, min(1.0, raw * dew_alignment))

    def analyze(self, temperature: float, pressure: float, humidity: float,
                history: Optional[Iterable[Dict[str, Any]]] = None,
                frozen: bool = False) -> Dict[str, Any]:
        """Return structured weather-event evidence.

        `history` should be chronological (oldest -> newest) and contain
        temperature/pressure/humidity values. Only the latest historical row is
        used for first-order changes; the broader window provides context.
        """
        temp = self._safe_float(temperature)
        press = self._safe_float(pressure)
        rh = self._safe_float(humidity)
        if temp is None or press is None or rh is None:
            return {
                "available": False,
                "weather_event_candidate": False,
                "weather_transition_score": 0.0,
                "confidence": 0.0,
                "reason": "Missing or non-finite meteorological values",
                "dew_point_c": None,
            }

        rows = []
        for row in history or []:
            try:
                t = self._safe_float(row.get("temperature"))
                p = self._safe_float(row.get("pressure"))
                h = self._safe_float(row.get("humidity"))
                if t is not None and p is not None and h is not None:
                    rows.append((t, p, h))
            except AttributeError:
                continue

        current_dew = PsychrometricsLayer.calculate_dew_point(temp, rh)
        previous = rows[-1] if rows else None
        previous_dew = (
            PsychrometricsLayer.calculate_dew_point(previous[0], previous[2])
            if previous else None
        )

        if previous:
            d_temp = temp - previous[0]
            d_pressure = press - previous[1]
            d_rh = rh - previous[2]
            d_dew = (current_dew - previous_dew) if current_dew is not None and previous_dew is not None else 0.0
        else:
            d_temp = d_pressure = d_rh = d_dew = 0.0

        # Historical context scores help distinguish a transition from a random
        # isolated observation without declaring the latter to be a fault here.
        temp_z = self._robust_z(temp, [r[0] for r in rows])
        pressure_z = self._robust_z(press, [r[1] for r in rows])
        humidity_z = self._robust_z(rh, [r[2] for r in rows])
        max_context_z = max(temp_z, pressure_z, humidity_z)

        movement_score = self._coordinated_score(d_temp, d_rh, d_pressure, d_dew)

        physical = PsychrometricsLayer.validate_reading(temp, rh, press)
        physically_plausible = bool(physical.get("is_physically_valid", True))

        # Weather events are evidence only when there is a coordinated change,
        # enough history, physical plausibility, and no frozen-sensor indication.
        history_ready = len(rows) >= self.min_history
        candidate = bool(
            history_ready
            and not frozen
            and physically_plausible
            and movement_score >= 0.42
            and sum(abs(x) >= threshold for x, threshold in ((d_temp, 1.5), (d_rh, 5.0), (d_pressure, 1.5))) >= 2
        )

        confidence = min(100.0, 35.0 + movement_score * 55.0) if candidate else movement_score * 35.0
        if not history_ready:
            confidence = min(confidence, 35.0)

        if frozen:
            reason = "Weather context suppressed because a frozen sensor signal is present"
        elif not physically_plausible:
            reason = "Atmospheric transition is not physically consistent"
        elif not history_ready:
            reason = f"Insufficient history ({len(rows)}/{self.min_history}) for weather-context evidence"
        elif candidate:
            reason = "Coordinated temperature, humidity and/or pressure transition with physically consistent dew-point behaviour"
        else:
            reason = "No sufficiently coordinated atmospheric transition detected"

        return {
            "available": True,
            "weather_event_candidate": candidate,
            "weather_transition_score": round(movement_score * 100.0, 2),
            "confidence": round(confidence, 2),
            "reason": reason,
            "dew_point_c": round(current_dew, 3) if current_dew is not None else None,
            "previous_dew_point_c": round(previous_dew, 3) if previous_dew is not None else None,
            "changes": {
                "temperature_c": round(d_temp, 3),
                "humidity_percent": round(d_rh, 3),
                "pressure_hpa": round(d_pressure, 3),
                "dew_point_c": round(d_dew, 3),
            },
            "historical_context": {
                "samples": len(rows),
                "temperature_z": round(temp_z, 3),
                "pressure_z": round(pressure_z, 3),
                "humidity_z": round(humidity_z, 3),
                "max_context_z": round(max_context_z, 3),
            },
            "physical_validation": {
                "is_physically_valid": physically_plausible,
                "violations": physical.get("violations", []),
                "confidence_score": physical.get("confidence_score", 0),
            },
            "method": "Thermodynamic + coordinated-change contextual evidence (Phase 3B)",
        }


__all__ = ["WeatherContextEngine"]
