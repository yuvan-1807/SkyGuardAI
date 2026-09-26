"""SkyGuard AI - Phase 3C ExtraTrees anomaly evidence.

Adds station-specific ExtraTrees regression as an evidence layer without
replacing the existing anomaly detectors or changing raw observations.

Design goals:
- Predict one target sensor parameter from the other sensor parameters plus
  calendar context.
- Train only on trusted/clean historical observations.
- Use walk-forward time-series cross-validation (no future leakage).
- Convert the observed-vs-expected residual into calibrated anomaly evidence.
- Keep this layer advisory until the Phase 3E Decision Fusion engine owns the
  final NORMAL / WEATHER EVENT / SENSOR ANOMALY classification.
"""
from __future__ import annotations

from db_utils import db_connect

import math
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
from sklearn.base import clone
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit

PARAMETERS: Tuple[str, ...] = ("temperature", "pressure", "humidity")
BOUNDS = {
    "temperature": (-60.0, 70.0),
    "pressure": (870.0, 1100.0),
    "humidity": (0.0, 100.0),
}
FEATURES = {
    "temperature": ("pressure", "humidity", "sin_doy", "cos_doy", "sin_hour", "cos_hour"),
    "pressure": ("temperature", "humidity", "sin_doy", "cos_doy", "sin_hour", "cos_hour"),
    "humidity": ("temperature", "pressure", "sin_doy", "cos_doy", "sin_hour", "cos_hour"),
}
PARAM_SCALE_FLOORS = {
    "temperature": 0.5,
    "pressure": 0.5,
    "humidity": 2.0,
}
MODEL_VERSION = "phase3c-extratrees-v1"


class ExtraTreesEvidence:
    """Station-specific ExtraTrees expected-value / residual evidence engine."""

    def __init__(self, db_path: str, min_samples: int = 90, n_estimators: int = 220):
        self.db_path = db_path
        self.min_samples = int(min_samples)
        self.n_estimators = int(n_estimators)
        self._cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

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
    def _robust_scale(values: Iterable[float], floor: float = 1e-3) -> float:
        arr = np.asarray([float(v) for v in values if v is not None and math.isfinite(float(v))], dtype=float)
        if arr.size < 2:
            return float(floor)
        med = float(np.median(arr))
        mad = float(np.median(np.abs(arr - med)))
        return max(1.4826 * mad, float(np.std(arr)), float(floor))

    @staticmethod
    def _context_features(timestamp: Any) -> Dict[str, float]:
        dt = ExtraTreesEvidence._parse_time(timestamp)
        hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
        doy = dt.timetuple().tm_yday
        return {
            "sin_doy": math.sin(2.0 * math.pi * doy / 365.25),
            "cos_doy": math.cos(2.0 * math.pi * doy / 365.25),
            "sin_hour": math.sin(2.0 * math.pi * hour / 24.0),
            "cos_hour": math.cos(2.0 * math.pi * hour / 24.0),
        }

    def _trusted_history(self, station: str, limit: int = 2500, before_event_time: Any = None) -> List[Dict[str, Any]]:
        with db_connect(self.db_path, timeout=10) as conn:
            if before_event_time:
                rows = conn.execute(
                    """SELECT id, event_time, temperature, pressure, humidity,
                              0 AS is_anomaly, is_clean, validation_status, quarantine_status
                       FROM validated_observations
                       WHERE station_name = ? AND event_time < ?
                       ORDER BY event_time DESC, id DESC
                       LIMIT ?""",
                    (station, str(before_event_time), int(limit)),
                ).fetchall()
                rows = list(reversed(rows))
            else:
                rows = conn.execute(
                    """SELECT id, event_time, temperature, pressure, humidity,
                              0 AS is_anomaly, is_clean, validation_status, quarantine_status
                       FROM validated_observations
                       WHERE station_name = ?
                       ORDER BY event_time ASC, id ASC
                       LIMIT ?""",
                    (station, int(limit)),
                ).fetchall()
        history = []
        for row in rows:
            values = {p: self._finite(row[2 + i]) for i, p in enumerate(PARAMETERS)}
            timestamp = row[1]
            if any(values[p] is None for p in PARAMETERS):
                continue
            if any(not (BOUNDS[p][0] <= values[p] <= BOUNDS[p][1]) for p in PARAMETERS):
                continue
            history.append({
                "id": int(row[0]),
                "event_time": str(timestamp),
                **values,
            })
        return history

    def _current_inputs_are_valid(self, values: Dict[str, float], target: str) -> Optional[str]:
        for p in PARAMETERS:
            value = self._finite(values.get(p))
            if value is None:
                return f"Missing/non-finite {p} input"
            lo, hi = BOUNDS[p]
            if value < lo or value > hi:
                return f"{p} input is out of engineering bounds"
        # The target itself is allowed to be unusual; it is the quantity being
        # scored. Companion parameters must be physically usable as features.
        return None

    def _matrix(self, rows: Sequence[Dict[str, Any]], target: str) -> Tuple[np.ndarray, np.ndarray]:
        names = FEATURES[target]
        X, y = [], []
        for row in rows:
            cf = self._context_features(row["event_time"])
            item = {**{p: row[p] for p in PARAMETERS}, **cf}
            X.append([float(item[name]) for name in names])
            y.append(float(row[target]))
        return np.asarray(X, dtype=float), np.asarray(y, dtype=float)

    @staticmethod
    def _walk_forward_cv(model: ExtraTreesRegressor, X: np.ndarray, y: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        splitter = TimeSeriesSplit(n_splits=3)
        actuals, preds = [], []
        for train_idx, test_idx in splitter.split(X):
            fitted = clone(model)
            fitted.fit(X[train_idx], y[train_idx])
            fold_pred = fitted.predict(X[test_idx])
            actuals.extend(y[test_idx].tolist())
            preds.extend(fold_pred.tolist())
        return np.asarray(actuals, dtype=float), np.asarray(preds, dtype=float)

    def _fit_bundle(self, station: str, target: str, history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        key = (station, target)
        count = len(history)
        signature = (count, history[-1]["id"] if history else -1)
        cached = self._cache.get(key)
        if cached and cached["signature"] == signature:
            return cached
        if count < self.min_samples:
            return None

        X, y = self._matrix(history, target)
        model = ExtraTreesRegressor(
            n_estimators=self.n_estimators,
            random_state=42,
            min_samples_leaf=4,
            max_features=0.9,
            n_jobs=1,
        )
        cv_actual, cv_pred = self._walk_forward_cv(model, X, y)
        cv_residuals = cv_actual - cv_pred
        residual_center = float(np.median(cv_residuals))
        residual_scale = self._robust_scale(cv_residuals, PARAM_SCALE_FLOORS[target] / 2.0)
        mae = float(mean_absolute_error(cv_actual, cv_pred))
        rmse = float(math.sqrt(mean_squared_error(cv_actual, cv_pred)))
        target_scale = self._robust_scale(y, PARAM_SCALE_FLOORS[target])
        skill = max(0.0, min(1.0, 1.0 - mae / max(target_scale, 1e-6)))

        model.fit(X, y)
        bundle = {
            "signature": signature,
            "model": model,
            "cv_mae": mae,
            "cv_rmse": rmse,
            "residual_center": residual_center,
            "residual_scale": residual_scale,
            "target_scale": target_scale,
            "quality": skill,
            "samples": count,
        }
        self._cache[key] = bundle
        return bundle

    def analyze_parameter(self, station: str, event_time: Any, target: str, observed: float,
                          values: Dict[str, float], history: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        if target not in PARAMETERS:
            raise ValueError(f"Unsupported target parameter: {target}")
        observed_f = self._finite(observed)
        if observed_f is None:
            return {"available": False, "parameter": target, "reason": "Observed target is missing/non-finite", "model_version": MODEL_VERSION}

        history = history if history is not None else self._trusted_history(station)
        if len(history) < self.min_samples:
            return {
                "available": False,
                "parameter": target,
                "reason": f"Insufficient trusted history ({len(history)}/{self.min_samples})",
                "trusted_training_samples": len(history),
                "model_version": MODEL_VERSION,
            }

        input_error = self._current_inputs_are_valid(values, target)
        if input_error:
            return {
                "available": False,
                "parameter": target,
                "reason": f"ML evidence unavailable: {input_error}",
                "trusted_training_samples": len(history),
                "model_version": MODEL_VERSION,
            }

        bundle = self._fit_bundle(station, target, history)
        if bundle is None:
            return {"available": False, "parameter": target, "reason": "Model training unavailable", "model_version": MODEL_VERSION}

        cf = self._context_features(event_time)
        item = {**{p: float(values[p]) for p in PARAMETERS}, **cf}
        names = FEATURES[target]
        try:
            x = np.asarray([[float(item[name]) for name in names]], dtype=float)
            expected = float(bundle["model"].predict(x)[0])
        except Exception as exc:
            return {
                "available": False,
                "parameter": target,
                "reason": f"ML prediction unavailable: {exc.__class__.__name__}",
                "trusted_training_samples": len(history),
                "model_version": MODEL_VERSION,
            }

        residual = observed_f - expected
        centered_residual = residual - bundle["residual_center"]
        normalized = abs(centered_residual) / max(bundle["residual_scale"], 1e-6)
        threshold_sigma = 4.0
        candidate = bool(normalized >= threshold_sigma)
        quality = float(bundle["quality"])
        confidence = 0.0
        if candidate:
            raw = 55.0 + max(0.0, normalized - threshold_sigma) * 11.0
            confidence = min(100.0, raw * (0.55 + 0.45 * quality))
        reason = (
            f"{target.title()} residual {residual:+.3f} is {normalized:.2f} robust residual-scale units from expected value"
            if candidate else
            f"{target.title()} residual {residual:+.3f} remains within calibrated ML residual envelope"
        )
        return {
            "available": True,
            "parameter": target,
            "observed": round(observed_f, 4),
            "expected": round(expected, 4),
            "residual": round(residual, 4),
            "absolute_residual": round(abs(residual), 4),
            "normalized_residual": round(float(normalized), 4),
            "residual_threshold_sigma": threshold_sigma,
            "anomaly_candidate": candidate,
            "confidence": round(float(confidence), 2),
            "model_quality": round(quality, 4),
            "trusted_training_samples": int(bundle["samples"]),
            "cross_validated_mae": round(bundle["cv_mae"], 4),
            "cross_validated_rmse": round(bundle["cv_rmse"], 4),
            "features": list(names),
            "model_version": MODEL_VERSION,
            "method": "Station-specific ExtraTrees regression with walk-forward residual calibration",
            "reason": reason,
        }

    def analyze(self, station: str, event_time: Any, temperature: float, pressure: float, humidity: float,
                targets: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        values = {"temperature": float(temperature), "pressure": float(pressure), "humidity": float(humidity)}
        targets = tuple(targets or PARAMETERS)
        history = self._trusted_history(station, before_event_time=event_time)
        per_parameter = {
            target: self.analyze_parameter(station, event_time, target, values[target], values, history)
            for target in targets
        }
        available = [r for r in per_parameter.values() if r.get("available")]
        candidates = [r for r in available if r.get("anomaly_candidate")]
        strongest = max(available, key=lambda r: float(r.get("normalized_residual", 0.0)), default=None)
        return {
            "available": bool(available),
            "station_id": station,
            "event_time": str(event_time),
            "model_version": MODEL_VERSION,
            "anomaly_candidate": bool(candidates),
            "confidence": round(max((float(r.get("confidence", 0.0)) for r in candidates), default=0.0), 2),
            "strongest_parameter": strongest.get("parameter") if strongest else None,
            "parameters": per_parameter,
            "method": "Multi-parameter ExtraTrees expected-value and residual evidence",
        }


__all__ = ["ExtraTreesEvidence", "MODEL_VERSION"]
