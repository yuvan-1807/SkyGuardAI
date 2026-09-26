"""SkyGuard Phase 2C - improved read-only self-healing advisor.

This module proposes replacement values for quarantined/suspicious observations.
It never overwrites raw telemetry.

Estimation is intentionally hybrid and explainable:
1) recent trusted history from the same station,
2) same-calendar-context baseline (month / day-of-year),
3) spatial residual transfer from nearby stations, and
4) a station-specific cross-parameter ExtraTrees model trained only on trusted
   observations.

The final estimate is an evidence-weighted ensemble of the available methods.
A basic physical guardrail prevents outputs from leaving the allowed parameter
bounds. Confidence is an evidence/agreement score, not a probability of truth.
"""
from __future__ import annotations

from db_utils import db_connect

from datetime import datetime
import math
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit, cross_val_predict

from config import DB_PATH
from psychrometrics_layer import PsychrometricsLayer

PARAMETERS = ("temperature", "pressure", "humidity")
BOUNDS = {
    "temperature": (-60.0, 70.0),
    "pressure": (870.0, 1100.0),
    "humidity": (0.0, 100.0),
}
PARAM_LABELS = {
    "temperature": "Temperature",
    "pressure": "Pressure",
    "humidity": "Humidity",
}
STATION_COORDS = {
    'AWS_01_Delhi': (28.7, 77.2), 'AWS_02_Mumbai': (19.1, 72.9), 'AWS_03_Chennai': (13.0, 80.3),
    'AWS_04_Himachal': (32.2, 77.0), 'AWS_05_Kolkata': (22.6, 88.4), 'AWS_06_Bangalore': (12.9, 77.6),
    'AWS_07_Kerala': (10.0, 76.3), 'AWS_08_Rajasthan': (26.9, 75.8), 'AWS_09_NorthEast': (25.6, 91.8),
    'AWS_10_Gujarat': (23.0, 72.6),
}

FEATURES = {
    "temperature": ("pressure", "humidity", "sin_doy", "cos_doy", "sin_hour", "cos_hour"),
    "pressure": ("temperature", "humidity", "sin_doy", "cos_doy", "sin_hour", "cos_hour"),
    "humidity": ("temperature", "pressure", "sin_doy", "cos_doy", "sin_hour", "cos_hour"),
}


class SelfHealingAdvisorV3:
    """Explainable hybrid correction advisor. Raw observations remain untouched."""

    def __init__(self, db_path: str = DB_PATH, max_neighbors: int = 3):
        self.db_path = db_path
        self.max_neighbors = int(max_neighbors)
        self._model_cache: Dict[Tuple[str, str], Tuple[int, ExtraTreesRegressor, float, float, int]] = {}

    # ---------- shared helpers ----------
    @staticmethod
    def _parse_time(value: str) -> datetime:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            else:
                dt = datetime.now()
        return dt.replace(tzinfo=None)

    @staticmethod
    def _finite(value: Any) -> Optional[float]:
        try:
            v = float(value)
            return v if math.isfinite(v) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _robust_scale(values: Iterable[float]) -> float:
        arr = np.asarray([float(v) for v in values if v is not None and math.isfinite(float(v))], dtype=float)
        if arr.size < 2:
            return 1.0
        med = float(np.median(arr))
        mad = float(np.median(np.abs(arr - med)))
        return max(1.4826 * mad, float(np.std(arr)), 1e-3)

    @staticmethod
    def _median(values: Iterable[float]) -> Optional[float]:
        vals = [float(v) for v in values if v is not None and math.isfinite(float(v))]
        return None if not vals else float(np.median(np.asarray(vals, dtype=float)))

    @staticmethod
    def _context_features(timestamp: str) -> Dict[str, float]:
        dt = SelfHealingAdvisorV3._parse_time(timestamp)
        hour = dt.hour + dt.minute / 60.0
        doy = dt.timetuple().tm_yday
        return {
            "sin_doy": math.sin(2.0 * math.pi * doy / 365.25),
            "cos_doy": math.cos(2.0 * math.pi * doy / 365.25),
            "sin_hour": math.sin(2.0 * math.pi * hour / 24.0),
            "cos_hour": math.cos(2.0 * math.pi * hour / 24.0),
            "month": float(dt.month),
            "doy": float(doy),
        }

    def _station_history(self, station: str, limit: int = 700) -> List[Dict[str, Any]]:
        with db_connect(self.db_path, timeout=10) as conn:
            rows = conn.execute(
                """SELECT id, timestamp, temperature, pressure, humidity
                   FROM sensor_readings
                   WHERE station_name = ? AND COALESCE(is_anomaly,0)=0
                   ORDER BY id ASC LIMIT ?""",
                (station, int(limit)),
            ).fetchall()
        out = []
        for row in rows:
            parsed = self._parse_time(row[1])
            item = {
                "id": int(row[0]),
                "timestamp": str(row[1]),
                "dt": parsed,
                "temperature": self._finite(row[2]),
                "pressure": self._finite(row[3]),
                "humidity": self._finite(row[4]),
            }
            out.append(item)
        return out

    # ---------- temporal evidence ----------
    def _temporal_estimates(self, history: List[Dict[str, Any]], parameter: str, timestamp: str) -> Dict[str, Any]:
        vals = [r[parameter] for r in history if r[parameter] is not None]
        recent = vals[-40:]
        recent_est = self._median(recent)
        dt = self._parse_time(timestamp)
        contextual = [
            r[parameter] for r in history
            if r[parameter] is not None and r["dt"].month == dt.month
        ]
        context_est = self._median(contextual)
        if recent_est is not None and context_est is not None:
            # Recent behavior is more informative for live correction; the
            # calendar-context reference prevents a short anomalous drift from
            # dominating the replacement.
            estimate = 0.65 * recent_est + 0.35 * context_est
            method = "Recent trusted median + same-month baseline"
        elif recent_est is not None:
            estimate = recent_est
            method = "Recent trusted median"
        elif context_est is not None:
            estimate = context_est
            method = "Same-month trusted baseline"
        else:
            estimate = None
            method = "No temporal reference"

        return {
            "estimate": None if estimate is None else float(estimate),
            "recent_median": recent_est,
            "context_median": context_est,
            "recent_samples": len(recent),
            "context_samples": len(contextual),
            "method": method,
        }

    # ---------- spatial residual evidence ----------
    def _nearest(self, station: str) -> List[Tuple[str, float]]:
        if station not in STATION_COORDS:
            return []
        lat, lon = STATION_COORDS[station]
        items = []
        for name, (nlat, nlon) in STATION_COORDS.items():
            if name == station:
                continue
            # Great-circle-like flat approximation is sufficient for ranking
            # these fixed station locations.
            dist = math.hypot((lat - nlat), (lon - nlon))
            items.append((name, max(dist, 1e-6)))
        items.sort(key=lambda x: x[1])
        return items[:self.max_neighbors]

    def _neighbor_at_time(self, station: str, timestamp: str, parameter: str) -> Optional[float]:
        # Prefer a close time match; the relaxed fallback makes the method work
        # with demo batches that may have slightly different ingestion times.
        try:
            with db_connect(self.db_path, timeout=10) as conn:
                row = conn.execute(
                    f"""SELECT {parameter} FROM sensor_readings
                        WHERE station_name = ? AND COALESCE(is_anomaly,0)=0
                          AND {parameter} IS NOT NULL
                          AND ABS(julianday(timestamp)-julianday(?)) <= 0.02
                        ORDER BY ABS(julianday(timestamp)-julianday(?)) ASC, id DESC
                        LIMIT 1""",
                    (station, timestamp, timestamp),
                ).fetchone()
            return self._finite(row[0]) if row else None
        except sqlite3.Error:
            return None

    def _station_context_baseline(self, station: str, parameter: str, timestamp: str) -> Optional[float]:
        dt = self._parse_time(timestamp)
        try:
            with db_connect(self.db_path, timeout=10) as conn:
                rows = conn.execute(
                    f"""SELECT {parameter}, timestamp FROM sensor_readings
                        WHERE station_name = ? AND COALESCE(is_anomaly,0)=0
                          AND {parameter} IS NOT NULL
                        ORDER BY id DESC LIMIT 700""",
                    (station,),
                ).fetchall()
        except sqlite3.Error:
            return None
        vals = []
        for value, ts in rows:
            v = self._finite(value)
            if v is None:
                continue
            if self._parse_time(ts).month == dt.month:
                vals.append(v)
        return self._median(vals)

    def _spatial_residual_estimate(self, station: str, timestamp: str, parameter: str) -> Dict[str, Any]:
        target_base = self._station_context_baseline(station, parameter, timestamp)
        if target_base is None:
            return {"estimate": None, "neighbors": {}, "method": "No spatial reference"}

        candidates = []
        for neighbor, distance in self._nearest(station):
            current = self._neighbor_at_time(neighbor, timestamp, parameter)
            neighbor_base = self._station_context_baseline(neighbor, parameter, timestamp)
            if current is None or neighbor_base is None:
                continue
            residual = current - neighbor_base
            adjusted = target_base + residual
            candidates.append((neighbor, distance, current, neighbor_base, residual, adjusted))

        if not candidates:
            return {"estimate": None, "neighbors": {}, "method": "No trusted nearby observation"}

        num = 0.0
        den = 0.0
        info = {}
        for neighbor, distance, current, baseline, residual, adjusted in candidates:
            w = 1.0 / (distance ** 2)
            num += w * adjusted
            den += w
            info[neighbor] = {
                "distance": round(distance, 4),
                "observed": round(current, 2),
                "baseline": round(baseline, 2),
                "residual": round(residual, 2),
                "adjusted_target": round(adjusted, 2),
            }
        estimate = num / den if den else None
        return {
            "estimate": None if estimate is None else float(estimate),
            "neighbors": info,
            "neighbor_count": len(info),
            "method": "Climatology-adjusted spatial IDW",
        }

    # ---------- cross-parameter ML evidence ----------
    def _build_model(self, station: str, parameter: str, history: List[Dict[str, Any]]):
        key = (station, parameter)
        rows = [r for r in history if all(r.get(f) is not None for f in PARAMETERS)]
        count = len(rows)
        cached = self._model_cache.get(key)
        if cached and cached[0] == count:
            return cached
        if count < 90:
            return None

        feature_names = FEATURES[parameter]
        X = []
        y = []
        for r in rows:
            cf = self._context_features(r["timestamp"])
            item = {
                "temperature": r["temperature"],
                "pressure": r["pressure"],
                "humidity": r["humidity"],
                **cf,
            }
            X.append([float(item[f]) for f in feature_names])
            y.append(float(r[parameter]))
        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float)

        model = ExtraTreesRegressor(
            n_estimators=220,
            random_state=42,
            min_samples_leaf=4,
            max_features=0.9,
            n_jobs=1,
        )
        try:
            n_splits = 3
            cv = TimeSeriesSplit(n_splits=n_splits)
            pred = cross_val_predict(model, X, y, cv=cv, n_jobs=1)
            mae = float(mean_absolute_error(y, pred))
            rmse = float(math.sqrt(mean_squared_error(y, pred)))
        except Exception:
            mae = float(np.mean(np.abs(y - np.median(y))))
            rmse = float(np.sqrt(np.mean((y - np.median(y)) ** 2)))
        scale = self._robust_scale(y)
        model.fit(X, y)
        cached = (count, model, mae, rmse, count)
        self._model_cache[key] = cached
        return cached

    def _ml_estimate(self, station: str, parameter: str, timestamp: str, values: Dict[str, float], history: List[Dict[str, Any]]) -> Dict[str, Any]:
        cache = self._build_model(station, parameter, history)
        if not cache:
            return {"estimate": None, "method": "No ML reference (insufficient trusted history)"}
        _, model, mae, rmse, count = cache
        feature_names = FEATURES[parameter]

        # Do not let another obviously corrupted sensor become an ML input.
        # A companion feature is accepted only when it is inside its engineering
        # bounds and is reasonably close to its own trusted temporal reference.
        for companion in (f for f in feature_names if f in PARAMETERS):
            cv = values.get(companion)
            if cv is None:
                return {"estimate": None, "method": f"ML unavailable (missing trusted {companion} input)"}
            lo, hi = BOUNDS[companion]
            if cv < lo or cv > hi:
                return {"estimate": None, "method": f"ML unavailable ({companion} is out of bounds)"}
            temp_ref = self._temporal_estimates(history, companion, timestamp).get("estimate")
            scale = self._robust_scale([r[companion] for r in history if r[companion] is not None])
            if temp_ref is not None and abs(float(cv) - float(temp_ref)) > max(4.0 * scale, 2.5 * abs(float(temp_ref)) if companion == "pressure" else 5.0):
                return {"estimate": None, "method": f"ML unavailable ({companion} companion value appears anomalous)"}

        cf = self._context_features(timestamp)
        item = {**{k: values.get(k) for k in PARAMETERS}, **cf}
        try:
            x = np.asarray([[float(item[f]) for f in feature_names]], dtype=float)
            estimate = float(model.predict(x)[0])
        except Exception:
            return {"estimate": None, "method": "ML prediction unavailable"}
        scale = self._robust_scale([r[parameter] for r in history if r[parameter] is not None])
        skill = max(0.0, 1.0 - (mae / max(scale, 1e-3)))
        quality = max(0.0, min(1.0, 0.35 + 0.65 * skill))
        return {
            "estimate": estimate,
            "method": "Cross-parameter ExtraTrees regression",
            "trusted_training_samples": count,
            "cross_validated_mae": round(mae, 3),
            "cross_validated_rmse": round(rmse, 3),
            "quality": round(quality, 3),
        }

    # ---------- ensemble ----------
    def estimate_parameter(self, station: str, timestamp: str, parameter: str, observed: float, values: Dict[str, float], history: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        if parameter not in PARAMETERS:
            raise ValueError(f"Unsupported parameter: {parameter}")
        history = history or self._station_history(station)

        temporal = self._temporal_estimates(history, parameter, timestamp)
        spatial = self._spatial_residual_estimate(station, timestamp, parameter)
        ml = self._ml_estimate(station, parameter, timestamp, values, history)

        candidates: List[Tuple[str, float, float]] = []
        # ML gets the strongest voice when it has enough trusted history and
        # companion parameters, but only in proportion to cross-validation skill.
        if ml.get("estimate") is not None:
            candidates.append(("ML", float(ml["estimate"]), float(0.55 * ml.get("quality", 0.5))))
        if temporal.get("estimate") is not None:
            support = min(1.0, (temporal.get("recent_samples", 0) / 40.0))
            if temporal.get("context_samples", 0) >= 8:
                support = min(1.0, support + 0.2)
            candidates.append(("Temporal", float(temporal["estimate"]), float(0.30 * support)))
        if spatial.get("estimate") is not None:
            n = int(spatial.get("neighbor_count", 0))
            support = min(1.0, n / 3.0)
            candidates.append(("Spatial", float(spatial["estimate"]), float(0.25 * support)))

        # Remove zero-weight components and normalize.
        candidates = [c for c in candidates if c[2] > 0]
        if not candidates:
            return {
                "parameter": parameter,
                "observed": round(float(observed), 2),
                "corrected_value": None,
                "method": "No trusted reference available",
                "confidence": 0.0,
                "basis": {},
            }

        total_w = sum(c[2] for c in candidates)
        corrected = sum(value * weight for _, value, weight in candidates) / total_w

        lo, hi = BOUNDS[parameter]
        corrected = max(lo, min(hi, corrected))

        # Agreement penalty: if methods disagree strongly relative to historical
        # variability, confidence falls rather than pretending the estimate is exact.
        scale = self._robust_scale([r[parameter] for r in history if r[parameter] is not None])
        spread = math.sqrt(sum((value - corrected) ** 2 * weight for _, value, weight in candidates) / total_w)
        agreement = max(0.0, 1.0 - spread / max(scale * 2.0, 1e-3))
        support = min(1.0, total_w / 0.75)
        confidence = 42.0 + 38.0 * support + 15.0 * agreement
        if len(candidates) == 1:
            confidence -= 8.0
        confidence = round(max(20.0, min(95.0, confidence)), 1)

        weighted_methods = ", ".join(f"{name} {weight / total_w * 100:.0f}%" for name, _, weight in candidates)
        method = f"Hybrid ensemble ({weighted_methods})"

        basis = {
            "temporal": temporal,
            "spatial": spatial,
            "ml": ml,
            "ensemble_spread": round(spread, 3),
            "historical_scale": round(scale, 3),
            "agreement_score": round(agreement, 3),
            "formula": "Evidence-weighted ensemble; stronger cross-validated ML and agreeing references receive more weight.",
        }
        return {
            "parameter": parameter,
            "observed": round(float(observed), 2),
            "corrected_value": round(float(corrected), 2),
            "method": method,
            "confidence": confidence,
            "basis": basis,
            "reference_counts": {
                "trusted_station_samples": len([r for r in history if r[parameter] is not None]),
                "neighbor_count": int(spatial.get("neighbor_count", 0)),
                "ml_training_samples": int(ml.get("trusted_training_samples", 0) or 0),
            },
            "guardrail": {
                "bounds": [lo, hi],
                "within_physical_bounds": True,
            },
        }

    def preview(self, station: str, timestamp: str, values: Dict[str, float], parameters=None) -> Dict[str, Any]:
        parameters = tuple(parameters or PARAMETERS)
        clean_values = {p: self._finite(values.get(p)) for p in PARAMETERS}
        history = self._station_history(station)
        results = {}
        for p in parameters:
            if p in PARAMETERS and clean_values.get(p) is not None:
                results[p] = self.estimate_parameter(station, timestamp, p, clean_values[p], clean_values, history)
        usable = [r for r in results.values() if r.get("corrected_value") is not None]
        corrected_triplet = dict(clean_values)
        for p, item in results.items():
            if item.get("corrected_value") is not None:
                corrected_triplet[p] = item["corrected_value"]
        physics = None
        if all(corrected_triplet.get(p) is not None for p in PARAMETERS):
            try:
                physics = PsychrometricsLayer.validate_reading(
                    float(corrected_triplet["temperature"]),
                    float(corrected_triplet["humidity"]),
                    float(corrected_triplet["pressure"]),
                )
            except Exception as exc:
                physics = {"is_physically_valid": None, "error": str(exc)}

        overall = float(np.mean([r["confidence"] for r in usable])) if usable else 0.0
        if physics and physics.get("is_physically_valid") is False:
            # This does not silently reject the estimate; it makes the physical
            # concern visible and lowers confidence for operator review.
            overall = max(0.0, overall - 12.0)

        return {
            "station": station,
            "timestamp": timestamp,
            "read_only": True,
            "raw_observation_preserved": True,
            "algorithm": "Hybrid evidence-weighted temporal + climatology-adjusted spatial + cross-parameter ML imputation with physics guardrail",
            "parameters": results,
            "overall_confidence": round(overall, 1),
            "physics_guard": physics,
            "explanation": "The suggested value is an evidence-based imputation, not a claim of the exact hidden true value. It combines trusted recent station history, same-month context, nearby-station deviations relative to their own baselines, and a cross-parameter model when sufficient trusted companion data is available. Agreement between methods raises confidence; disagreement lowers it.",
            "note": "Raw telemetry is never overwritten. The physical-consistency check is a guardrail/diagnostic, not ground truth. Operators can review the suggestion before any downstream use.",
        }
