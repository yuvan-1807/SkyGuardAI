"""SkyGuard AI - model-native SHAP explainability.

This layer uses the official ``shap`` library with a station-specific
IsolationForest surrogate trained only on readings currently marked normal.
The explainer is invoked on the read-only anomaly-details path so the live
sensor ingestion path remains lightweight and state-compatible.
"""
from __future__ import annotations

from db_utils import db_connect

import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest

from config import DB_PATH


FEATURES: Tuple[str, str, str] = ("temperature", "pressure", "humidity")
FEATURE_LABELS = {
    "temperature": "Temperature",
    "pressure": "Pressure",
    "humidity": "Humidity",
}


@dataclass
class _CachedModel:
    normal_count: int
    model: IsolationForest
    explainer: Any
    mean: np.ndarray
    std: np.ndarray


class SHAPExplainer:
    """Explain an AWS anomaly with actual SHAP TreeExplainer values.

    A station-specific IsolationForest is trained on the station's observations
    that are currently labelled normal. This gives SHAP a concrete tree model
    to explain without changing the existing anomaly-decision logic.
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._cache: Dict[str, _CachedModel] = {}
        self._shap = None
        self._shap_version = None

    # ------------------------------------------------------------------
    # Data / model helpers
    # ------------------------------------------------------------------
    def _load_normal_rows(self, station_name: str) -> np.ndarray:
        with db_connect(self.db_path, timeout=10) as conn:
            rows = conn.execute(
                """SELECT temperature, pressure, humidity
                   FROM sensor_readings
                   WHERE station_name = ?
                     AND COALESCE(is_anomaly, 0) = 0
                     AND temperature IS NOT NULL
                     AND pressure IS NOT NULL
                     AND humidity IS NOT NULL
                   ORDER BY id DESC
                   LIMIT 2000""",
                (station_name,),
            ).fetchall()

            # If a station has too little normal history, fall back to the
            # network-wide normal distribution. This still gives a concrete
            # model instead of inventing manual feature weights.
            if len(rows) < 50:
                rows = conn.execute(
                    """SELECT temperature, pressure, humidity
                       FROM sensor_readings
                       WHERE COALESCE(is_anomaly, 0) = 0
                         AND temperature IS NOT NULL
                         AND pressure IS NOT NULL
                         AND humidity IS NOT NULL
                       ORDER BY id DESC
                       LIMIT 2000"""
                ).fetchall()

        if len(rows) < 20:
            raise RuntimeError(
                f"Not enough normal AWS observations for SHAP (found {len(rows)}, need at least 20)."
            )

        return np.asarray(rows, dtype=float)

    @staticmethod
    def _standardize_fit(X: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        mean = np.nanmean(X, axis=0)
        std = np.nanstd(X, axis=0)
        std = np.where(np.isfinite(std) & (std > 1e-9), std, 1.0)
        Z = (X - mean) / std
        return Z, mean, std

    def _get_shap_library(self):
        if self._shap is None:
            try:
                import shap  # noqa: PLC0415
            except ImportError as exc:
                raise RuntimeError(
                    "The official SHAP library is not installed. Run: python -m pip install 'shap==0.52.0'"
                ) from exc
            self._shap = shap
            self._shap_version = getattr(shap, "__version__", "unknown")
        return self._shap

    def _model_for_station(self, station_name: str) -> _CachedModel:
        with db_connect(self.db_path, timeout=10) as conn:
            normal_count = int(
                conn.execute(
                    """SELECT COUNT(*)
                       FROM sensor_readings
                       WHERE station_name = ?
                         AND COALESCE(is_anomaly, 0) = 0
                         AND temperature IS NOT NULL
                         AND pressure IS NOT NULL
                         AND humidity IS NOT NULL""",
                    (station_name,),
                ).fetchone()[0]
                or 0
            )

        cached = self._cache.get(station_name)
        # Live demo ingestion adds a small number of normal rows every batch.
        # Rebuilding the IsolationForest for every few new rows made the SHAP
        # panel unnecessarily slow. Reuse the station model until the normal
        # training population changes materially; the explanation remains
        # read-only and the anomaly decision itself is unchanged.
        if cached is not None and abs(cached.normal_count - normal_count) < 50:
            return cached

        X_raw = self._load_normal_rows(station_name)
        X, mean, std = self._standardize_fit(X_raw)

        # A deterministic, unsupervised model gives SHAP a concrete tree model
        # to explain. The existing SkyGuard anomaly verdict is NOT replaced by
        # this model; SHAP explains this model's learned normality structure.
        model = IsolationForest(
            n_estimators=100,
            max_samples=min(128, len(X)),
            contamination="auto",
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X)

        shap = self._get_shap_library()
        explainer = shap.TreeExplainer(model)

        cached = _CachedModel(
            normal_count=normal_count,
            model=model,
            explainer=explainer,
            mean=mean,
            std=std,
        )
        self._cache[station_name] = cached
        return cached

    # ------------------------------------------------------------------
    # Public explanation API
    # ------------------------------------------------------------------
    def calculate_shap_values(
        self,
        station_name: str,
        temperature: float,
        pressure: float,
        humidity: float,
        expected_temp: Optional[float] = None,
        expected_press: Optional[float] = None,
        expected_humid: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Return actual SHAP values for the three AWS sensor features."""
        cached = self._model_for_station(station_name)

        values = np.asarray([[float(temperature), float(pressure), float(humidity)]], dtype=float)
        z = (values - cached.mean) / cached.std

        # SHAP explains the IsolationForest tree output. For this model,
        # negative SHAP values push the output toward more anomalous behavior;
        # positive values push it toward the learned normal pattern.
        shap_exp = cached.explainer(z)
        raw_values = np.asarray(shap_exp.values, dtype=float).reshape(-1, 3)
        base_value = float(np.asarray(shap_exp.base_values, dtype=float).reshape(-1)[0])
        model_output = float(base_value + raw_values[0].sum())

        # Keep the model's directly observable anomaly decision separate from
        # SkyGuard's consolidated rule-based verdict.
        model_prediction = int(cached.model.predict(z)[0])
        isolation_score = float(cached.model.score_samples(z)[0])

        abs_values = np.abs(raw_values[0])
        negative = np.clip(-raw_values[0], 0.0, None)
        total_abs = float(abs_values.sum())
        total_negative = float(negative.sum())

        contribution_basis = negative if total_negative > 1e-12 else abs_values
        basis_sum = float(contribution_basis.sum())
        if basis_sum > 1e-12:
            contributions = contribution_basis / basis_sum * 100.0
        else:
            contributions = np.zeros(3, dtype=float)

        feature_rows: List[Dict[str, Any]] = []
        for idx, feature in enumerate(FEATURES):
            direction = "toward anomaly" if raw_values[0, idx] < 0 else "toward normality"
            feature_rows.append(
                {
                    "feature": feature,
                    "parameter": FEATURE_LABELS[feature],
                    "shap_value": round(float(raw_values[0, idx]), 6),
                    "contribution_percent": round(float(contributions[idx]), 1),
                    "absolute_importance": round(float(abs_values[idx]), 6),
                    "direction": direction,
                    "observed_value": float(values[0, idx]),
                    "baseline_value": float(cached.mean[idx]),
                    "standardized_deviation": float(z[0, idx]),
                }
            )

        feature_rows.sort(key=lambda item: item["contribution_percent"], reverse=True)
        primary = feature_rows[0] if feature_rows else None

        if primary and total_negative > 1e-12:
            narrative = (
                f"SHAP identified {primary['parameter']} as the largest anomaly-driving feature, "
                f"accounting for {primary['contribution_percent']:.1f}% of the negative SHAP magnitude. "
                f"Its SHAP value was {primary['shap_value']:.3f}, pushing the IsolationForest output toward anomaly."
            )
        elif primary:
            narrative = (
                "The SHAP explanation contains no negative feature contribution for this observation; "
                "the learned IsolationForest representation does not attribute the point toward anomaly."
            )
        else:
            narrative = "No SHAP feature attribution was produced."

        return {
            "available": True,
            "method": "SHAP TreeExplainer + station-specific IsolationForest",
            "library": "shap",
            "library_version": self._shap_version,
            "model": "IsolationForest trained on station normal observations",
            "station": station_name,
            "feature_names": [FEATURE_LABELS[x] for x in FEATURES],
            "base_value": round(base_value, 6),
            "model_output": round(model_output, 6),
            "model_prediction": "ANOMALY" if model_prediction == -1 else "NORMAL",
            "isolation_score": round(isolation_score, 6),
            "normal_training_samples": int(len(self._load_normal_rows(station_name))),
            "shap_values": {
                "temperature": round(float(raw_values[0, 0]), 6),
                "pressure": round(float(raw_values[0, 1]), 6),
                "humidity": round(float(raw_values[0, 2]), 6),
            },
            "contribution_percent": {
                "temperature": round(float(contributions[0]), 1),
                "pressure": round(float(contributions[1]), 1),
                "humidity": round(float(contributions[2]), 1),
            },
            "ranked_features": [
                (
                    item["parameter"],
                    float(item["contribution_percent"]),
                    abs(float(item["shap_value"])),
                )
                for item in feature_rows
            ],
            "features": feature_rows,
            "primary_cause": primary["parameter"] if primary else None,
            "anomaly_explanation": narrative,
            "baseline_values": {
                "temperature": round(float(cached.mean[0]), 4),
                "pressure": round(float(cached.mean[1]), 4),
                "humidity": round(float(cached.mean[2]), 4),
            },
            "expected_values": {
                "temperature": float(expected_temp if expected_temp is not None else cached.mean[0]),
                "pressure": float(expected_press if expected_press is not None else cached.mean[1]),
                "humidity": float(expected_humid if expected_humid is not None else cached.mean[2]),
            },
        }

    def get_feature_summary(self, shap_result: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "method": shap_result.get("method"),
            "library_version": shap_result.get("library_version"),
            "primary_cause": shap_result.get("primary_cause"),
            "importance_breakdown": shap_result.get("contribution_percent", {}),
            "explanation": shap_result.get("anomaly_explanation"),
            "ranked_causes": [
                {
                    "feature": item.get("parameter"),
                    "importance_percent": item.get("contribution_percent", 0),
                    "shap_value": item.get("shap_value", 0),
                    "direction": item.get("direction"),
                }
                for item in shap_result.get("features", [])
            ],
        }

    def batch_explain_anomalies(self, anomalies_list):
        return [
            self.calculate_shap_values(station, temp, press, humid)
            for station, temp, press, humid in anomalies_list
        ]

    def visualize_shap_waterfall(self, shap_result: Dict[str, Any]) -> str:
        lines = ["SHAP TreeExplainer Feature Attribution:", "=" * 62]
        for item in shap_result.get("features", []):
            pct = float(item.get("contribution_percent", 0))
            bar = "█" * int(pct / 10) + "░" * max(0, 10 - int(pct / 10))
            lines.append(
                f"{item['parameter']:12} | {bar} | {pct:5.1f}% | SHAP={float(item['shap_value']): .3f} | {item['direction']}"
            )
        lines.append("=" * 62)
        lines.append(f"Model output: {shap_result.get('model_output')}")
        lines.append(f"Model prediction: {shap_result.get('model_prediction')}")
        lines.append(f"Primary cause: {shap_result.get('primary_cause')}")
        return "\n".join(lines)

    def get_interpretable_ranges(self, station_name: str):
        cached = self._model_for_station(station_name)
        lower = cached.mean - 2 * cached.std
        upper = cached.mean + 2 * cached.std
        return {
            "temperature_normal": (float(lower[0]), float(upper[0])),
            "pressure_normal": (float(lower[1]), float(upper[1])),
            "humidity_normal": (float(lower[2]), float(upper[2])),
        }
