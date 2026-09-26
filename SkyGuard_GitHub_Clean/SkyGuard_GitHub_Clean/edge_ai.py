"""SkyGuard Edge AI hard-anomaly gate.

Designed for two environments:
1. Python edge emulator used for the SIH demonstration.
2. The same decision logic is small enough to port to ESP32 firmware.

The runtime does not require scikit-learn: it loads a compact exported decision
 tree from ``edge_ai_model.json`` and performs inference with simple floating-
 point comparisons. A deterministic safety gate catches invalid/out-of-range
 sensor values before the learned tree runs.
"""
from __future__ import annotations

import json
import math
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional

MODEL_PATH = Path(__file__).resolve().with_name("edge_ai_model.json")

# These are hard physical / instrumentation bounds, intentionally conservative.
# They are a safety layer, not a substitute for the learned classifier.
SAFETY_LIMITS = {
    "temperature": (-40.0, 60.0),
    "pressure": (800.0, 1100.0),
    "humidity": (0.0, 100.0),
}

FEATURE_ORDER = [
    "temperature",
    "pressure",
    "humidity",
    "abs_temp_delta",
    "abs_pressure_delta",
    "abs_humidity_delta",
    "temp_std5",
    "pressure_std5",
    "humidity_std5",
]


class EdgeAIHardAnomalyGate:
    """Lightweight local hard-anomaly classifier."""

    def __init__(self, model_path: str | Path = MODEL_PATH, history_size: int = 5):
        self.model_path = Path(model_path)
        self.model = json.loads(self.model_path.read_text(encoding="utf-8"))
        self.history: deque[tuple[float, float, float]] = deque(maxlen=history_size)
        self.observed_history: deque[tuple[float, float, float]] = deque(maxlen=history_size)
        self.history_size = history_size
        self.total_processed = 0
        self.hard_blocked = 0

    @staticmethod
    def _finite(value: Any) -> bool:
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError):
            return False

    def _features(self, temperature: float, pressure: float, humidity: float) -> Dict[str, float]:
        last = self.history[-1] if self.history else (temperature, pressure, humidity)
        t_hist = [x[0] for x in self.history]
        p_hist = [x[1] for x in self.history]
        h_hist = [x[2] for x in self.history]
        return {
            "temperature": float(temperature),
            "pressure": float(pressure),
            "humidity": float(humidity),
            "abs_temp_delta": abs(float(temperature) - float(last[0])),
            "abs_pressure_delta": abs(float(pressure) - float(last[1])),
            "abs_humidity_delta": abs(float(humidity) - float(last[2])),
            "temp_std5": float(_std(t_hist)),
            "pressure_std5": float(_std(p_hist)),
            "humidity_std5": float(_std(h_hist)),
        }

    def _walk_tree(self, node: Dict[str, Any], features: Dict[str, float]) -> Dict[str, float]:
        while not node.get("leaf", False):
            value = float(features.get(node["feature"], 0.0))
            node = node["left"] if value <= float(node["threshold"]) else node["right"]
        return {
            "prob_normal": float(node.get("prob_normal", 0.0)),
            "prob_hard": float(node.get("prob_hard", 0.0)),
        }

    def predict(self, temperature: float, pressure: float, humidity: float) -> Dict[str, Any]:
        """Run local inference without sending the reading anywhere."""
        self.total_processed += 1
        vals = {
            "temperature": temperature,
            "pressure": pressure,
            "humidity": humidity,
        }

        # Non-finite values are always a local hard fault.
        invalid = [k for k, v in vals.items() if not self._finite(v)]
        if invalid:
            self.hard_blocked += 1
            return {
                "decision": "HARD_ANOMALY",
                "hard_anomaly": True,
                "confidence": 100.0,
                "type": "INVALID_VALUE",
                "reason": f"Non-finite value detected in {', '.join(invalid)}",
                "model": "Safety gate",
                "features": {},
            }

        t, p, h = float(temperature), float(pressure), float(humidity)
        range_violations = []
        for key, value in (("temperature", t), ("pressure", p), ("humidity", h)):
            lo, hi = SAFETY_LIMITS[key]
            if value < lo or value > hi:
                range_violations.append(f"{key}={value:g} outside [{lo:g}, {hi:g}]")
        if range_violations:
            self.hard_blocked += 1
            return {
                "decision": "HARD_ANOMALY",
                "hard_anomaly": True,
                "confidence": 100.0,
                "type": "RANGE_VIOLATION",
                "reason": "; ".join(range_violations),
                "model": "Safety gate",
                "features": {},
            }

        # Retain every observed sample for local signature detection. The learned
        # model itself uses the trusted history so a blocked fault cannot poison
        # the reference distribution.
        self.observed_history.append((t, p, h))
        features = self._features(t, p, h)

        # Fast hard-fault signatures: these are deliberately conservative and
        # cheap enough for an ESP32. They prevent obvious faults from reaching
        # the central server at all.
        if len(self.observed_history) >= 2:
            if features["abs_temp_delta"] >= 12.0 or features["abs_pressure_delta"] >= 20.0 or features["abs_humidity_delta"] >= 30.0:
                self.hard_blocked += 1
                changed = []
                if features["abs_temp_delta"] >= 12.0: changed.append(f"temperature jump {features['abs_temp_delta']:.1f}°C")
                if features["abs_pressure_delta"] >= 20.0: changed.append(f"pressure jump {features['abs_pressure_delta']:.1f} hPa")
                if features["abs_humidity_delta"] >= 30.0: changed.append(f"humidity jump {features['abs_humidity_delta']:.1f}%")
                return {
                    "decision": "HARD_ANOMALY", "hard_anomaly": True,
                    "confidence": 99.5, "type": "ABRUPT_SPIKE",
                    "reason": "; ".join(changed), "model": "Edge fast-path + portable ML gate",
                    "features": {k: round(float(v), 5) for k, v in features.items()}
                }

        if len(self.observed_history) >= self.history_size:
            # A channel frozen to the same value for the full local window is a
            # high-confidence hardware fault.
            channels = [(0, "Temperature"), (1, "Pressure"), (2, "Humidity")]
            frozen_channel = None
            for idx, name in channels:
                vals5 = [x[idx] for x in self.observed_history]
                if max(vals5) - min(vals5) < 1e-6:
                    frozen_channel = name
                    break
            if frozen_channel:
                self.hard_blocked += 1
                return {
                    "decision": "HARD_ANOMALY", "hard_anomaly": True,
                    "confidence": 99.0, "type": "FROZEN_SENSOR",
                    "reason": f"{frozen_channel} unchanged across {self.history_size} local samples",
                    "model": "Edge frozen-value detector + portable ML gate",
                    "features": {k: round(float(v), 5) for k, v in features.items()}
                }

        probs = self._walk_tree(self.model["tree"], features)
        hard_probability = probs["prob_hard"]
        is_hard = hard_probability >= 0.50
        if is_hard:
            self.hard_blocked += 1

        if is_hard:
            fault_type = "EDGE_ML_HARD_ANOMALY"
            reason = "Compact edge decision tree classified the observation as a hard anomaly"
        else:
            fault_type = "NORMAL"
            reason = "Observation passed the local hard-anomaly gate"

        return {
            "decision": "HARD_ANOMALY" if is_hard else "PASS",
            "hard_anomaly": bool(is_hard),
            "confidence": round(float(max(hard_probability, 1.0 - hard_probability) * 100.0), 1),
            "type": fault_type,
            "reason": reason,
            "model": "Portable Decision Tree + edge safety gate",
            "model_hard_probability": round(float(hard_probability), 4),
            "features": {k: round(float(v), 5) for k, v in features.items()},
        }

    def accept_and_record(self, temperature: float, pressure: float, humidity: float) -> Dict[str, Any]:
        """Classify a reading, then remember it only if it is not locally blocked."""
        result = self.predict(temperature, pressure, humidity)
        if not result["hard_anomaly"]:
            self.history.append((float(temperature), float(pressure), float(humidity)))
        return result


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))


__all__ = ["EdgeAIHardAnomalyGate", "SAFETY_LIMITS"]
