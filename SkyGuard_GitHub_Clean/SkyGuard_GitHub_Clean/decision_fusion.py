"""
SkyGuard AI - Phase 3E
Multi-evidence Decision Fusion.

This module converts the independent evidence layers into one explicit
three-level classification while preserving the existing v37 boolean
`is_anomaly` semantics:

    NORMAL
    GENUINE_WEATHER_EVENT
    SENSOR_ANOMALY

The fusion engine is intentionally deterministic and explainable. Hard sensor
fault evidence (physical impossibility, frozen sensor, confirmed security
threat) can veto a weather-event classification. Otherwise the engine compares
weighted sensor-fault evidence against weather-context evidence.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

MODEL_VERSION = "phase3e-decision-fusion-v1"

# Weights reflect the intended multi-evidence design. They are not probabilities.
SENSOR_WEIGHTS = {
    "input": 0.10,
    "temporal": 0.18,
    "multivariate": 0.18,
    "ml": 0.24,
    "spatial": 0.20,
    "security": 0.10,
}
WEATHER_TRANSITION_WEIGHT = 0.55
WEATHER_CONFIDENCE_WEIGHT = 0.45
MIN_SENSOR_SCORE = 60.0
MIN_WEATHER_SCORE = 60.0
WEATHER_MARGIN = 5.0


class DecisionFusionEngine:
    """Combine edge/ML/context evidence into the final three-way decision."""

    def __init__(self) -> None:
        self.model_version = MODEL_VERSION

    @staticmethod
    def _f(value: Any, default: float = 0.0) -> float:
        try:
            x = float(value)
            if x != x or x in (float("inf"), float("-inf")):
                return float(default)
            return max(0.0, min(100.0, x))
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _candidate_conf(layer: Mapping[str, Any], candidate_key: str = "anomaly_candidate") -> float:
        if not bool(layer.get(candidate_key, False)):
            return 0.0
        return DecisionFusionEngine._f(layer.get("confidence", 0.0))

    @staticmethod
    def _parameter_peak(layer: Mapping[str, Any]) -> Tuple[bool, float, str | None]:
        parameters = layer.get("parameters") or {}
        strongest: Tuple[str | None, float] = (None, 0.0)
        for name, result in parameters.items():
            if not isinstance(result, Mapping):
                continue
            if not bool(result.get("anomaly_candidate", False)):
                continue
            confidence = DecisionFusionEngine._f(result.get("confidence", 0.0))
            if confidence > strongest[1]:
                strongest = (str(name), confidence)
        return strongest[0] is not None, strongest[1], strongest[0]

    def _sensor_evidence(self, layers: Mapping[str, Any], security_confirmed: bool,
                         supplied_anomaly: bool, supplied_confidence: float) -> Dict[str, Any]:
        evidence: List[Dict[str, Any]] = []

        if supplied_anomaly:
            evidence.append({
                "source": "input",
                "confidence": self._f(supplied_confidence),
                "weight": SENSOR_WEIGHTS["input"],
                "reason": "Incoming observation was explicitly marked anomalous",
            })

        temporal = layers.get("temporal") or {}
        if temporal.get("is_anomaly"):
            evidence.append({
                "source": "temporal",
                "confidence": self._f(temporal.get("score")),
                "weight": SENSOR_WEIGHTS["temporal"],
                "reason": temporal.get("reason", "Temporal deviation"),
            })

        multivariate = layers.get("multivariate") or {}
        if multivariate.get("is_anomaly"):
            evidence.append({
                "source": "multivariate",
                "confidence": self._f(multivariate.get("score")),
                "weight": SENSOR_WEIGHTS["multivariate"],
                "reason": multivariate.get("reason", "Multivariate inconsistency"),
            })

        ml = layers.get("ml_evidence") or {}
        ml_peak, ml_conf, ml_parameter = self._parameter_peak(ml)
        if ml.get("anomaly_candidate") or ml_peak:
            evidence.append({
                "source": "ml",
                "confidence": max(self._f(ml.get("confidence")), ml_conf),
                "weight": SENSOR_WEIGHTS["ml"],
                "reason": f"ExtraTrees residual evidence ({ml_parameter or ml.get('strongest_parameter') or 'sensor'})",
            })

        spatial = layers.get("spatial_evidence") or {}
        spatial_peak, spatial_conf, spatial_parameter = self._parameter_peak(spatial)
        if spatial.get("anomaly_candidate") or spatial_peak:
            evidence.append({
                "source": "spatial",
                "confidence": max(self._f(spatial.get("confidence")), spatial_conf),
                "weight": SENSOR_WEIGHTS["spatial"],
                "reason": f"Spatial residual evidence ({spatial_parameter or spatial.get('strongest_parameter') or 'sensor'})",
            })

        if security_confirmed:
            threat = layers.get("quarantine") or {}
            evidence.append({
                "source": "security",
                "confidence": max(90.0, self._f(threat.get("threat_score", 0.0) * 100.0)),
                "weight": SENSOR_WEIGHTS["security"],
                "reason": "Confirmed security/integrity anomaly",
            })

        total_weight = sum(item["weight"] for item in evidence)
        weighted = (sum(item["confidence"] * item["weight"] for item in evidence) / total_weight) if total_weight else 0.0
        support = min(1.0, len(evidence) / 3.0)
        # Evidence from more independent layers receives more confidence than a
        # single detector acting alone, while preserving the detector strengths.
        fused = weighted * (0.65 + 0.35 * support)
        return {
            "score": round(fused, 2),
            "raw_weighted_score": round(weighted, 2),
            "supporting_layers": len(evidence),
            "evidence": evidence,
        }

    def fuse(self, *, layers: Mapping[str, Any], supplied_anomaly: bool = False,
             supplied_confidence: float = 0.0, security_confirmed: bool = False) -> Dict[str, Any]:
        """Return the final classification and transparent fusion evidence."""
        psychro = layers.get("psychrometrics") or {}
        entropy = layers.get("entropy") or {}
        weather = layers.get("weather_context") or {}

        physical_invalid = not bool(psychro.get("is_physically_valid", True))
        frozen = bool(entropy.get("is_frozen", False))

        # Hard gates: these represent direct evidence of an unreliable sensor or
        # compromised transport/data integrity. They override weather context.
        hard_faults: List[Dict[str, Any]] = []
        if physical_invalid:
            hard_faults.append({
                "source": "psychrometrics",
                "confidence": 100.0 - self._f(psychro.get("confidence_score", 100.0)),
                "reason": "; ".join(psychro.get("violations", [])) or "Physical consistency violation",
            })
        if frozen:
            hard_faults.append({
                "source": "entropy",
                "confidence": self._f(entropy.get("confidence")),
                "reason": entropy.get("reason", "Frozen sensor"),
            })
        if security_confirmed:
            threat = self._f((layers.get("quarantine") or {}).get("threat_score", 0.0) * 100.0)
            hard_faults.append({
                "source": "security",
                "confidence": max(90.0, threat),
                "reason": "Confirmed security/integrity anomaly",
            })

        sensor = self._sensor_evidence(
            layers,
            security_confirmed=security_confirmed,
            supplied_anomaly=supplied_anomaly,
            supplied_confidence=supplied_confidence,
        )

        weather_candidate = bool(weather.get("weather_event_candidate", False)) and not physical_invalid and not frozen
        weather_transition = self._f(weather.get("weather_transition_score"))
        weather_confidence = self._f(weather.get("confidence"))
        weather_score = (WEATHER_TRANSITION_WEIGHT * weather_transition + WEATHER_CONFIDENCE_WEIGHT * weather_confidence) if weather_candidate else 0.0
        weather_score = round(weather_score, 2)

        if hard_faults:
            classification = "SENSOR_ANOMALY"
            final_conf = max([self._f(x.get("confidence")) for x in hard_faults] + [sensor["score"]])
            decision_reason = "Hard sensor/integrity evidence overrides weather-context classification"
            review_required = False
        elif weather_candidate and weather_score >= MIN_WEATHER_SCORE and sensor["score"] < MIN_SENSOR_SCORE:
            classification = "GENUINE_WEATHER_EVENT"
            final_conf = weather_score
            decision_reason = weather.get("reason", "Physically consistent coordinated atmospheric transition")
            review_required = False
        elif sensor["score"] >= MIN_SENSOR_SCORE and sensor["score"] >= weather_score + WEATHER_MARGIN:
            classification = "SENSOR_ANOMALY"
            final_conf = sensor["score"]
            decision_reason = "; ".join(dict.fromkeys(item["reason"] for item in sensor["evidence"])) or "Multi-layer sensor anomaly evidence"
            review_required = False
        elif weather_candidate and weather_score >= MIN_WEATHER_SCORE:
            # When both interpretations are strong, prefer the physically-consistent
            # weather explanation unless sensor evidence clearly dominates. The
            # conflict remains visible to operators through review_required.
            classification = "GENUINE_WEATHER_EVENT"
            final_conf = weather_score
            decision_reason = "Weather-context evidence is strong; sensor evidence is not clearly dominant"
            review_required = sensor["score"] >= MIN_SENSOR_SCORE
        else:
            classification = "NORMAL"
            final_conf = max(sensor["score"], weather_score)
            decision_reason = "No evidence combination crossed the final classification criteria"
            review_required = False

        contributing = sorted(
            sensor["evidence"],
            key=lambda item: (float(item.get("confidence", 0.0)) * float(item.get("weight", 0.0))),
            reverse=True,
        )

        return {
            "classification": classification,
            "confidence": round(min(100.0, max(0.0, final_conf)), 2),
            "sensor_anomaly_score": round(sensor["score"], 2),
            "weather_event_score": weather_score,
            "weather_event_candidate": weather_candidate,
            "hard_fault": bool(hard_faults),
            "hard_faults": hard_faults,
            "supporting_layers": sensor["supporting_layers"],
            "contributing_evidence": contributing,
            "weather_evidence": {
                "candidate": weather_candidate,
                "transition_score": weather_transition,
                "confidence": weather_confidence,
                "fused_score": weather_score,
                "reason": weather.get("reason"),
            },
            "sensor_evidence": sensor,
            "review_required": review_required,
            "decision_reason": decision_reason,
            "method": "Weighted multi-evidence decision fusion with hard-fault safety gates (Phase 3E)",
            "model_version": self.model_version,
        }


__all__ = ["DecisionFusionEngine", "MODEL_VERSION"]
