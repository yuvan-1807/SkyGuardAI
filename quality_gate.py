"""SkyGuard AI - Phase 3F Observation Quality Gate.

Adds a persisted observation lifecycle on top of the Phase 3E decision engine:

RAW RECEIVED -> QUALITY GATE -> VALIDATED / QUARANTINED / REVIEW REQUIRED

The raw meteorological values are never overwritten.  Every decision is
recorded on the observation and in an append-only lineage table.  Only rows
that are explicitly VALIDATED, clean, non-imputed, and not quarantined are
exposed through the `validated_observations` training view.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

NORMAL = "NORMAL"
WEATHER_EVENT = "GENUINE_WEATHER_EVENT"
SENSOR_ANOMALY = "SENSOR_ANOMALY"

VALIDATED = "VALIDATED"
QUARANTINED = "QUARANTINED"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
NONE = "NONE"
QUARANTINE_CANDIDATE = "QUARANTINE_CANDIDATE"


class ObservationQualityGate:
    """Apply deterministic data-quality lifecycle rules to an analysis result."""

    MODEL_VERSION = "phase3f-quality-gate-v1"

    def evaluate(
        self,
        analysis: Dict[str, Any],
        *,
        source: str = "API",
        imputation_status: str = NONE,
    ) -> Dict[str, Any]:
        classification = str(analysis.get("classification") or NORMAL).upper()
        confidence = self._float(analysis.get("confidence"))
        reason = str(analysis.get("reason") or analysis.get("root_cause") or "No quality-gate reason supplied")
        review_required = bool((analysis.get("layers") or {}).get("decision_fusion", {}).get("review_required"))
        imputation = str(imputation_status or NONE).upper()

        if classification not in {NORMAL, WEATHER_EVENT, SENSOR_ANOMALY}:
            classification = SENSOR_ANOMALY
            reason = f"Unsupported classification coerced to SENSOR_ANOMALY: {classification}"

        # Any imputed/self-healed observation is kept out of the trusted
        # training source, even when the surrounding event is otherwise normal.
        if imputation not in {NONE, "NOT_IMPUTED"}:
            return self._result(
                classification=classification,
                is_clean=False,
                validation_status=QUARANTINED,
                quarantine_status=QUARANTINED,
                imputation_status=imputation,
                confidence=confidence,
                reason=f"Imputed/self-healed observation is quarantined: {imputation}",
                source=source,
                training_eligible=False,
            )

        if classification == SENSOR_ANOMALY:
            return self._result(
                classification=classification,
                is_clean=False,
                validation_status=QUARANTINED,
                quarantine_status=QUARANTINED,
                imputation_status=NONE,
                confidence=confidence,
                reason=reason,
                source=source,
                training_eligible=False,
            )

        # A strong weather/sensor conflict remains visible for operator review.
        # It is held out of training until a human or later validation step resolves it.
        if classification == WEATHER_EVENT and review_required:
            return self._result(
                classification=classification,
                is_clean=False,
                validation_status=REVIEW_REQUIRED,
                quarantine_status=QUARANTINE_CANDIDATE,
                imputation_status=NONE,
                confidence=confidence,
                reason="Weather-event classification has conflicting sensor evidence and requires review",
                source=source,
                training_eligible=False,
            )

        # Normal observations and physically consistent weather events are valid
        # measurements, so they can remain trusted for future training.
        return self._result(
            classification=classification,
            is_clean=True,
            validation_status=VALIDATED,
            quarantine_status=NONE,
            imputation_status=NONE,
            confidence=confidence,
            reason=reason,
            source=source,
            training_eligible=True,
        )

    @staticmethod
    def _float(value: Any) -> float:
        try:
            x = float(value or 0.0)
            return x if x == x and abs(x) != float("inf") else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _result(self, **kwargs: Any) -> Dict[str, Any]:
        return {
            "quality_gate_model_version": self.MODEL_VERSION,
            "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            **kwargs,
        }


__all__ = ["ObservationQualityGate"]
