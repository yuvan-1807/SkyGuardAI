"""
SkyGuard AI - Phase 2A
Unified anomaly-processing pipeline.

One canonical path for every incoming AWS observation:
validation -> temporal -> multivariate -> physics -> entropy -> security
-> explainability -> consolidated verdict.

The pipeline is intentionally side-effect aware: ingestion calls process_reading(),
while dashboard inspection calls analyze_reading() and does not advance state twice.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np

from config import DB_PATH
from psychrometrics_layer import PsychrometricsLayer
from weather_context import WeatherContextEngine
from extra_trees_evidence import ExtraTreesEvidence
from spatial_evidence import SpatialConsistencyEvidence
from decision_fusion import DecisionFusionEngine
from entropy_trap import EntropyTrap
from quarantine_layer import QuarantineLayer
from self_healing_engine import SelfHealingEngine
from shap_explainer import SHAPExplainer


class AnomalyPipeline:
    """Consolidates the existing SkyGuard detection layers into one verdict."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.psychro = PsychrometricsLayer()
        self.weather_context = WeatherContextEngine()
        self.ml_evidence = ExtraTreesEvidence(db_path)
        self.spatial_evidence = SpatialConsistencyEvidence(db_path)
        self.decision_fusion = DecisionFusionEngine()
        self.entropy = EntropyTrap()
        self.quarantine = QuarantineLayer(db_path)
        self.healing = SelfHealingEngine(db_path)
        self.shap = SHAPExplainer(db_path)
        self._entropy_primed = set()

    # ---------- data helpers ----------
    def _query(self, sql: str, params=()):
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            conn.close()

    def recent_readings(self, station: str, limit: int = 50, before_event_time: Optional[str] = None) -> List[tuple]:
        if before_event_time:
            return self._query(
                """SELECT temperature, pressure, humidity, COALESCE(event_time, timestamp), is_anomaly
                   FROM sensor_readings
                   WHERE station_name = ? AND COALESCE(event_time, timestamp) < ?
                   ORDER BY COALESCE(event_time, timestamp) DESC, id DESC LIMIT ?""",
                (station, before_event_time, limit),
            )
        return self._query(
            """SELECT temperature, pressure, humidity, COALESCE(event_time, timestamp), is_anomaly
               FROM sensor_readings
               WHERE station_name = ?
               ORDER BY COALESCE(event_time, timestamp) DESC, id DESC LIMIT ?""",
            (station, limit),
        )

    def _baseline(self, station: str, before_event_time: Optional[str] = None) -> Dict[str, float]:
        where = "station_name = ? AND is_anomaly = 0"
        params: tuple = (station,)
        if before_event_time:
            where += " AND COALESCE(event_time, timestamp) < ?"
            params = (station, before_event_time)
        row = self._query(
            f"""SELECT AVG(temperature), AVG(pressure), AVG(humidity),
                      COUNT(*),
                      COALESCE(AVG(temperature*temperature) - AVG(temperature)*AVG(temperature), 0),
                      COALESCE(AVG(pressure*pressure) - AVG(pressure)*AVG(pressure), 0),
                      COALESCE(AVG(humidity*humidity) - AVG(humidity)*AVG(humidity), 0)
               FROM sensor_readings
               WHERE {where}""",
            params,
        )[0]
        return {
            "temperature": float(row[0]) if row[0] is not None else 25.0,
            "pressure": float(row[1]) if row[1] is not None else 1013.0,
            "humidity": float(row[2]) if row[2] is not None else 60.0,
            "count": int(row[3] or 0),
            "temperature_std": max(float(row[4] or 0) ** 0.5, 1.0),
            "pressure_std": max(float(row[5] or 0) ** 0.5, 1.0),
            "humidity_std": max(float(row[6] or 0) ** 0.5, 1.0),
        }

    def _prime_entropy(self, station: str):
        """Restore enough history after a server restart so freeze detection works."""
        if station in self._entropy_primed:
            return
        rows = self.recent_readings(station, self.entropy.window_size - 1)
        for temp, pressure, humidity, _ts, _anom in reversed(rows):
            self.entropy.update_history(station, float(temp), float(pressure), float(humidity))
        self._entropy_primed.add(station)

    @staticmethod
    def _z(value: float, mean: float, std: float) -> float:
        return abs(float(value) - mean) / max(std, 1e-6)

    def _temporal_analysis(self, station: str, temperature: float, pressure: float, humidity: float, before_event_time: Optional[str] = None) -> Dict[str, Any]:
        rows = self.recent_readings(station, 30, before_event_time)
        if len(rows) < 5:
            return {
                "is_anomaly": False,
                "score": 0.0,
                "reason": "Insufficient history for temporal analysis",
                "baseline_samples": len(rows),
            }

        # rows are newest first; reverse for chronological calculations.
        rows = list(reversed(rows))
        arrays = {
            "temperature": np.array([r[0] for r in rows if r[0] is not None], dtype=float),
            "pressure": np.array([r[1] for r in rows if r[1] is not None], dtype=float),
            "humidity": np.array([r[2] for r in rows if r[2] is not None], dtype=float),
        }
        current = {"temperature": temperature, "pressure": pressure, "humidity": humidity}
        signals = []
        for name, values in arrays.items():
            if len(values) < 5:
                continue
            recent = values[-10:]
            median = float(np.median(recent))
            mad = float(np.median(np.abs(recent - median)))
            scale = max(1.4826 * mad, float(np.std(recent)), 1e-6)
            score = abs(current[name] - median) / scale
            if score >= 5:
                signals.append((name, score))

        if not signals:
            return {"is_anomaly": False, "score": 0.0, "reason": "Temporal pattern within recent baseline", "baseline_samples": len(rows)}

        strongest = max(signals, key=lambda x: x[1])
        # A 5-sigma threshold is intentionally conservative for weather data.
        # This reduces false alarms from ordinary short-term meteorological variability.
        confidence = min(100.0, 75.0 + (strongest[1] - 5.0) * 6.25)
        return {
            "is_anomaly": True,
            "score": confidence,
            "reason": f"{strongest[0].title()} temporal deviation ({strongest[1]:.1f}σ from recent baseline)",
            "baseline_samples": len(rows),
            "signals": [{"parameter": n, "z_score": float(s)} for n, s in signals],
        }

    def _multivariate_analysis(self, station: str, temperature: float, pressure: float, humidity: float, before_event_time: Optional[str] = None) -> Dict[str, Any]:
        b = self._baseline(station, before_event_time)
        z = {
            "temperature": self._z(temperature, b["temperature"], b["temperature_std"]),
            "pressure": self._z(pressure, b["pressure"], b["pressure_std"]),
            "humidity": self._z(humidity, b["humidity"], b["humidity_std"]),
        }
        # Flag when the joint observation is substantially outside the station's normal envelope.
        joint = float(np.sqrt(sum(v * v for v in z.values())))
        anomaly = joint >= 4.0 or max(z.values()) >= 4.5
        confidence = min(100.0, max(0.0, 55.0 + (joint - 4.0) * 8.0)) if anomaly else 0.0
        primary = max(z, key=z.get)
        return {
            "is_anomaly": anomaly,
            "score": confidence,
            "joint_z_score": joint,
            "z_scores": z,
            "primary_parameter": primary,
            "baseline": b,
            "reason": f"Multivariate deviation dominated by {primary}" if anomaly else "Multivariate pattern within baseline",
        }

    # ---------- canonical processing ----------
    def analyze_reading(self, station: str, temperature: float, pressure: float, humidity: float,
                        supplied_anomaly: bool = False, supplied_reason: Optional[str] = None,
                        supplied_confidence: Optional[float] = None,
                        update_state: bool = False, include_shap: bool = False,
                        event_time: Optional[str] = None, out_of_order: bool = False) -> Dict[str, Any]:
        """Analyze one reading and return a stable, dashboard/API-friendly result."""
        effective_before = str(event_time) if event_time else None
        if update_state and not out_of_order:
            self._prime_entropy(station)
            entropy_result = self.entropy.detect_frozen_sensor(station, temperature, pressure, humidity)
        else:
            # Read-only mode is also used for late/backfilled observations so a delayed
            # reading never mutates the current rolling detector state.
            rows = self.recent_readings(station, self.entropy.window_size - 1, effective_before)
            temps = [float(r[0]) for r in rows if r[0] is not None] + [float(temperature)]
            presses = [float(r[1]) for r in rows if r[1] is not None] + [float(pressure)]
            humids = [float(r[2]) for r in rows if r[2] is not None] + [float(humidity)]
            entropy_result = {
                "is_frozen": False, "frozen_parameter": None, "confidence": 0,
                "severity": "Low", "reason": "No frozen parameter detected", "entropy_scores": {}
            }
            for name, values, threshold, conf in [
                ("Temperature", temps, 1e-4, 95),
                ("Pressure", presses, 1e-3, 92),
                ("Humidity", humids, 1e-3, 88),
            ]:
                if len(values) >= self.entropy.window_size:
                    variance = float(np.var(values))
                    entropy_result["entropy_scores"][name.lower()] = float(self.entropy.calculate_entropy(values))
                    if variance < threshold:
                        entropy_result.update({
                            "is_frozen": True, "frozen_parameter": name,
                            "confidence": conf, "severity": "Critical",
                            "reason": f"{name} frozen (near-zero variance over rolling window)"
                        })
                        break

        psychro = self.psychro.validate_reading(temperature, humidity, pressure)
        temporal = self._temporal_analysis(station, temperature, pressure, humidity, effective_before)
        multivariate = self._multivariate_analysis(station, temperature, pressure, humidity, effective_before)

        recent = self.recent_readings(station, 25, effective_before)
        analysis_event_time = event_time or (recent[-1][3] if recent else datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        temp_history = [float(r[0]) for r in reversed(recent) if r[0] is not None]
        temp_history.append(float(temperature))
        quarantine = self.quarantine.scan_for_attacks(station, temperature, pressure, humidity, temp_history)

        weather_history = [
            {"temperature": r[0], "pressure": r[1], "humidity": r[2], "timestamp": r[3]}
            for r in reversed(recent)
        ]
        weather_context = self.weather_context.analyze(
            temperature, pressure, humidity, history=weather_history,
            frozen=bool(entropy_result.get("is_frozen"))
        )
        ml_evidence = self.ml_evidence.analyze(
            station, analysis_event_time, temperature, pressure, humidity
        )
        spatial_evidence = self.spatial_evidence.analyze(
            station, analysis_event_time, temperature, pressure, humidity
        )

        baseline = multivariate["baseline"]
        # Keep real SHAP off the live ingestion path. It is computed only for
        # read-only anomaly inspection so adding explainability cannot disturb
        # the proven injector -> API -> database flow.
        if include_shap:
            shap_result = self.shap.calculate_shap_values(
                station, temperature, pressure, humidity,
                baseline["temperature"], baseline["pressure"], baseline["humidity"]
            )
        else:
            shap_result = {
                "available": False,
                "method": "Deferred until anomaly inspection",
                "primary_cause": None,
                "anomaly_explanation": "SHAP is computed on the read-only anomaly explanation path.",
                "shap_values": {},
                "contribution_percent": {},
                "features": [],
            }

        signals = []
        if supplied_anomaly:
            signals.append({"source": "input", "confidence": float(supplied_confidence or 0), "reason": supplied_reason or "Input marked as anomaly"})
        if not psychro.get("is_physically_valid", True):
            signals.append({"source": "psychrometrics", "confidence": 100.0 - float(psychro.get("confidence_score", 100)), "reason": "; ".join(psychro.get("violations", []))})
        if entropy_result.get("is_frozen"):
            signals.append({"source": "entropy", "confidence": float(entropy_result.get("confidence", 0)), "reason": entropy_result.get("reason", "Frozen sensor")})
        if temporal.get("is_anomaly"):
            signals.append({"source": "temporal", "confidence": float(temporal.get("score", 0)), "reason": temporal.get("reason")})
        if multivariate.get("is_anomaly"):
            signals.append({"source": "multivariate", "confidence": float(multivariate.get("score", 0)), "reason": multivariate.get("reason")})
        # Phase 3B produces weather-event evidence only. It is intentionally not
        # promoted to the anomaly verdict yet; Decision Fusion will own that
        # classification in the next phase.
        # The existing security layer intentionally reports low/medium suspicion too.
        # Phase 2A only promotes high-confidence security findings to the meteorological
        # anomaly verdict; weaker patterns remain visible as a security signal.
        security_confirmed = float(quarantine.get("threat_score", 0)) >= 0.90
        if security_confirmed:
            signals.append({"source": "quarantine", "confidence": float(quarantine.get("threat_score", 0)) * 100, "reason": (quarantine.get("detected_attacks") or [{}])[0].get("reason", "Security anomaly")})
        if ml_evidence.get("anomaly_candidate"):
            signals.append({"source": "ml", "confidence": float(ml_evidence.get("confidence", 0)), "reason": "ExtraTrees residual evidence"})
        if spatial_evidence.get("anomaly_candidate"):
            signals.append({"source": "spatial", "confidence": float(spatial_evidence.get("confidence", 0)), "reason": spatial_evidence.get("reason", "Spatial residual evidence")})

        fusion_layers = {
            "psychrometrics": psychro,
            "entropy": entropy_result,
            "temporal": temporal,
            "multivariate": multivariate,
            "weather_context": weather_context,
            "ml_evidence": ml_evidence,
            "spatial_evidence": spatial_evidence,
            "quarantine": quarantine,
        }
        decision = self.decision_fusion.fuse(
            layers=fusion_layers,
            supplied_anomaly=supplied_anomaly,
            supplied_confidence=float(supplied_confidence or 0.0),
            security_confirmed=security_confirmed,
        )
        classification = decision["classification"]
        detected = classification == "SENSOR_ANOMALY"
        confidence = float(decision["confidence"])

        # Root cause priority is deterministic and preserves familiar v37 labels
        # while adding weather, ML and spatial explanations.
        if classification == "GENUINE_WEATHER_EVENT":
            root_cause = "Genuine weather event"
        elif entropy_result.get("is_frozen"):
            root_cause = "Frozen sensor"
        elif security_confirmed:
            root_cause = "Possible spoofing / data integrity issue"
        elif not psychro.get("is_physically_valid", True):
            root_cause = "Physical consistency violation"
        elif ml_evidence.get("anomaly_candidate"):
            root_cause = f"ML residual anomaly ({ml_evidence.get('strongest_parameter') or 'sensor'})"
        elif spatial_evidence.get("anomaly_candidate"):
            root_cause = f"Spatial inconsistency ({spatial_evidence.get('strongest_parameter') or 'sensor'})"
        elif temporal.get("is_anomaly"):
            root_cause = "Temporal spike / drift"
        elif multivariate.get("is_anomaly"):
            root_cause = "Multivariate inconsistency"
        elif supplied_anomaly:
            root_cause = "Externally flagged anomaly"
        else:
            root_cause = "Normal"

        if classification == "NORMAL":
            severity = "NORMAL"
        elif classification == "GENUINE_WEATHER_EVENT":
            severity = "WEATHER_EVENT"
        elif confidence >= 90 or security_confirmed:
            severity = "CRITICAL"
        elif confidence >= 75 or quarantine.get("threat_level") == "High":
            severity = "HIGH"
        else:
            severity = "MEDIUM"

        return {
            "station_id": station,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "event_time": str(analysis_event_time),
            "is_anomaly": detected,
            "classification": classification,
            "model_version": decision.get("model_version", self.decision_fusion.model_version),
            "severity": severity,
            "confidence": confidence,
            "root_cause": root_cause,
            "reason": decision.get("decision_reason") or ("; ".join(dict.fromkeys(str(s["reason"]) for s in signals)) if signals else "No anomaly detected"),
            "signals": signals,
            "layers": {
                **fusion_layers,
                "decision_fusion": decision,
                "shap": shap_result,
            },
        }

    def process_reading(self, station: str, temperature: float, pressure: float, humidity: float,
                        supplied_anomaly: bool = False, supplied_reason: Optional[str] = None,
                        supplied_confidence: Optional[float] = None,
                        event_time: Optional[str] = None, out_of_order: bool = False) -> Dict[str, Any]:
        """Canonical ingestion operation. Advances state exactly once."""
        return self.analyze_reading(
            station, temperature, pressure, humidity,
            supplied_anomaly, supplied_reason, supplied_confidence,
            update_state=True,
            include_shap=False,
            event_time=event_time,
            out_of_order=out_of_order,
        )


__all__ = ["AnomalyPipeline"]
