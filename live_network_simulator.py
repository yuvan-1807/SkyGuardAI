"""SkyGuard live 10-station network simulator for SIH demonstration.

This is an additive demo/data-simulation layer. It does NOT modify the frozen
/api/ingest path or the existing data_injector.py.

Architecture per 5/10-minute production window:
  10 virtual AWS stations sample locally
        -> Edge AI gate (one gate/history per station)
        -> hard anomalies: block locally + compact edge event
        -> normal/soft observations: local buffer
        -> periodic per-station batch -> /api/edge-batch -> frozen /api/ingest

For SIH video use --demo, which compresses the normal 5/10 minute transmission
window to 10 seconds so the full behavior is visible on camera.
"""
from __future__ import annotations

from db_utils import db_connect

import argparse
import random
import sqlite3
import time
import json
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

import requests

from config import API_URL, DB_PATH, STATIONS, canonical_station_name
from edge_ai import EdgeAIHardAnomalyGate
from edge_transport import SecureEdgeTransport

SKYGUARD_BUILD = "PHASE2D-v36-FIX-SEQUENCE-COLLISION"

API_BASE = API_URL.rsplit("/api/ingest", 1)[0] + "/api"
EDGE_EVENT_URL = f"{API_BASE}/edge-event"
EDGE_HEARTBEAT_URL = f"{API_BASE}/edge-heartbeat"
EDGE_BATCH_URL = f"{API_BASE}/edge-batch"
DEVICE_PROVISION_URL = f"{API_BASE}/device-registry/provision"
DEVICE_PROVISION_BATCH_URL = f"{API_BASE}/device-registry/provision-batch"
DEVICE_REGISTRY_URL = f"{API_BASE}/device-registry"

FALLBACK_PROFILES = {
    "AWS_01_Delhi": (27.5, 1011.5, 55.0),
    "AWS_02_Mumbai": (29.0, 1008.8, 74.0),
    "AWS_03_Chennai": (29.5, 1012.4, 75.0),
    "AWS_04_Himachal": (18.0, 895.0, 68.0),
    "AWS_05_Kolkata": (30.0, 1008.0, 72.0),
    "AWS_06_Bangalore": (24.0, 900.0, 65.0),
    "AWS_07_Kerala": (28.0, 1008.0, 80.0),
    "AWS_08_Rajasthan": (34.0, 1005.0, 42.0),
    "AWS_09_NorthEast": (23.0, 1000.0, 78.0),
    "AWS_10_Gujarat": (31.0, 1008.0, 58.0),
}


@dataclass
class Profile:
    temp_mean: float
    temp_std: float
    pressure_mean: float
    pressure_std: float
    humidity_mean: float
    humidity_std: float


class LiveNetworkSimulator:
    def __init__(
        self,
        cycle_seconds: float = 2.0,
        batch_seconds: float = 600.0,
        scenario: str = "mixed",
        soft_probability: float = 0.08,
        hard_probability: float = 0.03,
        hard_type: str = "SPIKE",
        demo: bool = False,
        seed: int = 20260923,
        secure: bool = False,
        offline_seconds: float = 0.0,
        queue_dir: str = ".edge_outbox",
    ):
        self.cycle_seconds = max(0.25, float(cycle_seconds))
        self.batch_seconds = 10.0 if demo else max(60.0, float(batch_seconds))
        self.scenario = scenario.upper()
        self.soft_probability = max(0.0, min(1.0, soft_probability))
        self.hard_probability = max(0.0, min(1.0, hard_probability))
        self.hard_type = hard_type.upper()
        self.demo = demo
        self.rng = random.Random(seed)
        self.secure = bool(secure)
        self.offline_seconds = max(0.0, float(offline_seconds))
        self.queue_dir = queue_dir
        self.secure_transports = {}
        self._network_was_offline = bool(self.offline_seconds > 0)
        self.device_ids = self._load_demo_device_ids() if self.secure else {}
        if self.secure:
            for station in STATIONS:
                # Demo device IDs are persisted outside the disposable outbox.
                # This prevents replay-protection collisions when --fresh-outbox
                # clears queued messages while the backend retains old receipts.
                device_id = self.device_ids[station]
                self.secure_transports[station] = SecureEdgeTransport(API_BASE, station, device_id, queue_dir=self.queue_dir, offline_seconds=self.offline_seconds)

        self.profiles = self._load_profiles()
        self.gates: Dict[str, EdgeAIHardAnomalyGate] = {
            station: EdgeAIHardAnomalyGate() for station in STATIONS
        }
        self.buffers: Dict[str, deque] = {station: deque(maxlen=1000) for station in STATIONS}
        self.current: Dict[str, Tuple[float, float, float]] = {
            station: (
                self.profiles[station].temp_mean,
                self.profiles[station].pressure_mean,
                self.profiles[station].humidity_mean,
            )
            for station in STATIONS
        }
        self.total_samples = 0
        self.hard_blocked = 0
        self.soft_generated = 0
        self.batches_sent = 0
        self.stats = defaultdict(lambda: {"samples": 0, "hard": 0, "soft": 0, "buffered": 0, "batches": 0})

    def _load_demo_device_ids(self) -> Dict[str, str]:
        path = __import__("pathlib").Path(self.queue_dir).parent / ".skyguard_demo_device_ids.json"
        try:
            existing = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            if all(station in existing and existing[station] for station in STATIONS):
                return {station: str(existing[station]) for station in STATIONS}
        except Exception:
            existing = {}
        run_id = uuid.uuid4().hex[:10].upper()
        ids = {station: f"ESP32-DEMO-{run_id}-{station}" for station in STATIONS}
        path.write_text(json.dumps(ids, indent=2), encoding="utf-8")
        return ids

    def _load_profiles(self) -> Dict[str, Profile]:
        profiles: Dict[str, Profile] = {}
        try:
            with db_connect(DB_PATH, timeout=10) as conn:
                for station in STATIONS:
                    row = conn.execute(
                        """
                        SELECT AVG(temperature),
                               AVG(pressure),
                               AVG(humidity),
                               COUNT(*),
                               AVG(temperature*temperature) - AVG(temperature)*AVG(temperature),
                               AVG(pressure*pressure) - AVG(pressure)*AVG(pressure),
                               AVG(humidity*humidity) - AVG(humidity)*AVG(humidity)
                        FROM sensor_readings
                        WHERE station_name = ? AND COALESCE(is_anomaly, 0) = 0
                        """,
                        (station,),
                    ).fetchone()
                    if row and row[3]:
                        profiles[station] = Profile(
                            temp_mean=float(row[0]),
                            temp_std=max(float(row[4] or 0.0) ** 0.5, 0.5),
                            pressure_mean=float(row[1]),
                            pressure_std=max(float(row[5] or 0.0) ** 0.5, 0.2),
                            humidity_mean=float(row[2]),
                            humidity_std=max(float(row[6] or 0.0) ** 0.5, 1.0),
                        )
        except Exception as exc:
            print(f"[PROFILE] DB baseline read failed: {exc}")

        for station in STATIONS:
            if station not in profiles:
                t, p, h = FALLBACK_PROFILES[station]
                profiles[station] = Profile(t, 2.5, p, 1.0, h, 5.0)
        return profiles

    def normal_reading(self, station: str) -> Tuple[float, float, float]:
        """Generate a smooth, station-specific atmospheric stream."""
        profile = self.profiles[station]
        last_t, last_p, last_h = self.current[station]
        # Gentle mean reversion + noise gives continuity while staying realistic.
        t = last_t + 0.18 * (profile.temp_mean - last_t) + self.rng.gauss(0, max(0.12, profile.temp_std * 0.12))
        p = last_p + 0.18 * (profile.pressure_mean - last_p) + self.rng.gauss(0, max(0.05, profile.pressure_std * 0.12))
        h = last_h + 0.18 * (profile.humidity_mean - last_h) + self.rng.gauss(0, max(0.18, profile.humidity_std * 0.12))
        h = max(2.0, min(98.0, h))
        reading = (round(t, 2), round(p, 2), round(h, 2))
        self.current[station] = reading
        return reading

    def soft_anomaly(self, station: str) -> Tuple[float, float, float]:
        """Create a backend-visible anomaly that stays below Edge hard thresholds.

        A pressure-dominant ~4.5-sigma shift plus smaller temperature/humidity
        shifts is intentionally subtle in absolute units, so the edge gate should
        pass it while the backend multivariate detector can flag the joint deviation.
        """
        profile = self.profiles[station]
        t, p, h = self.current[station]

        dt = min(7.5, 1.5 * profile.temp_std)
        dp = min(12.0, 4.5 * profile.pressure_std)
        dh = min(18.0, 1.0 * profile.humidity_std)

        # Alternate sign so the event is not always an upward shift.
        sign = -1.0 if self.rng.random() < 0.35 else 1.0
        reading = (
            round(t + sign * dt, 2),
            round(p + sign * dp, 2),
            round(max(2.0, min(98.0, h - sign * dh)), 2),
        )
        self.current[station] = reading
        return reading

    @staticmethod
    def hard_anomaly(fault_type: str, station: str, current: Tuple[float, float, float]):
        t, p, h = current
        if fault_type == "SPIKE":
            return 58.0, 1070.0, 15.0
        if fault_type == "PHYSICS_BREACH":
            return 54.5, 1012.4, 99.0
        if fault_type == "INVALID":
            return float("nan"), p, h
        # FROZEN_SENSOR is handled by the caller by repeatedly using the same current value.
        return current

    def post(self, url: str, payload: dict, timeout: float = 10.0):
        if self.secure:
            station = canonical_station_name(payload.get("station_id") or payload.get("station_name") or "")
            transport = self.secure_transports.get(station)
            if transport is None:
                print(f"   [SECURE] no transport for {station}")
                return None
            if url.endswith("/edge-heartbeat"):
                # Heartbeats are best-effort and not queued; telemetry/events are durable.
                try:
                    if transport.offline_simulation_active:
                        print(f"   [SECURE] heartbeat deferred: network offline ({station})")
                        return {"status":"queued","transient":True}
                    import requests as _requests
                    seq = transport.outbox.next_sequence(transport.device_id)
                    from edge_security import build_envelope
                    hb = dict(payload)
                    hb["station_id"] = station
                    hb["device_id"] = transport.device_id
                    env = build_envelope(transport.device_id, station, seq, "heartbeat", hb, transport.master_secret)
                    r = _requests.post(transport.url, json=env, timeout=min(timeout,5.0))
                    if r.ok:
                        return r.json()
                    print(f"   [SECURE] heartbeat HTTP {r.status_code}: {r.text[:220]}")
                except Exception as exc:
                    print(f"   [SECURE] heartbeat failed: {exc}")
                return None
            kind = "event" if url.endswith("/edge-event") else "batch" if url.endswith("/edge-batch") else None
            if kind:
                reply = transport.send(kind, payload)
                if reply.get("status") == "queued":
                    print(f"   📦 [SECURE QUEUE] {station}: {kind} persisted locally")
                else:
                    print(f"   🔐 [SECURE TX] {station}: {kind} acknowledged")
                return reply
            return None
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            if response.ok:
                return response.json()
            print(f"   [HTTP {response.status_code}] {response.text[:180]}")
        except requests.RequestException as exc:
            print(f"   [NETWORK] {url} -> {exc}")
        return None

    def choose_scenario(self, cycle: int, station_index: int) -> str:
        """Return NORMAL, SOFT or HARD for this station/cycle."""
        if self.scenario == "NORMAL":
            return "NORMAL"
        if self.scenario == "SOFT":
            return "SOFT"
        if self.scenario == "HARD":
            return "HARD"

        # DEMO/MIXED: deterministic visible rotation, then probability-based operation.
        if self.demo:
            hard_idx = (cycle // 5) % len(STATIONS)
            soft_idx = (cycle // 2 * 3 + 2) % len(STATIONS)
            if cycle > 0 and cycle % 5 == 0 and station_index == hard_idx:
                return "HARD"
            if cycle > 0 and cycle % 2 == 0 and station_index == soft_idx:
                return "SOFT"
            return "NORMAL"

        roll = self.rng.random()
        if roll < self.hard_probability:
            return "HARD"
        if roll < self.hard_probability + self.soft_probability:
            return "SOFT"
        return "NORMAL"


    def register_secure_devices(self):
        if not self.secure:
            return
        print("\n[REGISTRY] Provisioning/validating secure demo device identities...")
        devices=[{"device_id":self.device_ids[station],"station_id":station,"source":"LIVE_SIMULATOR"} for station in STATIONS]
        last_exc=None
        # Primary path: one atomic request for all 10 devices. This removes
        # sequential SQLite writer churn and makes live startup deterministic.
        for attempt in range(1,5):
            try:
                r=requests.post(DEVICE_PROVISION_BATCH_URL, json={"devices":devices,"source":"LIVE_SIMULATOR"}, timeout=20)
                if r.status_code == 200:
                    body=r.json() if r.content else {}
                    returned=body.get("devices") or []
                    returned_by_station={d.get("station_id"):d for d in returned}
                    missing=[s for s in STATIONS if s not in returned_by_station or returned_by_station[s].get("status") != "ACTIVE"]
                    if missing:
                        raise RuntimeError(f"Batch enrollment incomplete: {missing}")
                    for station in STATIONS:
                        print(f"   ✅ {station}: {self.device_ids[station]} ACTIVE")
                    print("[REGISTRY] All 10 secure demo devices ACTIVE. Registry enforcement is ON.")
                    return
                last_exc=RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            except requests.RequestException as exc:
                last_exc=exc
            except Exception as exc:
                last_exc=exc
            if attempt < 4:
                wait=attempt*2
                print(f"   [REGISTRY] Batch attempt {attempt} failed: {last_exc}; retrying in {wait}s...")
                time.sleep(wait)
        # Compatibility fallback: enroll individually with retries. This path
        # keeps older API deployments usable, but it is no longer the default.
        print("   [REGISTRY] Falling back to per-device enrollment with retries...")
        failures=[]
        for station, device_id in ((s,self.device_ids[s]) for s in STATIONS):
            ok=False; last=None
            for attempt in range(1,5):
                try:
                    r=requests.post(DEVICE_PROVISION_URL, json={"device_id":device_id,"station_id":station,"source":"LIVE_SIMULATOR"}, timeout=15)
                    if r.status_code==200:
                        print(f"   ✅ {station}: {device_id} ACTIVE")
                        ok=True; break
                    last=f"HTTP {r.status_code}: {r.text[:200]}"
                except requests.RequestException as exc:
                    last=str(exc)
                if attempt<4:
                    time.sleep(attempt*1.5)
            if not ok:
                failures.append((station,last))
        if failures:
            detail="; ".join(f"{s}: {e}" for s,e in failures)
            raise RuntimeError(f"Device registry enrollment failed: {detail}")
        print("[REGISTRY] All 10 secure demo devices ACTIVE. Registry enforcement is ON.")

    def heartbeat_all(self):
        for station in STATIONS:
            s = self.stats[station]
            self.post(
                EDGE_HEARTBEAT_URL,
                {
                    "station_id": station,
                    "device": f"ESP32-DEMO-{station}",
                    "mode": "demo" if self.demo else "production",
                    "samples_processed": s["samples"],
                    "hard_blocked": s["hard"],
                    "buffered_readings": len(self.buffers[station]),
                    "batches_sent": s["batches"],
                },
                timeout=5.0,
            )

    def flush_station(self, station: str):
        buf = self.buffers[station]
        if not buf:
            return
        batch = list(buf)
        payload = {
            "station_id": station,
            "device": f"ESP32-DEMO-{station}",
            "window_seconds": self.batch_seconds,
            "generated_count": len(batch),
            "readings": batch,
        }
        reply = self.post(EDGE_BATCH_URL, payload)
        if reply and reply.get("status") in {"accepted", "queued", "duplicate"}:
            self.batches_sent += 1
            self.stats[station]["batches"] += 1
            buf.clear()
            self.stats[station]["buffered"] = 0
            if reply.get("status") == "queued":
                print(f"   📦 {station}: batch safely persisted to offline outbox ({len(batch)} readings)")
            else:
                print(f"   📤 {station}: batch sent ({len(batch)} trusted readings)")
        else:
            print(f"   📦 {station}: batch retained locally ({len(batch)} readings)")

    def run(self, max_cycles: int = 0):
        print(f"\n================ SKYGUARD LIVE NETWORK ================\nBUILD: {SKYGUARD_BUILD}")
        print(f"Stations: {len(STATIONS)}")
        print(f"Scenario: {self.scenario}")
        print(f"Sampling cycle: {self.cycle_seconds:g}s (all 10 stations)")
        print(f"Normal/soft transmission window: {self.batch_seconds:g}s {'(DEMO)' if self.demo else '(production target)'}")
        print(f"Hard anomaly type: {self.hard_type}")
        print(f"API gateway: {API_BASE}")
        print("=======================================================\n")
        if self.secure:
            self.register_secure_devices()
        self.heartbeat_all()
        if self.secure:
            for station, transport in self.secure_transports.items():
                transport.drain(limit=20)

        start = time.monotonic()
        last_flush = start
        network_restore_announced = False
        last_heartbeat = start
        cycle = 0

        try:
            while True:
                for i, station in enumerate(STATIONS):
                    mode = self.choose_scenario(cycle, i)
                    reading = self.normal_reading(station)
                    if mode == "SOFT":
                        reading = self.soft_anomaly(station)
                        self.soft_generated += 1
                        self.stats[station]["soft"] += 1
                    elif mode == "HARD":
                        reading = self.hard_anomaly(self.hard_type, station, reading)

                    result = self.gates[station].accept_and_record(*reading)
                    self.total_samples += 1
                    self.stats[station]["samples"] += 1

                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    t, p, h = reading
                    tag = f" {mode}" if mode != "NORMAL" else ""
                    print(f"[{now}] {station:<18} {tag:<6} T={t!s:>6} P={p!s:>8} RH={h!s:>6}")

                    # In an explicit HARD simulation, guarantee a hard event for the
                    # demonstration. The real EdgeAI gate still runs above, but the
                    # demo must not depend on model thresholds to produce the event.
                    force_demo_hard = mode == "HARD" and self.demo
                    if result["hard_anomaly"] or force_demo_hard:
                        self.hard_blocked += 1
                        self.stats[station]["hard"] += 1
                        if force_demo_hard and not result["hard_anomaly"]:
                            edge_type = self.hard_type
                            edge_conf = 99.0
                            edge_reason = f"Demonstration hard anomaly: {self.hard_type} injected at Edge"
                            edge_model = "Edge AI (demo injection)"
                            edge_prob = 0.99
                            edge_features = {"simulation": True, "forced_hard_event": True}
                        else:
                            edge_type = result["type"]
                            edge_conf = result["confidence"]
                            edge_reason = result["reason"]
                            edge_model = result.get("model")
                            edge_prob = result.get("model_hard_probability")
                            edge_features = result.get("features") or {}
                        event = {
                            "station_id": station,
                            "timestamp": now,
                            "temperature": None if t != t else t,
                            "pressure": None if p != p else p,
                            "humidity": None if h != h else h,
                            "decision": "HARD_ANOMALY",
                            "severity": "CRITICAL" if edge_conf >= 90 else "HIGH",
                            "confidence": edge_conf,
                            "anomaly_type": edge_type,
                            "reason": edge_reason,
                            "blocked_locally": True,
                            "model": edge_model,
                            "model_hard_probability": edge_prob,
                            "features": edge_features,
                            "simulation_mode": mode,
                        }
                        print(f"   🚫 EDGE BLOCKED {station} | {edge_type} | {edge_conf:.1f}%")
                        reply = self.post(EDGE_EVENT_URL, event)
                        if self.secure and reply and reply.get("status") == "queued":
                            print(f"   📦 OFFLINE QUEUE {station} | hard anomaly safely stored")
                    else:
                        self.buffers[station].append(
                            {
                                "station_id": station,
                                "timestamp": now,
                                "temperature": t,
                                "pressure": p,
                                "humidity": h,
                                "edge_verdict": "PASS",
                                "simulation_mode": mode,
                            }
                        )
                        self.stats[station]["buffered"] = len(self.buffers[station])
                        print(f"   ✅ EDGE PASS {station} → buffer {len(self.buffers[station])}")

                cycle += 1

                if time.monotonic() - last_flush >= self.batch_seconds:
                    print("\n--- TRANSMISSION WINDOW ---")
                    for station in STATIONS:
                        self.flush_station(station)
                    print("----------------------------\n")
                    last_flush = time.monotonic()

                if self.secure:
                    # Announce the simulated network transition independently of queue state.
                    # This makes the demo state visible even if a queue is already empty.
                    network_offline_now = any(t.offline_simulation_active for t in self.secure_transports.values())
                    if self._network_was_offline and not network_offline_now and not network_restore_announced:
                        print("\n🟢 ================= NETWORK RESTORED =================")
                        print("   Secure edge connectivity is back. Replaying durable outbox messages...")
                        print("========================================================\n")
                        network_restore_announced = True
                    self._network_was_offline = network_offline_now
                    for station, transport in self.secure_transports.items():
                        if not transport.offline_simulation_active:
                            pending_before = transport.outbox.stats().get("pending", 0)
                            if pending_before:
                                print(f"   🟢 [NETWORK RESTORED] {station}: attempting secure store-and-forward sync ({pending_before} pending)...")
                        drained = transport.drain(limit=20)
                        if drained.get("sent") or drained.get("duplicate"):
                            print(f"   🔄 [STORE-FORWARD] {station}: synced {drained.get('sent', 0)} queued message(s)" + (f" ({drained.get('duplicate', 0)} duplicate ACK)" if drained.get('duplicate') else ""))
                        elif drained.get("attempted") and drained.get("pending"):
                            err = self.secure_transports[station].last_error
                            print(f"   ⚠️ [STORE-FORWARD RETRY] {station}: attempted {drained.get('attempted')} message(s), {drained.get('pending')} still pending")
                            if err:
                                print(f"      ↳ {err}")

                if time.monotonic() - last_heartbeat >= 8.0:
                    self.heartbeat_all()
                    last_heartbeat = time.monotonic()

                if max_cycles and cycle >= max_cycles:
                    break
                time.sleep(self.cycle_seconds)
        except KeyboardInterrupt:
            print("\n✅ Live network simulation stopped")

        print("\n================ SIMULATION SUMMARY ================")
        print(f"Cycles: {cycle}")
        print(f"Samples processed: {self.total_samples}")
        print(f"Soft anomalies generated: {self.soft_generated}")
        print(f"Hard anomalies blocked at edge: {self.hard_blocked}")
        print(f"Batches sent: {self.batches_sent}")
        print("Per-station:")
        for station in STATIONS:
            s = self.stats[station]
            print(f"  {station}: samples={s['samples']} soft={s['soft']} hard={s['hard']} batches={s['batches']}")
        print("=====================================================\n")


def parse_args():
    p = argparse.ArgumentParser(description="SkyGuard continuous 10-station Edge/Backend simulator")
    p.add_argument("--scenario", choices=["normal", "soft", "hard", "mixed"], default="mixed",
                   help="normal=all normal, soft=one soft event per station/cycle, hard=one hard event per station/cycle, mixed=normal+soft+hard")
    p.add_argument("--cycle-seconds", type=float, default=2.0,
                   help="Seconds between full 10-station sample cycles")
    p.add_argument("--batch-seconds", type=float, default=600.0,
                   help="Production normal-data batch window; use 300 or 600")
    p.add_argument("--demo", action="store_true",
                   help="Compress the transmission window to 10 seconds and use a deterministic mixed schedule")
    p.add_argument("--soft-probability", type=float, default=0.08)
    p.add_argument("--hard-probability", type=float, default=0.03)
    p.add_argument("--hard-type", choices=["SPIKE", "PHYSICS_BREACH", "INVALID"], default="SPIKE")
    p.add_argument("--secure", action="store_true", help="Use authenticated secure edge gateway and durable outbox")
    p.add_argument("--offline-seconds", type=float, default=0.0, help="Simulate network outage for this many seconds at startup (secure mode)")
    p.add_argument("--queue-dir", default=".edge_outbox", help="Persistent local edge outbox directory")
    p.add_argument("--fresh-outbox", action="store_true", help="Clear this demo's local outbox before starting (does not touch the dashboard DB)")
    p.add_argument("--max-cycles", type=int, default=0, help="Stop after N full 10-station cycles; 0 means run forever")
    return p.parse_args()


def main():
    args = parse_args()
    if args.fresh_outbox and args.secure:
        from pathlib import Path as _Path
        q = _Path(args.queue_dir)
        if q.exists():
            # Remove only disposable queue databases. Keep the persistent demo
            # device identity outside this directory so backend sequence numbers
            # cannot collide with old receipts after a fresh demo.
            # Preserve the per-device sequence metadata. Deleting the whole
            # SQLite file reset sequences to 1 while the backend registry kept
            # the previous last_sequence, causing restarted demo heartbeats to
            # be rejected as replay/stale messages. Clear only queued payloads.
            from edge_outbox import EdgeOutbox as _DemoOutbox
            for db in q.glob("*.sqlite3"):
                try:
                    _DemoOutbox(db).clear_pending_keep_sequences()
                except OSError:
                    pass
        print("🧹 Fresh secure demo outbox: cleared queued messages; device identity and sequence state preserved.")

    simulator = LiveNetworkSimulator(
        cycle_seconds=args.cycle_seconds,
        batch_seconds=args.batch_seconds,
        scenario=args.scenario,
        soft_probability=args.soft_probability,
        hard_probability=args.hard_probability,
        hard_type=args.hard_type,
        demo=args.demo,
        secure=args.secure,
        offline_seconds=args.offline_seconds,
        queue_dir=args.queue_dir,
    )
    simulator.run(max_cycles=args.max_cycles)


if __name__ == "__main__":
    main()
