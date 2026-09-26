"""SkyGuard Phase 2B virtual ESP32 edge node.

This replaces the old demo-only streamer with an edge-quality gate that mirrors
the intended ESP32 behavior:

1. Sensor values are generated locally.
2. A compact decision-tree Edge AI model checks for hard anomalies locally.
3. Hard anomalies are BLOCKED from normal backend ingestion and only a small
   event is transmitted so the operator can still see the fault.
4. Normal observations are buffered on the virtual device.
5. The buffer is transmitted to the backend in a batch every 5/10 minutes in
   real deployment; use --demo for a 10-second batch window during recording.

The existing data_injector.py and /api/ingest route are intentionally untouched.
"""
from __future__ import annotations

import argparse
import random
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import requests

from config import API_URL, STATIONS, canonical_station_name
from edge_ai import EdgeAIHardAnomalyGate
from edge_transport import SecureEdgeTransport

API_BASE = API_URL.rsplit("/api/ingest", 1)[0] + "/api"
EDGE_EVENT_URL = f"{API_BASE}/edge-event"
EDGE_HEARTBEAT_URL = f"{API_BASE}/edge-heartbeat"
EDGE_BATCH_URL = f"{API_BASE}/edge-batch"


def parse_args():
    p = argparse.ArgumentParser(description="SkyGuard virtual ESP32 Edge AI emulator")
    p.add_argument("--station", default="AWS_03_Chennai", help="Canonical station name or alias")
    p.add_argument("--sample-seconds", type=float, default=2.0, help="Sensor sampling interval")
    p.add_argument("--batch-seconds", type=float, default=600.0, help="Normal-data transmission window")
    p.add_argument("--demo", action="store_true", help="Use a 10-second batch window for SIH demo recording")
    p.add_argument("--fault", choices=["NONE", "PHYSICS_BREACH", "FROZEN_SENSOR", "SPIKE", "INVALID"], default="NONE")
    p.add_argument("--fault-after", type=int, default=10, help="Inject the requested fault after N normal samples")
    p.add_argument("--max-samples", type=int, default=0, help="Stop after N samples; 0 means run forever")
    p.add_argument("--secure", action="store_true", help="Use authenticated secure gateway + durable outbox")
    p.add_argument("--offline-seconds", type=float, default=0.0, help="Simulate network outage in secure mode")
    p.add_argument("--queue-dir", default=".edge_outbox", help="Persistent local outbox directory")
    return p.parse_args()


def safe_post(url: str, payload: dict, timeout: float = 5.0, transport=None, kind=None):
    if transport is not None and kind is not None:
        result = transport.send(kind, payload)
        if result.get("status") == "queued":
            print(f"   📦 [SECURE QUEUE] {kind} persisted locally")
        return result
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.ok:
            return r.json()
        print(f"   [EDGE->BACKEND] HTTP {r.status_code}: {r.text[:200]}")
    except requests.RequestException as exc:
        print(f"   [EDGE->BACKEND] offline: {exc}")
    return None


def generate_normal(station: str, rng: random.Random):
    profiles = {
        "AWS_01_Delhi": (27.5, 55.0, 1011.5),
        "AWS_02_Mumbai": (29.0, 74.0, 1008.8),
        "AWS_03_Chennai": (29.5, 75.0, 1012.4),
        "AWS_04_Himachal": (18.0, 68.0, 895.0),
        "AWS_05_Kolkata": (30.0, 72.0, 1008.0),
        "AWS_06_Bangalore": (24.0, 65.0, 900.0),
        "AWS_07_Kerala": (28.0, 80.0, 1008.0),
        "AWS_08_Rajasthan": (34.0, 42.0, 1005.0),
        "AWS_09_NorthEast": (23.0, 78.0, 1000.0),
        "AWS_10_Gujarat": (31.0, 58.0, 1008.0),
    }
    t0, h0, p0 = profiles.get(station, profiles["AWS_03_Chennai"])
    return (
        round(t0 + rng.uniform(-2.0, 2.0), 2),
        round(p0 + rng.uniform(-0.8, 0.8), 2),
        round(max(0.0, min(100.0, h0 + rng.uniform(-5.0, 5.0))), 2),
    )


def apply_fault(temp, pressure, humidity, fault: str):
    if fault == "PHYSICS_BREACH":
        return 54.5, 1012.4, 99.0
    if fault == "FROZEN_SENSOR":
        return 31.1, 1011.45, 78.0
    if fault == "SPIKE":
        return 58.0, 1070.0, 15.0
    if fault == "INVALID":
        # The Python emulator can carry NaN internally; edge_ai catches this
        # before anything is serialized into an HTTP packet.
        return float("nan"), pressure, humidity
    return temp, pressure, humidity


def main():
    args = parse_args()
    station = canonical_station_name(args.station)
    batch_seconds = 10.0 if args.demo else max(1.0, args.batch_seconds)
    rng = random.Random(20260921)
    gate = EdgeAIHardAnomalyGate()
    device_id = "ESP32-DEMO"
    secure_transport = SecureEdgeTransport(API_BASE, station, device_id, queue_dir=args.queue_dir, offline_seconds=args.offline_seconds) if args.secure else None
    buffer = deque(maxlen=1000)
    samples = 0
    sent_batches = 0
    hard_blocked = 0
    fault = args.fault

    print("\n================ SKYGUARD EDGE AI ================")
    print(f"Virtual ESP32 node: {station}")
    print("Local model: Portable Decision Tree")
    print("Hard anomaly policy: BLOCK locally + send compact event")
    print(f"Normal transmit window: {batch_seconds:g}s {'(DEMO)' if args.demo else '(production target)'}")
    print(f"Backend gateway: {API_BASE}")
    print("==================================================\n")

    if secure_transport:
        if not secure_transport.offline_simulation_active:
            # Best-effort secure heartbeat (not queued).
            import requests as _requests
            from edge_security import build_envelope
            seq = secure_transport.outbox.next_sequence(device_id)
            env = build_envelope(device_id, station, seq, "heartbeat", {"station_id":station,"device":"ESP32-DEMO","mode":"demo" if args.demo else "production"}, secure_transport.master_secret)
            try: _requests.post(secure_transport.url, json=env, timeout=5.0)
            except Exception: pass
        else:
            print("   [SECURE] heartbeat deferred while network is offline")
    else:
        safe_post(EDGE_HEARTBEAT_URL, {"station_id": station, "device": "ESP32-DEMO", "mode": "demo" if args.demo else "production"})

    last_flush = time.monotonic()
    last_heartbeat = time.monotonic()
    last_injected_sample = None

    while True:
        temp, pressure, humidity = generate_normal(station, rng)
        if fault != "NONE" and samples >= args.fault_after:
            temp, pressure, humidity = apply_fault(temp, pressure, humidity, fault)
            if last_injected_sample != samples:
                print(f"\n⚠ Injecting EDGE fault: {fault}\n")
                last_injected_sample = samples

        result = gate.accept_and_record(temp, pressure, humidity)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        samples += 1
        if secure_transport:
            drained = secure_transport.drain(limit=10)
            if drained.get("sent"):
                print(f"   🔄 [STORE-FORWARD] synced {drained.get('sent')} queued message(s)")

        print(
            f"[{now}] Sample #{samples:04d} | T={temp!s:>6}°C | P={pressure!s:>7} hPa | RH={humidity!s:>6}%"
        )

        if result["hard_anomaly"]:
            hard_blocked += 1
            event = {
                "station_id": station,
                "timestamp": now,
                "temperature": None if temp != temp else temp,
                "pressure": None if pressure != pressure else pressure,
                "humidity": None if humidity != humidity else humidity,
                "decision": "HARD_ANOMALY",
                "severity": "CRITICAL" if result["confidence"] >= 90 else "HIGH",
                "confidence": result["confidence"],
                "anomaly_type": result["type"],
                "reason": result["reason"],
                "blocked_locally": True,
                "model": result.get("model"),
                "model_hard_probability": result.get("model_hard_probability"),
                "features": result.get("features") or {},
            }
            print(
                f"   🚫 [EDGE GATE] BLOCKED locally | {result['type']} | confidence {result['confidence']:.1f}%"
            )
            safe_post(EDGE_EVENT_URL, event, transport=secure_transport, kind="event" if secure_transport else None)
            if secure_transport:
                secure_transport.drain(limit=10)
        else:
            buffer.append(
                {
                    "station_id": station,
                    "timestamp": now,
                    "temperature": temp,
                    "pressure": pressure,
                    "humidity": humidity,
                    "edge_verdict": "PASS",
                }
            )
            print(f"   ✅ [EDGE GATE] PASS → buffered ({len(buffer)} readings)")

        if time.monotonic() - last_flush >= batch_seconds and buffer:
            batch = list(buffer)
            payload = {
                "station_id": station,
                "device": "ESP32-DEMO",
                "window_seconds": batch_seconds,
                "generated_count": len(batch),
                "readings": batch,
            }
            reply = safe_post(EDGE_BATCH_URL, payload, timeout=10.0, transport=secure_transport, kind="batch" if secure_transport else None)
            if reply and reply.get("status") in {"accepted", "queued", "duplicate"}:
                sent_batches += 1
                if reply.get("status") == "queued":
                    print(f"   📦 [EDGE OUTBOX] Batch persisted locally: {len(batch)} trusted readings")
                else:
                    print(f"   📤 [EDGE TX] Batch #{sent_batches} sent: {len(batch)} trusted readings")
                buffer.clear()
            else:
                print("   📦 [EDGE TX] Batch retained locally; backend unavailable/rejected")
            last_flush = time.monotonic()

        if time.monotonic() - last_heartbeat >= 15.0:
            safe_post(
                EDGE_HEARTBEAT_URL,
                {
                    "station_id": station,
                    "device": "ESP32-DEMO",
                    "mode": "demo" if args.demo else "production",
                    "samples_processed": gate.total_processed,
                    "hard_blocked": hard_blocked,
                    "buffered_readings": len(buffer),
                    "batches_sent": sent_batches,
                },
            )
            last_heartbeat = time.monotonic()

        if args.max_samples and samples >= args.max_samples:
            break
        time.sleep(max(0.05, args.sample_seconds))

    print("\nEdge emulator stopped.")
    print(f"Samples processed: {samples}")
    print(f"Hard anomalies blocked locally: {hard_blocked}")
    print(f"Batches sent: {sent_batches}")


if __name__ == "__main__":
    main()
