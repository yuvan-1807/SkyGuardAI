"""Authenticated, offline-first Edge transport used by the demo emulator.

Security and resilience are additive. Legacy edge endpoints are untouched.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import requests

from edge_outbox import EdgeOutbox
from edge_security import build_envelope, get_master_secret, model_integrity_ok, sign_envelope


class SecureEdgeTransport:
    def __init__(self, base_url: str, station_id: str, device_id: str, queue_dir: str | Path = ".edge_outbox",
                 offline_seconds: float = 0.0, timeout: float = 10.0, model_path: str | Path = "edge_ai_model.json"):
        self.url = base_url.rstrip("/") + "/edge-secure"
        self.station_id = station_id
        self.device_id = device_id
        self.timeout = timeout
        self.offline_until = time.monotonic() + max(0.0, float(offline_seconds))
        safe = "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in device_id)
        self.outbox = EdgeOutbox(Path(queue_dir) / f"{safe}.sqlite3")
        self.master_secret = get_master_secret()
        ok, actual, expected = model_integrity_ok(model_path)
        self.model_integrity = {"ok": ok, "actual": actual, "expected": expected}
        if not ok:
            raise RuntimeError("Edge AI model integrity check failed; refusing to start secure edge transport")
        self.last_error = ""

    @property
    def offline_simulation_active(self) -> bool:
        return time.monotonic() < self.offline_until

    def send(self, kind: str, payload: dict[str, Any], force_queue: bool = False) -> dict[str, Any]:
        seq = self.outbox.next_sequence(self.device_id)
        payload = dict(payload)
        # This metadata is signed into the envelope and survives store-and-forward.
        # It describes transport only; anomaly detection remains Edge AI / Soft Backend.
        payload["delivery_mode"] = "SECURE_OFFLINE_SYNC" if (force_queue or self.offline_simulation_active) else "SECURE_LIVE"
        env = build_envelope(self.device_id, self.station_id, seq, kind, payload, self.master_secret)
        self.outbox.enqueue(env)
        if force_queue or self.offline_simulation_active:
            self.last_error = "network unavailable (simulated)"
            return {"status": "queued", "message_id": env["message_id"], "sequence": seq, "queued": True}
        return self._attempt(env)

    def _attempt(self, env: dict[str, Any]) -> dict[str, Any]:
        try:
            response = requests.post(self.url, json=env, timeout=self.timeout)
        except requests.RequestException as exc:
            self.last_error = str(exc)
            self.outbox.fail(env["message_id"], str(exc), permanent=False)
            return {"status": "queued", "message_id": env["message_id"], "sequence": env["sequence"], "queued": True, "error": str(exc)}
        if response.ok:
            body = response.json() if response.content else {"status": "accepted"}
            self.outbox.ack(env["message_id"])
            return body
        permanent = response.status_code in {400, 401, 403, 404, 413, 422}
        err = f"HTTP {response.status_code}: {response.text[:220]}"
        self.outbox.fail(env["message_id"], err, permanent=permanent)
        self.last_error = err
        return {"status": "queued" if not permanent else "dead", "message_id": env["message_id"], "sequence": env["sequence"], "queued": not permanent, "error": err}

    def drain(self, limit: int = 20) -> dict[str, int]:
        sent = duplicate = attempted = 0
        if self.offline_simulation_active:
            return {"sent": 0, "duplicate": 0, "attempted": 0, **self.outbox.stats()}
        for row in self.outbox.pending(limit):
            attempted += 1
            payload = row[4]
            import json
            env = json.loads(payload)
            # Always canonicalize the queued envelope before replay.
            # The outbox row is authoritative for device_id/kind and this transport
            # instance is authoritative for station_id. This prevents older queue
            # formats (or malformed legacy envelopes) from reaching /api/edge-secure
            # without the required top-level identity fields.
            body = env.get("payload") if isinstance(env.get("payload"), dict) else {}
            device_id = str(row[2] or env.get("device_id") or self.device_id)
            station_id = str(self.station_id or body.get("station_id") or env.get("station_id") or "")
            sequence = int(env.get("sequence", 0) or 0)
            if sequence <= 0:
                # Legacy records without a usable sequence get a fresh sequence.
                sequence = self.outbox.next_sequence(device_id)
            body = dict(body)
            body.setdefault("station_id", station_id)
            body.setdefault("device_id", device_id)
            body.setdefault("delivery_mode", "SECURE_OFFLINE_SYNC")
            env = {
                "version": 1,
                "message_id": str(env.get("message_id") or row[1]),
                "device_id": device_id,
                "station_id": station_id,
                "sequence": sequence,
                "sent_at": env.get("sent_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "kind": str(env.get("kind") or row[3]),
                "payload": body,
            }
            env["signature"] = sign_envelope({k: v for k, v in env.items() if k != "signature"}, device_id, self.master_secret)
            result = self._attempt(env)
            if result.get("status") == "accepted":
                sent += 1
            elif result.get("status") == "duplicate":
                duplicate += 1
            elif result.get("status") == "dead":
                break
            else:
                break
        return {"sent": sent, "duplicate": duplicate, "attempted": attempted, **self.outbox.stats()}

    def queue_status(self) -> dict[str, Any]:
        s = self.outbox.stats()
        return {**s, "oldest_age_seconds": self.outbox.oldest_age_seconds(), "offline_simulation": self.offline_simulation_active, "last_error": self.last_error}
