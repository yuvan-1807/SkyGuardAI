"""SkyGuard Edge Security primitives.

Provides device-key derivation, authenticated message envelopes, replay/idempotency
helpers and local Edge-AI model integrity verification. For the ESP32 prototype,
replace the demo master secret with a per-device provisioned secret.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from pathlib import Path
from typing import Any

DEFAULT_DEMO_MASTER_SECRET = "skyguard-demo-master-secret-change-me"
MODEL_SHA256 = "34ff202011e38b507823267c5715b8383619364fa19de31b6f594d6590793396"
MAX_ENVELOPE_BYTES = 256 * 1024
ALLOWED_KINDS = {"event", "batch", "heartbeat"}


def get_master_secret() -> bytes:
    value = os.getenv("SKYGUARD_EDGE_SECRET", DEFAULT_DEMO_MASTER_SECRET)
    if not value or len(value) < 16:
        raise ValueError("SKYGUARD_EDGE_SECRET must be at least 16 characters")
    return value.encode("utf-8")


def derive_device_key(device_id: str, master_secret: bytes | None = None) -> bytes:
    secret = master_secret or get_master_secret()
    return hmac.new(secret, ("skyguard-device:" + str(device_id)).encode("utf-8"), hashlib.sha256).digest()


def canonical_payload(envelope_without_signature: dict[str, Any]) -> bytes:
    return json.dumps(envelope_without_signature, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign_envelope(envelope_without_signature: dict[str, Any], device_id: str, master_secret: bytes | None = None) -> str:
    return hmac.new(derive_device_key(device_id, master_secret), canonical_payload(envelope_without_signature), hashlib.sha256).hexdigest()


def build_envelope(device_id: str, station_id: str, sequence: int, kind: str, payload: dict[str, Any], master_secret: bytes | None = None) -> dict[str, Any]:
    if kind not in ALLOWED_KINDS:
        raise ValueError(f"Unsupported edge message kind: {kind}")
    envelope = {
        "version": 1,
        "message_id": hashlib.sha256(f"{device_id}:{sequence}:{time.time_ns()}".encode()).hexdigest()[:32],
        "device_id": str(device_id),
        "station_id": str(station_id),
        "sequence": int(sequence),
        "sent_at": int(time.time()),
        "kind": kind,
        "payload": payload,
    }
    envelope["signature"] = sign_envelope(envelope, device_id, master_secret)
    return envelope


def verify_envelope(envelope: dict[str, Any], master_secret: bytes | None = None) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        raise ValueError("Envelope must be a JSON object")
    required = {"version", "message_id", "device_id", "station_id", "sequence", "sent_at", "kind", "payload", "signature"}
    missing = sorted(required - set(envelope))
    if missing:
        raise ValueError("Missing envelope fields: " + ", ".join(missing))
    if int(envelope["version"]) != 1:
        raise ValueError("Unsupported envelope version")
    if envelope["kind"] not in ALLOWED_KINDS:
        raise ValueError("Unsupported message kind")
    if not isinstance(envelope["payload"], dict):
        raise ValueError("payload must be an object")
    sequence = int(envelope["sequence"])
    if sequence < 1:
        raise ValueError("sequence must be positive")
    unsigned = dict(envelope)
    supplied = str(unsigned.pop("signature"))
    expected = sign_envelope(unsigned, str(envelope["device_id"]), master_secret)
    if not hmac.compare_digest(supplied, expected):
        raise ValueError("Invalid message signature")
    return unsigned


def model_integrity_ok(model_path: str | Path) -> tuple[bool, str, str]:
    p = Path(model_path)
    actual = hashlib.sha256(p.read_bytes()).hexdigest()
    return actual == MODEL_SHA256, actual, MODEL_SHA256


__all__ = ["DEFAULT_DEMO_MASTER_SECRET", "MODEL_SHA256", "MAX_ENVELOPE_BYTES", "ALLOWED_KINDS", "get_master_secret", "derive_device_key", "canonical_payload", "sign_envelope", "build_envelope", "verify_envelope", "model_integrity_ok"]
