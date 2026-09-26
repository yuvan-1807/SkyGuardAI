"""Phase 2D self-test for security + durable outbox without requiring Flask."""
from pathlib import Path
import tempfile
import json

from edge_security import build_envelope, verify_envelope, model_integrity_ok
from edge_outbox import EdgeOutbox


def main():
    ok, actual, expected = model_integrity_ok(Path("edge_ai_model.json"))
    assert ok, f"model hash mismatch: {actual} != {expected}"
    secret = b"phase2d-test-secret-12345"
    env = build_envelope("TEST-DEVICE", "AWS_01_Delhi", 1, "event", {"reason":"test"}, secret)
    assert verify_envelope(env, secret)["message_id"] == env["message_id"]
    tampered = dict(env)
    tampered["payload"] = {"reason":"tampered"}
    try:
        verify_envelope(tampered, secret)
    except ValueError:
        pass
    else:
        raise AssertionError("tampering was not detected")
    with tempfile.TemporaryDirectory() as td:
        q = EdgeOutbox(Path(td)/"outbox.sqlite3")
        q.enqueue(env)
        assert q.stats()["pending"] == 1
        assert q.next_sequence("TEST-DEVICE") == 1
        q.ack(env["message_id"])
        assert q.stats()["pending"] == 0
    print("Phase 2D security/outbox self-test: PASS")
    print("Model integrity: PASS")
    print("HMAC authentication/tamper detection: PASS")
    print("Persistent queue + ACK deletion: PASS")


if __name__ == "__main__":
    main()
