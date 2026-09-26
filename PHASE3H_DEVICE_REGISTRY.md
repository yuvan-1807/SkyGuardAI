# Phase 3H — Device Registry + Live Secure Integration

The prototype registry binds each simulated AWS station to one independent device identity and tracks ACTIVE/REVOKED lifecycle. Secure messages are HMAC authenticated first; after signature verification, the registry authorizes the device/station pair. Revocation is therefore enforced without weakening message authentication.

The live simulator automatically provisions its ten persistent demo identities when run with `--secure`, then uses the same identities for secure heartbeats, hard events and store-and-forward batches.

Production target: replace the demo derived key with a hardware-protected per-device secret/derivation mechanism on ESP32-S3.
