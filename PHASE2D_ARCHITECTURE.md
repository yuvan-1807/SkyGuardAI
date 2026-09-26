# Phase 2D Architecture

```text
10 AWS / ESP32 nodes
        |
        v
 Edge AI quality gate
        |
   +----+------------------+
   |                       |
 HARD                     PASS
   |                       |
 local block          local buffer
   |                       |
 secure event queue   periodic batch
   |                       |
   +----------+------------+
              v
     HMAC authenticated gateway
              |
       network available?
        /            \
      no              yes
      |                |
 persistent queue   verify signature
      |              + identity
      |              + replay/idempotency
      |              + rate limit
      |                    |
      |                    v
      |               accept/ACK
      |                    |
      +-------> oldest-first sync <----+
                       |
                 FastAPI backend
                       |
                    dashboard
```

## Why this improves SkyGuard

The edge is no longer network-dependent. The local detector continues to operate offline. Normal telemetry and hard-anomaly events are durable on-device until acknowledged by the backend. A captured or altered message cannot simply be accepted without the device-authenticated HMAC; duplicate sequence/message combinations are idempotently rejected.

The current Python queue is the SIH demonstrator for the intended ESP32 persistent queue. The reference firmware continues to show the same policy for embedded deployment.
