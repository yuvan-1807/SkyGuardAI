# Phase 3I — Operational API Consolidation & Runtime Performance

This phase keeps the clean UI unchanged and makes the backend authoritative for the final integration.

## Fixes

1. Secure heartbeat envelopes are verified and registry-enforced.
2. Heartbeat receipts are recorded so authenticated security counts include heartbeat traffic.
3. `/api/final-dashboard` exposes real Phase 2C sensor health for every station.
4. `system_health_score` is the real network health score; `network_coverage_pct` separately reports live edge coverage.
5. Live station state can fall back to a recent received observation when a legacy heartbeat is unavailable.
6. `/api/station-view/<station>` is a lightweight snapshot endpoint for fast station selection.
7. `/api/station-analysis/<station>` performs the expensive multi-layer analysis separately and is cached briefly.
8. `/api/anomaly/<id>/summary` is a fast metadata endpoint for alert lists; deep SHAP explanation remains on the existing endpoint.
9. Operational caches are short-lived and invalidated when new telemetry/heartbeat data arrives.

## Intended UI contract

The final UI should use `station-view` for selection and only call `station-analysis` when a user requests the deeper evidence view. It should use the `health` object from `/api/final-dashboard` rather than hard-coded UI fallbacks.
