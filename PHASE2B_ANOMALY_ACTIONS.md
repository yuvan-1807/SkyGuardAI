# Phase 2B — Operator Anomaly Actions

This branch adds operator case-management actions to the existing SkyGuard anomaly explanation UI.

## Actions
- **Mark as Acknowledged** → status `ACKNOWLEDGED`; the anomaly is no longer counted as active.
- **Mark for Inspection** → status `INSPECTION_REQUIRED`; it remains active and is included in the inspection queue.

## Important design boundary
These actions do not modify sensor readings, anomaly model outputs, Edge AI decisions, or the `/api/ingest` path. They are stored in a separate additive SQLite table named `anomaly_actions`.

## UI behavior
1. Click a backend soft anomaly or Edge hard anomaly.
2. Explanation modal shows the current case status.
3. Click **Mark as Acknowledged** or **Mark for Inspection**.
4. The status is saved.
5. The modal reloads and the live page refreshes within the same flow.
6. The anomaly feed displays the new status, active anomaly count updates, and inspection queue updates.
