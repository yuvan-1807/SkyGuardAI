# SkyGuard AI — Final Operational Dashboard

The dashboard is the final command-center UI for the consolidated prototype.

## UI

- Fixed navigation sidebar.
- Light-green operational palette.
- Large readable text across cards, tables, station inspector, alerts and system views.
- Extra-large SHAP explainability headings, notes and evidence rows.
- One interactive India AWS map with the 10 simulated stations.
- Hover status + click-to-inspect station detail.
- Station-aware live telemetry charts based on event-time history.
- Separate Live Anomalies and Offline → Synced visibility.
- Weather Events view with dew point / transition evidence.
- Sensor Health view with degradation indicators and maintenance advisory.
- Data Quality view with trusted training source, quarantine and lineage state.
- System Status view with authentication, rejection, rate limiting and pipeline state.
- Start/Stop Live Network control retained in the top bar.

## Backend integration

The UI reads the consolidated operational APIs directly. The dashboard does not require a separate Dash callback process.

## Run

Use `START_SKYGUARD.bat`.
