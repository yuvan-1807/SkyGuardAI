# Phase 2B Live 10-Station Network Simulation

This is an additive demonstration/simulation layer built on top of the frozen Phase 2B anomaly/Edge AI implementation.

## Purpose

The simulator continuously generates readings for **all 10 configured AWS stations** in every sampling cycle.

Each reading follows the intended architecture:

1. Virtual sensor reading is generated locally.
2. The station's own Edge AI gate evaluates it.
3. **Hard anomaly:** blocked locally and only a compact `/api/edge-event` is transmitted.
4. **Normal or soft anomaly:** passes the edge gate, is buffered locally and transmitted later through `/api/edge-batch`.
5. `/api/edge-batch` routes trusted readings through the existing `/api/ingest` path, so backend soft-anomaly detection, psychrometrics, temporal analysis, multivariate analysis and SHAP remain active.

## SIH demo command

Start the dashboard first:

```powershell
python app_new.py
```

Then in another terminal:

```powershell
python live_network_simulator.py --demo --scenario mixed
```

The demo generates one reading for each of the 10 stations every 2 seconds and compresses the production 5/10-minute transmission window to 10 seconds.

The deterministic mixed schedule guarantees that both soft and hard events appear during a recording without requiring manual fault commands.

## Continuous production-style simulation

10-minute window:

```powershell
python live_network_simulator.py --scenario mixed --batch-seconds 600
```

5-minute window:

```powershell
python live_network_simulator.py --scenario mixed --batch-seconds 300
```

Normal only:

```powershell
python live_network_simulator.py --scenario normal --batch-seconds 600
```

Soft-only stress test:

```powershell
python live_network_simulator.py --scenario soft --demo
```

Hard-only stress test:

```powershell
python live_network_simulator.py --scenario hard --demo
```

## What the demo should show

### Normal / soft path

```text
Sensor -> Edge AI PASS -> Local Buffer -> Batch -> FastAPI -> Backend analysis -> Dashboard
```

### Hard path

```text
Sensor -> Edge AI BLOCK -> Compact Edge Event -> Dashboard
```

Hard telemetry is not posted to `/api/ingest`.

## Frozen integration guard

The simulator is a new file. It does not replace or modify:

- `data_injector.py`
- `edge_emulator.py`
- `edge_ai.py`
- `/api/ingest`
- `config.py`
- the existing database schema for `sensor_readings`

This keeps the proven Phase 2B data path intact.
