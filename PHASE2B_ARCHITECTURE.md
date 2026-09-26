# Phase 2B Architecture

```text
Temperature / Pressure / RH sensors
                 |
                 v
        +-------------------+
        | ESP32 Edge AI     |
        |                   |
        | Range safety      |
        | Spike detection   |
        | Frozen detection  |
        | Tiny ML tree      |
        +---------+---------+
                  |
          +-------+--------+
          |                |
       HARD FAULT       TRUSTED DATA
          |                |
          v                v
   compact event       local buffer
   (small packet)           |
                              +----> 5/10 min batch
                                         |
                                         v
                                   FastAPI /api/edge-batch
                                         |
                                         v
                              Existing /api/ingest path
                                         |
                                         v
                         Phase 2A soft-anomaly engine
                    Psychrometrics + temporal + multivariate
                           + ML + SHAP + quarantine
                                         |
                                         v
                                    Dashboard
```

## Why the split exists

**Edge AI** handles faults that can be identified cheaply and quickly from the current value and a small rolling window. This minimizes unnecessary radio transmissions and local processing cost.

**Backend AI/physics** handles minute or contextual anomalies that need historical baselines, multivariate context, psychrometric checks, and model explainability.

For the SIH demonstration, `edge_emulator.py` is the software stand-in for the ESP32 so the video can demonstrate the same data-flow and filtering policy without physical hardware.
