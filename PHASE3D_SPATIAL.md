# SkyGuard Phase 3D — Spatial Consistency Evidence

Phase 3D adds a read-only spatial evidence layer to the canonical anomaly pipeline.

## What it does

For a station observation, SkyGuard:

1. Finds the nearest registered AWS stations using haversine distance.
2. Retrieves trusted neighbour observations close to the **event_time**.
3. Builds a same-month climatology baseline for the target and neighbours.
4. Transfers each neighbour's contextual residual to the target station.
5. Combines the adjusted neighbour estimates using inverse-distance weighting.
6. Computes observed-vs-spatial-expected residuals for temperature, pressure and humidity.
7. Produces a spatial anomaly candidate only when at least two trusted neighbours support a strong residual.

## Design rule

Spatial evidence is **advisory** in Phase 3D. It is exposed through:

```python
result["layers"]["spatial_evidence"]
```

The legacy `is_anomaly` verdict is not changed by spatial evidence yet. Phase 3E Decision Fusion will combine spatial evidence with Edge, temporal, multivariate, ML and thermodynamic evidence.

## Timestamp rule

Neighbour lookups use:

```text
event_time
```

with a configurable tolerance, not backend arrival order. This protects the spatial comparison during delayed/offline delivery.
