# SkyGuard AI — Phase 3E Decision Fusion

Phase 3E converts independent evidence layers into the final three-level classification:

- `NORMAL`
- `GENUINE_WEATHER_EVENT`
- `SENSOR_ANOMALY`

## Evidence used

- Input/edge anomaly evidence
- Temporal evidence
- Multivariate evidence
- ExtraTrees residual evidence
- Spatial consistency evidence
- Security evidence
- Weather-context evidence
- Physical/frozen-sensor safety gates

## Important compatibility rule

`is_anomaly` remains `True` only for `SENSOR_ANOMALY`. A `GENUINE_WEATHER_EVENT` is an operational weather classification and does not get counted as a sensor fault.

## Decision behaviour

1. Hard sensor/integrity faults override weather interpretation.
2. Otherwise, weighted multi-layer sensor evidence is fused.
3. Weather evidence is fused from the weather transition score and contextual confidence.
4. Strong, physically valid weather evidence is retained as `GENUINE_WEATHER_EVENT` unless sensor evidence clearly dominates.
5. Ambiguous weather/sensor conflicts are marked `review_required=True` while keeping the three-level classification explicit.

The final decision object is available at:

```python
result["layers"]["decision_fusion"]
```
