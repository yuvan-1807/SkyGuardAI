"""Offline tests for the Phase 2B Edge AI gate."""
from edge_ai import EdgeAIHardAnomalyGate


def assert_normal(g, reading):
    r = g.accept_and_record(*reading)
    assert not r["hard_anomaly"], r


def test_range_fault():
    r = EdgeAIHardAnomalyGate().accept_and_record(120, 1012, 75)
    assert r["hard_anomaly"] and r["type"] == "RANGE_VIOLATION", r


def test_spike_fault():
    g = EdgeAIHardAnomalyGate()
    for i in range(5):
        assert_normal(g, (29.5 + i * 0.2, 1012.4 + i * 0.1, 75 + i * 0.3))
    r = g.accept_and_record(54.5, 1012.4, 99.0)
    assert r["hard_anomaly"] and r["type"] == "ABRUPT_SPIKE", r


def test_frozen_fault():
    g = EdgeAIHardAnomalyGate()
    results = [g.accept_and_record(31.1, 1011.45, 78.0) for _ in range(6)]
    assert any(r["type"] == "FROZEN_SENSOR" for r in results), results


def test_normal_variability():
    g = EdgeAIHardAnomalyGate()
    for reading in [
        (29.2, 1012.3, 73.0),
        (29.8, 1012.6, 77.0),
        (30.1, 1012.2, 74.0),
        (29.6, 1012.5, 76.0),
        (30.0, 1012.1, 75.0),
    ]:
        assert_normal(g, reading)


if __name__ == "__main__":
    test_range_fault(); test_spike_fault(); test_frozen_fault(); test_normal_variability()
    print("ALL EDGE AI TESTS PASSED")
