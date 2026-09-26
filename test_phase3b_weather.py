from weather_context import WeatherContextEngine

engine = WeatherContextEngine(min_history=5)

history = [
    {"temperature": 30.0, "pressure": 1012.0, "humidity": 70.0},
    {"temperature": 30.2, "pressure": 1011.8, "humidity": 71.0},
    {"temperature": 30.4, "pressure": 1011.6, "humidity": 72.0},
    {"temperature": 30.3, "pressure": 1011.5, "humidity": 73.0},
    {"temperature": 30.5, "pressure": 1011.2, "humidity": 74.0},
]

# Coordinated atmospheric transition: temperature drops, RH rises, pressure drops.
weather = engine.analyze(27.5, 1007.8, 90.0, history=history)
assert weather["available"] is True
assert weather["weather_event_candidate"] is True, weather
assert weather["dew_point_c"] is not None
assert weather["changes"]["temperature_c"] < 0
assert weather["changes"]["humidity_percent"] > 0
assert weather["changes"]["pressure_hpa"] < 0

# Isolated temperature jump: not enough coordinated change to be a weather event.
spike = engine.analyze(47.0, 1011.0, 74.0, history=history)
assert spike["available"] is True
assert spike["weather_event_candidate"] is False, spike

# Invalid humidity must never become a weather-event candidate.
invalid = engine.analyze(30.0, 1012.0, 120.0, history=history)
assert invalid["weather_event_candidate"] is False
assert invalid["physical_validation"]["is_physically_valid"] is False

print("PHASE 3B WEATHER CONTEXT TESTS: PASS")
print("Weather candidate:", weather["weather_event_candidate"], weather["weather_transition_score"], weather["reason"])
print("Isolated spike candidate:", spike["weather_event_candidate"], spike["weather_transition_score"])
