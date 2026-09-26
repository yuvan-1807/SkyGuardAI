"""Phase 2C orchestration layer."""
from sensor_health_v2 import SensorHealthEngine
from self_healing_v3 import SelfHealingAdvisorV3


class Phase2CService:
    def __init__(self, db_path):
        self.health = SensorHealthEngine(db_path)
        self.healing = SelfHealingAdvisorV3(db_path)

    def network_health(self):
        return self.health.all_stations()

    def station_health(self, station):
        return self.health.station_health(station)

    def healing_preview(self, station, timestamp, values, parameters=None):
        return self.healing.preview(station, timestamp, values, parameters)
