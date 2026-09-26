"""Shared SkyGuard configuration.

All components import paths from here so the injector, API, dashboard and
processing layers always use the same database.
"""
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.getenv("SKYGUARD_DB_PATH") or os.getenv("SKYGUARD_DB") or str(BASE_DIR / "aws_data.db")
CSV_PATH = os.getenv("SKYGUARD_CSV_PATH", str(BASE_DIR / "10_AWS_stations_combined.csv"))
API_URL = os.getenv("SKYGUARD_API_URL", "http://127.0.0.1:8050/api/ingest")

STATIONS = [
    'AWS_01_Delhi', 'AWS_02_Mumbai', 'AWS_03_Chennai', 'AWS_04_Himachal',
    'AWS_05_Kolkata', 'AWS_06_Bangalore', 'AWS_07_Kerala', 'AWS_08_Rajasthan',
    'AWS_09_NorthEast', 'AWS_10_Gujarat'
]

STATION_ALIASES = {
    "AWS-MAA": "AWS_03_Chennai",
    "AWS_CHENNAI": "AWS_03_Chennai",
    "AWS_03": "AWS_03_Chennai",
}


def canonical_station_name(station_id: str) -> str:
    """Convert edge/station aliases to the canonical database station name."""
    station_id = str(station_id).strip()
    return STATION_ALIASES.get(station_id, station_id)
