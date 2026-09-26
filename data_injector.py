"""
Data Injector: Real-time sensor data injection with anomalies
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import random
import time
import requests

from config import DB_PATH, API_URL, CSV_PATH, STATIONS, canonical_station_name

class DataInjector:
    def __init__(self, interval=5):
        self.interval = interval
        self.stations_data = {}
        self.load_baseline_data()
    
    def load_baseline_data(self):
        """Load baseline data for each station"""
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("SELECT DISTINCT station_name FROM sensor_readings")
            stations = [row[0] for row in cursor.fetchall()]
            
            for station in stations:
                cursor.execute("""
                    SELECT temperature, pressure, humidity 
                    FROM sensor_readings 
                    WHERE station_name = ?
                    ORDER BY timestamp
                """, (station,))
                
                data = cursor.fetchall()
                if data:
                    temps = [d[0] for d in data]
                    presses = [d[1] for d in data]
                    humids = [d[2] for d in data]
                    
                    self.stations_data[station] = {
                        'data': data,
                        'index': 0,
                        'temp_mean': np.mean(temps),
                        'temp_std': np.std(temps) or 1,
                        'pressure_mean': np.mean(presses),
                        'pressure_std': np.std(presses) or 1,
                        'humidity_mean': np.mean(humids),
                        'humidity_std': np.std(humids) or 1,
                    }
            
            conn.close()
            print(f"✅ Loaded baseline data for {len(stations)} stations from {DB_PATH}")
            if not stations:
                # Fall back to the bundled historical CSV so the injector never becomes
                # a silent no-op just because the local DB has not been bootstrapped.
                try:
                    df = pd.read_csv(CSV_PATH)
                    for station in sorted(df['station'].dropna().astype(str).unique()):
                        part = df[df['station'].astype(str) == station]
                        data = list(zip(part['temperature'].astype(float), part['pressure'].astype(float), part['humidity'].astype(float)))
                        if data:
                            temps=[x[0] for x in data]; presses=[x[1] for x in data]; humids=[x[2] for x in data]
                            self.stations_data[canonical_station_name(station)] = {
                                'data': data, 'index': 0,
                                'temp_mean': float(np.mean(temps)), 'temp_std': float(np.std(temps) or 1),
                                'pressure_mean': float(np.mean(presses)), 'pressure_std': float(np.std(presses) or 1),
                                'humidity_mean': float(np.mean(humids)), 'humidity_std': float(np.std(humids) or 1),
                            }
                    stations=list(self.stations_data.keys())
                    print(f"⚠️ DB empty; loaded injector baseline from CSV for {len(stations)} stations")
                except Exception as fallback_error:
                    print(f"⚠️ CSV fallback failed: {fallback_error}")
        except Exception as e:
            print(f"Error loading baseline: {e}")
    
    def generate_reading(self, station_name):
        """Generate realistic reading with occasional anomalies"""
        if station_name not in self.stations_data:
            return None
        
        station_info = self.stations_data[station_name]
        
        temp_base = station_info['temp_mean']
        pressure_base = station_info['pressure_mean']
        humidity_base = station_info['humidity_mean']
        
        temp = temp_base + np.random.normal(0, 1.5)
        pressure = pressure_base + np.random.normal(0, 0.5)
        humidity = np.clip(humidity_base + np.random.normal(0, 3), 10, 99)
        
        anomaly_reason = None
        confidence = 95.0
        is_anomaly = 0
        
        if random.random() < 0.1:
            anomaly_choice = random.choice(['spike', 'drift', 'stuck'])
            is_anomaly = 1
            
            if anomaly_choice == 'spike':
                temp = np.random.uniform(45, 60)
                anomaly_reason = f'Temperature spike: {temp:.1f}°C'
                confidence = 92.0
            elif anomaly_choice == 'drift':
                temp = temp_base + 5
                anomaly_reason = f'Calibration drift detected'
                confidence = 78.0
            elif anomaly_choice == 'stuck':
                temp = temp_base
                anomaly_reason = f'Frozen sensor at {temp:.1f}°C'
                confidence = 88.0
        
        return {
            'temperature': temp,
            'pressure': pressure,
            'humidity': humidity,
            'is_anomaly': is_anomaly,
            'anomaly_reason': anomaly_reason,
            'confidence': confidence
        }
    
    def inject_reading(self, station_name):
        """Generate one reading and send it through the canonical API."""
        reading = self.generate_reading(station_name)
        if not reading:
            return False

        payload = {
            "station_id": station_name,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            **reading,
        }

        try:
            response = requests.post(API_URL, json=payload, timeout=5)
            response.raise_for_status()
            result = response.json()
            status = result.get("node_verdict", "UNKNOWN")
            print(
                f"{status} | {station_name} | "
                f"Temp: {reading['temperature']:.1f}C | "
                f"Press: {reading['pressure']:.1f} | "
                f"Humid: {reading['humidity']:.1f}%"
            )
            if result.get("is_anomaly"):
                print(f"  → {result.get('anomaly_reason') or 'Anomaly detected by SkyGuard'}")
            return True
        except requests.RequestException as exc:
            print(f"❌ API ingestion failed: {exc}")
            print("   Start app_new.py first, then run data_injector.py.")
            return False

    def run(self):
        """Run continuous injection"""
        stations = list(self.stations_data.keys())
        if not stations:
            print("❌ No stations available for injection.")
            print(f"   Database: {DB_PATH}")
            return
        
        try:
            health = requests.get(API_URL.replace('/api/ingest', '/api/health'), timeout=5)
            health.raise_for_status()
            print(f"✅ API connected: {health.json().get('database', 'unknown database')}")
        except requests.RequestException as exc:
            print(f"❌ Cannot reach SkyGuard API at {API_URL}: {exc}")
            print("   Start app_new.py first.")
            return

        station_idx = 0
        print(f"\n🚀 Starting real-time data injection ({self.interval}s interval)...\n")
        
        try:
            while True:
                station = stations[station_idx % len(stations)]
                self.inject_reading(station)
                station_idx += 1
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("\n✅ Injection stopped")

if __name__ == "__main__":
    injector = DataInjector(interval=5)
    injector.run()
