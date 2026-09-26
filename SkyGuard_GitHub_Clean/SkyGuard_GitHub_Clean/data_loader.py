"""
Data Loader: Load 10 AWS station CSVs into SQLite database
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

from config import DB_PATH, CSV_PATH

def init_database():
    """Create SQLite database with proper schema"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Drop existing table if it exists
    cursor.execute("DROP TABLE IF EXISTS sensor_readings")
    
    # Create sensor_readings table
    cursor.execute("""
        CREATE TABLE sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            temperature REAL,
            pressure REAL,
            humidity REAL,
            rainfall REAL,
            label INTEGER DEFAULT 0,
            is_anomaly INTEGER DEFAULT 0,
            anomaly_reason TEXT,
            confidence REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ Database initialized")

def load_csv_files():
    """Load combined 10 AWS stations CSV into database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    csv_file = CSV_PATH
    
    if not os.path.exists(csv_file):
        print(f"❌ File not found: {csv_file}")
        conn.close()
        return
    
    df = pd.read_csv(csv_file)
    # Normalize legacy DD-MM-YYYY dates to one ISO timestamp format.
    parsed_dates = pd.to_datetime(df['date'], format='mixed', dayfirst=True, errors='coerce')
    if parsed_dates.isna().any():
        bad = df.loc[parsed_dates.isna(), 'date'].head(5).tolist()
        raise ValueError(f"Invalid date values in CSV: {bad}")
    df['normalized_timestamp'] = parsed_dates.dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f"📥 Loading combined CSV ({len(df)} records)...")
    
    # Insert all records
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO sensor_readings 
            (station_name, timestamp, temperature, pressure, humidity, rainfall, label)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row['station'],
            row['normalized_timestamp'],
            row['temperature'],
            row['pressure'],
            row['humidity'],
            row['rainfall'],
            int(row.get('label', 0))
        ))
    
    conn.commit()
    
    cursor.execute("SELECT station_name, COUNT(*) FROM sensor_readings GROUP BY station_name")
    stations = cursor.fetchall()
    
    print(f"\n✅ Loaded {len(df)} total records:")
    for station, count in stations:
        print(f"   {station}: {count} records")
    
    conn.close()

def get_database_stats():
    """Print database statistics"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM sensor_readings")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT station_name) FROM sensor_readings")
    total_stations = cursor.fetchone()[0]
    
    cursor.execute("SELECT station_name FROM sensor_readings GROUP BY station_name")
    stations = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    print(f"\n📊 Database Statistics:")
    print(f"   Total Records: {total_records}")
    print(f"   Total Stations: {total_stations}")
    print(f"   Stations: {', '.join(stations)}")

if __name__ == "__main__":
    print("🚀 Initializing AWS Data Database...\n")
    init_database()
    load_csv_files()
    get_database_stats()
