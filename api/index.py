import os
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Vercel's deployed filesystem is read-only.
# Use a writable temporary SQLite database for each function instance.
source_db = os.path.join(BASE_DIR, "aws_data.db")
runtime_db = "/tmp/skyguard_ai.db"

if os.path.exists(source_db) and not os.path.exists(runtime_db):
    shutil.copy2(source_db, runtime_db)

os.environ["SKYGUARD_DB_PATH"] = runtime_db
os.environ["SKYGUARD_CSV_PATH"] = os.path.join(
    BASE_DIR, "10_AWS_stations_combined.csv"
)

from app_new import app
