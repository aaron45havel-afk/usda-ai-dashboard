import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "usda_data.db")


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commodity_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity TEXT NOT NULL,
            price REAL,
            unit TEXT,
            market TEXT,
            report_date TEXT,
            source TEXT DEFAULT 'AMS',
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crop_statistics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity TEXT NOT NULL,
            state TEXT,
            year INTEGER,
            stat_category TEXT,
            value REAL,
            unit TEXT,
            source TEXT DEFAULT 'NASS',
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS snap_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT,
            year INTEGER,
            month INTEGER,
            participants INTEGER,
            benefits_millions REAL,
            avg_benefit_per_person REAL,
            source TEXT DEFAULT 'FNS',
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT,
            station TEXT,
            date TEXT,
            temp_avg REAL,
            precip REAL,
            drought_index REAL,
            source TEXT DEFAULT 'NOAA',
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_source TEXT,
            record_id INTEGER,
            anomaly_score REAL,
            anomaly_type TEXT,
            description TEXT,
            signals TEXT,
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT,
            status_code INTEGER,
            records_fetched INTEGER,
            error_message TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migration: add signals column if missing
    try:
        cursor.execute("SELECT signals FROM anomaly_scores LIMIT 1")
    except sqlite3.OperationalError:
        cursor.execute("ALTER TABLE anomaly_scores ADD COLUMN signals TEXT")

    conn.commit()
    conn.close()


def log_api_call(endpoint, status_code, records_fetched=0, error_message=None):
    conn = get_db()
    conn.execute(
        "INSERT INTO api_log (endpoint, status_code, records_fetched, error_message) VALUES (?, ?, ?, ?)",
        (endpoint, status_code, records_fetched, error_message),
    )
    conn.commit()
    conn.close()
