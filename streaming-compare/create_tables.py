# create_tables.py
import psycopg2
from psycopg2 import sql
import time

DB_CONFIG = { "host":"localhost", "port":5433, "dbname":"streaming", "user":"postgres", "password":"qwertyuiop"}

DDL_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    user_id VARCHAR(36) NOT NULL,
    page_id VARCHAR(36) NOT NULL,
    ad_id VARCHAR(36) NOT NULL,
    ad_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL, -- view, click, purchase, heartbeat
    event_time_ns BIGINT NOT NULL,    -- nanosecond timestamp
    producer_timestamp_ns BIGINT NOT NULL,
    ip_address VARCHAR(64),
    PRIMARY KEY (ad_id, event_time_ns, user_id)
);
"""

DDL_MAPPINGS = """
CREATE TABLE IF NOT EXISTS mappings (
    ad_id VARCHAR(36) PRIMARY KEY,
    campaign_id INT NOT NULL
);
"""

if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("Creating mappings table...")
        cur.execute(DDL_MAPPINGS)
        print("Creating events table...")
        cur.execute(DDL_EVENTS)
        print("Creating indexes...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_time_ad ON events (event_time_ns, ad_id);")
        conn.commit()
        print("Tables and indexes created successfully.")
    except Exception as e:
        print("Database setup failed:", e)
    finally:
        if conn:
            conn.close()
