# populate_mappings.py
import psycopg2
import uuid

NUM_CAMPAIGNS = 10
ADS_PER_CAMPAIGN = 100
DB_CONFIG = { "host":"localhost", "port":5433, "dbname":"streaming", "user":"postgres", "password":"qwertyuiop"}

if __name__ == "__main__":
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        for campaign_id in range(1, NUM_CAMPAIGNS + 1):
            for _ in range(ADS_PER_CAMPAIGN):
                ad = str(uuid.uuid4())
                cur.execute("INSERT INTO mappings (ad_id, campaign_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (ad, campaign_id))
        conn.commit()
        print("Mappings inserted.")
    except Exception as e:
        print("Populating mappings failed:", e)
    finally:
        if conn:
            conn.close()
