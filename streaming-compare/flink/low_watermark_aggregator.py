#!/usr/bin/env python3
from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment
)

# ---------------------------------------------------------------------------------
# FINAL WORKING FLINK FILE
# Uses 20s watermark, matches Spark exactly, correct output schema for collector.
# ---------------------------------------------------------------------------------

KAFKA_BROKER = "kafka:9092"
SOURCE_TOPIC = "ad_events_topic"
SINK_TOPIC   = "ctr_results_topic"

LOW_WATERMARK_INTERVAL = "'20' SECOND"   # VERY important for Docker Desktop latency

def main():

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # -------------------------------
    # KAFKA SOURCE
    # -------------------------------
    t_env.execute_sql(f"""
    CREATE TABLE ad_events (
        ad_id STRING,
        event_type STRING,
        event_time_ms BIGINT,
        producer_timestamp_ns BIGINT,
        event_ts AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
        WATERMARK FOR event_ts AS event_ts - INTERVAL {LOW_WATERMARK_INTERVAL}
    ) WITH (
        'connector'='kafka',
        'topic'='{SOURCE_TOPIC}',
        'properties.bootstrap.servers'='{KAFKA_BROKER}',
        'format'='json',
        'json.ignore-parse-errors'='true',
        'scan.startup.mode'='earliest-offset'
    )
    """)

    # -------------------------------
    # DERIVED campaign_id
    # -------------------------------
    t_env.execute_sql(r"""
    CREATE TEMPORARY VIEW enriched AS
    SELECT
        CAST(( 
            COALESCE(ASCII(SUBSTRING(ad_id,1,1)),0) +
            COALESCE(ASCII(SUBSTRING(ad_id,2,1)),0) +
            COALESCE(ASCII(SUBSTRING(ad_id,3,1)),0) +
            COALESCE(ASCII(SUBSTRING(ad_id,4,1)),0) +
            COALESCE(ASCII(SUBSTRING(ad_id,5,1)),0) +
            COALESCE(ASCII(SUBSTRING(ad_id,6,1)),0)
        ) % 10 AS INT) AS campaign_id,
        ad_id,
        event_type,
        event_ts,
        event_time_ms,
        producer_timestamp_ns
    FROM ad_events
    WHERE ad_id IS NOT NULL AND event_ts IS NOT NULL
    """)

    # -------------------------------
    # 10-second tumbling windows
    # -------------------------------
    t_env.execute_sql(r"""
    CREATE TEMPORARY VIEW agg AS
    SELECT
        TUMBLE_START(event_ts, INTERVAL '10' SECOND) AS window_start,
        TUMBLE_END(event_ts, INTERVAL '10' SECOND) AS window_end,
        campaign_id,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS cnt_views,
        SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS cnt_clicks,
        MAX(producer_timestamp_ns) AS max_producer_ns
    FROM enriched
    GROUP BY TUMBLE(event_ts, INTERVAL '10' SECOND), campaign_id
    """)

    # -------------------------------
    # KAFKA SINK
    # -------------------------------
    t_env.execute_sql(f"""
    CREATE TABLE ctr_results_sink (
        window_start_ms BIGINT,
        window_end_ms BIGINT,
        campaign_id STRING,
        cnt_views BIGINT,
        cnt_clicks BIGINT,
        ctr DOUBLE,
        latest_producer_ts_ms BIGINT,
        max_producer_ns BIGINT
    ) WITH (
        'connector'='kafka',
        'topic'='{SINK_TOPIC}',
        'properties.bootstrap.servers'='{KAFKA_BROKER}',
        'format'='json'
    )
    """)

    # -------------------------------
    # FINAL INSERT
    # -------------------------------
    t_env.execute_sql(r"""
    INSERT INTO ctr_results_sink
    SELECT
        UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 AS window_start_ms,
        UNIX_TIMESTAMP(CAST(window_end   AS STRING)) * 1000 AS window_end_ms,
        CAST(campaign_id AS STRING),
        cnt_views,
        cnt_clicks,
        CASE WHEN cnt_views > 0 THEN cnt_clicks * 1.0 / cnt_views ELSE 0 END AS ctr,
        (max_producer_ns / 1000000) AS latest_producer_ts_ms,
        max_producer_ns
    FROM agg
    """)

if __name__ == "__main__":
    main()
