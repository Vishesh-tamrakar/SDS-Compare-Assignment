#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window,
    sum as _sum, when, max as _max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

def main():
    spark = (
        SparkSession.builder
        .appName("spark_ctr_final")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    KAFKA = "kafka:9092"

    # ------------------------------------------------------------------
    # 🔹 Schema must match producer.py
    # ------------------------------------------------------------------
    schema = StructType([
        StructField("producer_id", StringType()),
        StructField("user_id", StringType()),
        StructField("page_id", StringType()),
        StructField("ad_id", StringType()),
        StructField("ad_type", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time_ms", LongType()),
        StructField("producer_timestamp_ns", LongType()),
        StructField("ip_address", StringType())
    ])

    # ------------------------------------------------------------------
    # 🔹 Read streaming data
    # ------------------------------------------------------------------
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "ad_events_topic")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("d")
    ).select(
        col("d.ad_id").alias("ad_id"),
        col("d.event_type").alias("event_type"),
        col("d.event_time_ms"),
        col("d.producer_timestamp_ns").alias("producer_ns")
    ).where(col("event_type").isin("view", "click"))

    # ------------------------------------------------------------------
    # 🔹 Convert MS → proper timestamp
    # ------------------------------------------------------------------
    parsed = parsed.withColumn(
        "event_ts",
        (col("event_time_ms") / 1000).cast(TimestampType())
    )

    # ------------------------------------------------------------------
    # 🔹 WIDER WATERMARK FOR DOCKER DESKTOP
    # ------------------------------------------------------------------
    parsed = parsed.withWatermark("event_ts", "20 seconds")

    # ------------------------------------------------------------------
    # 🔹 10-second window CTR
    # ------------------------------------------------------------------
    grouped = parsed.groupBy(
        window(parsed.event_ts, "10 seconds")
    ).agg(
        _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
        _sum(when(col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
        _max("producer_ns").alias("max_producer_ns")
    )

    # ------------------------------------------------------------------
    # 🔹 Prepare final output to Kafka
    # ------------------------------------------------------------------
    result = grouped.select(
        (col("window").start.cast("long") * 1000).alias("window_start_ms"),
        (col("window").end.cast("long") * 1000).alias("window_end_ms"),
        col("views"),
        col("clicks"),
        when(col("views") == 0, 0.0)
        .otherwise(col("clicks") / col("views")).alias("ctr"),
        (col("max_producer_ns") / 1_000_000).cast("long").alias("latest_producer_ts_ms"),
        col("max_producer_ns")
    )

    final = result.select(to_json(struct("*")).alias("value"))

    # ------------------------------------------------------------------
    # 🔹 Write to Kafka
    # ------------------------------------------------------------------
    query = (
        final.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", "ctr_results_topic")
        .option("checkpointLocation", "/opt/spark/checkpoints/ctr_final")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
