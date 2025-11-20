#!/usr/bin/env python3
# FINAL FIXED latency_analyzer.py
# Works with:
#   - Spark flat JSON (your new output)
#   - Flink flat JSON
#   - Old Spark {"results": [...] } JSON
#
# Computes:
#   measured_latency_s = (now_ms - latest_producer_ts_ms) / 1000

import argparse
import json
import time
import sys

try:
    from kafka import KafkaConsumer
except Exception as e:
    print("Please install kafka-python: pip install kafka-python", file=sys.stderr)
    raise


def now_ms():
    return int(time.time() * 1000)


def extract_latest_ts_ms(parsed):
    """
    Extract latest_producer_ts_ms from ANY format:
    
    1. NEW SPARK & NEW FLINK:
        {
          "window_start_ms": ...,
          "window_end_ms": ...,
          "views": ...,
          "clicks": ...,
          "ctr": ...,
          "latest_producer_ts_ms": 17000,
          "max_producer_ns": 123456789
        }

    2. OLD SPARK:
        {
          "window_start_ms": ...,
          "results": [
             {
               "latest_producer_ts_ms": ... OR "latest_producer_ns": ...
             }
          ]
        }
    """

    # --- CASE 1: Flat JSON directly carries latest_producer_ts_ms ---
    if isinstance(parsed, dict):
        if "latest_producer_ts_ms" in parsed:
            try:
                return int(parsed["latest_producer_ts_ms"])
            except:
                pass

        # If only nanoseconds exist
        if "max_producer_ns" in parsed:
            try:
                return int(parsed["max_producer_ns"]) // 1_000_000
            except:
                pass

        # --- CASE 2: old Spark schema uses results: [{...}] ---
        if "results" in parsed and isinstance(parsed["results"], list) and len(parsed["results"]) > 0:
            first = parsed["results"][0]

            if "latest_producer_ts_ms" in first:
                try:
                    return int(first["latest_producer_ts_ms"])
                except:
                    pass

            if "latest_producer_ns" in first:
                try:
                    return int(first["latest_producer_ns"]) // 1_000_000
                except:
                    pass

            if "latest_kafka_ts_ms" in first:
                try:
                    return int(first["latest_kafka_ts_ms"])
                except:
                    pass

    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output-file", "-o", required=True)
    p.add_argument("--timeout", "-t", type=int, default=60)
    p.add_argument("--bootstrap", default="localhost:29092")
    args = p.parse_args()

    topic = "ctr_results_topic"

    print(f"[LatencyAnalyzer] Connecting to Kafka at {args.bootstrap}")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[args.bootstrap],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        value_deserializer=lambda v: v.decode("utf-8") if v else None,
        group_id=None
    )

    start = time.time()
    deadline = start + args.timeout

    result = {
        "found": False,
        "measured_latency_s": None,
        "raw_message": None,
        "error": None
    }

    try:
        print("[LatencyAnalyzer] Waiting for CTR output...")
        while time.time() < deadline:
            try:
                for msg in consumer:
                    if msg is None:
                        break

                    val = msg.value
                    if not val:
                        continue

                    # parse JSON
                    try:
                        parsed = json.loads(val)
                    except:
                        try:
                            parsed = json.loads(json.loads(val))
                        except:
                            continue

                    lp_ms = extract_latest_ts_ms(parsed)
                    if lp_ms is None:
                        continue  # try next msg

                    # Compute latency
                    measured_latency_s = (now_ms() - lp_ms) / 1000.0
                    result["found"] = True
                    result["measured_latency_s"] = measured_latency_s
                    result["raw_message"] = parsed

                    print(f"[LatencyAnalyzer] FOUND! latency={measured_latency_s:.3f}s")
                    raise StopIteration

            except StopIteration:
                break
            except Exception as e:
                result["error"] = str(e)

    finally:
        try:
            consumer.close()
        except:
            pass

    # Write result JSON
    with open(args.output_file, "w") as f:
        json.dump(result, f, indent=2)

    if result["found"]:
        print(f"[LatencyAnalyzer] Final latency: {result['measured_latency_s']:.3f}s")
    else:
        print("[LatencyAnalyzer] No CTR message found before timeout.")


if __name__ == "__main__":
    main()
