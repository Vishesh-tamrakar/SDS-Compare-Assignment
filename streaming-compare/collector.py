#!/usr/bin/env python3
"""
collector.py — FINAL WORKING VERSION (updated)

- Consumes from ctr_results_topic
- Handles Spark and Flink JSON formats
- Accepts flat JSON that contains window_start_ms/window_end_ms and
  either fields named "views"/"clicks" (preferred) OR nested "results" array
- Computes measured_latency_ms = now - latest_producer_ts_ms (if available)
- Writes CSV used by create_plots.py
"""

import argparse
import json
import time
import csv
import sys
from kafka import KafkaConsumer

TOPIC = "ctr_results_topic"

def now_ms():
    return int(time.time() * 1000)

def unwrap_json(val):
    """Parse a Kafka message value (string) into python object (dict/list) or None."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    s = val.strip()
    # Try direct JSON parse
    try:
        return json.loads(s)
    except Exception:
        # Maybe it's a quoted JSON string (double encoded)
        try:
            return json.loads(s.strip('"'))
        except Exception:
            return None

def extract_fields(msg):
    """
    Normalize message into a fields dict:
      {
        "window_start_ms": int,
        "window_end_ms": int,
        "views": int or None,
        "clicks": int or None,
        "ctr": float or None,
        "latest_kafka_ts_ms": int or None,
        "latest_producer_ts_ms": int or None,
        "max_producer_ns": int or None
      }

    Accepts:
      - Flat JSON (Spark final / Flink): keys like window_start_ms, views, clicks, ctr, latest_producer_ts_ms, max_producer_ns
      - Spark legacy: top-level has "window_start_ms" + "results": [ {...} ]
      - Flink sometimes emits arrays — but collector expects JSON (dict) normally.
    """
    if not isinstance(msg, dict):
        return None

    # Preferred flat schema (what our updated Spark / Flink produce)
    if "window_start_ms" in msg and "window_end_ms" in msg:
        # Try to read views/clicks as either top-level "views" or "cnt_views"
        window_start_ms = msg.get("window_start_ms")
        window_end_ms = msg.get("window_end_ms")

        # Try direct fields first
        views = msg.get("views")
        clicks = msg.get("clicks")
        ctr = msg.get("ctr")

        latest_kafka_ts_ms = msg.get("latest_kafka_ts_ms") or msg.get("latest_kafka_ts")
        latest_producer_ts_ms = msg.get("latest_producer_ts_ms") or msg.get("latest_producer_ns_ms") or msg.get("latest_producer_ns")
        max_producer_ns = msg.get("max_producer_ns")

        # If latest_producer_ts_ms is in ns (named latest_producer_ns), convert when possible
        if latest_producer_ts_ms is None and max_producer_ns is not None:
            # some producers give max_producer_ns only; convert to ms for latency
            try:
                latest_producer_ts_ms = int(max_producer_ns) // 1_000_000
            except Exception:
                latest_producer_ts_ms = None

        # If fields missing but there's a results array, attempt to extract from it
        if (views is None or clicks is None) and "results" in msg and isinstance(msg["results"], list) and len(msg["results"])>0:
            first = msg["results"][0]
            if isinstance(first, dict):
                views = views or first.get("views") or first.get("cnt_views")
                clicks = clicks or first.get("clicks") or first.get("cnt_clicks")
                ctr = ctr or first.get("ctr")
                latest_kafka_ts_ms = latest_kafka_ts_ms or first.get("latest_kafka_ts_ms")
                latest_producer_ts_ms = latest_producer_ts_ms or first.get("latest_producer_ts_ms") or first.get("latest_producer_ns")
                max_producer_ns = max_producer_ns or first.get("max_producer_ns")
                # convert if needed
                if latest_producer_ts_ms is not None and latest_producer_ts_ms > 1_000_000_000_000:
                    # likely in ms already but very large; leave
                    pass

        # Try casting types (best-effort)
        try:
            window_start_ms = int(window_start_ms) if window_start_ms is not None else None
        except Exception:
            window_start_ms = None
        try:
            window_end_ms = int(window_end_ms) if window_end_ms is not None else None
        except Exception:
            window_end_ms = None

        try:
            views = int(views) if views is not None else None
        except Exception:
            views = None
        try:
            clicks = int(clicks) if clicks is not None else None
        except Exception:
            clicks = None
        try:
            ctr = float(ctr) if ctr is not None else None
        except Exception:
            ctr = None
        try:
            latest_kafka_ts_ms = int(latest_kafka_ts_ms) if latest_kafka_ts_ms is not None else None
        except Exception:
            latest_kafka_ts_ms = None
        try:
            latest_producer_ts_ms = int(latest_producer_ts_ms) if latest_producer_ts_ms is not None else None
        except Exception:
            latest_producer_ts_ms = None
        try:
            max_producer_ns = int(max_producer_ns) if max_producer_ns is not None else None
        except Exception:
            max_producer_ns = None

        # Must at least have window start and either views or clicks or timestamps
        if window_start_ms is None or window_end_ms is None:
            return None

        return {
            "window_start_ms": window_start_ms,
            "window_end_ms": window_end_ms,
            "views": views,
            "clicks": clicks,
            "ctr": ctr,
            "latest_kafka_ts_ms": latest_kafka_ts_ms,
            "latest_producer_ts_ms": latest_producer_ts_ms,
            "max_producer_ns": max_producer_ns
        }

    # Not recognized
    return None


def run_consumer(output, bootstrap, timeout):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[bootstrap],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        value_deserializer=lambda v: v.decode("utf-8") if v else None,
        group_id=None
    )

    csvfile = open(output, "w", newline="", encoding="utf-8")
    writer = csv.writer(csvfile)
    writer.writerow([
        "window_start_ms","window_end_ms","views","clicks","ctr",
        "latest_kafka_ts_ms","latest_producer_ts_ms","max_producer_ns","measured_latency_ms"
    ])
    csvfile.flush()

    start = time.time()
    msgs = 0
    try:
        while time.time() - start < timeout:
            try:
                for msg in consumer:
                    if msg is None:
                        break
                    raw_val = msg.value
                    parsed = unwrap_json(raw_val)
                    if parsed is None:
                        # unparsable
                        continue
                    fields = extract_fields(parsed)
                    if fields is None:
                        # not in expected format
                        continue

                    lp = fields.get("latest_producer_ts_ms")
                    # If lp is missing but max_producer_ns exists, use it (convert ns→ms)
                    if lp is None and fields.get("max_producer_ns") is not None:
                        try:
                            lp = int(fields.get("max_producer_ns")) // 1_000_000
                        except Exception:
                            lp = None

                    latency_ms = (now_ms() - int(lp)) if (lp is not None) else None

                    writer.writerow([
                        fields.get("window_start_ms"),
                        fields.get("window_end_ms"),
                        fields.get("views"),
                        fields.get("clicks"),
                        fields.get("ctr"),
                        fields.get("latest_kafka_ts_ms"),
                        fields.get("latest_producer_ts_ms"),
                        fields.get("max_producer_ns"),
                        latency_ms
                    ])
                    csvfile.flush()
                    msgs += 1
                    # keep reading until timeout or consumer exhaustion
                # small sleep to avoid tight loop
            except Exception as e:
                print("[collector] transient error:", e, file=sys.stderr)
            time.sleep(0.1)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        csvfile.close()
        print(f"[collector] finished. rows written: {msgs}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--output", "-o", required=True)
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--timeout", type=int, default=600)
    args = p.parse_args()
    run_consumer(args.output, args.bootstrap, args.timeout)
