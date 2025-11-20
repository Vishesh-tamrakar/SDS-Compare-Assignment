#!/usr/bin/env python3
"""
FINAL WORKING benchmark_runner_adaptive.py (updated)

- Uses results/{engine}/ for outputs
- Robust partition readiness checks (works on Windows + Docker Desktop)
- Starts collector and runner in a way that create_plots.py will find CSVs
- Keeps CLI compatible with your previous usage
"""

from __future__ import annotations
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
import os

ROOT = Path(__file__).parent.resolve()
RESULTS_DIR = ROOT / "results"

KAFKA_CONTAINER = "kafka"
SPARK_MASTER = "spark-master"
SPARK_WORKER = "spark-worker"
FLINK_JOBMANAGER = "flink-jobmanager"

# kafka: from host perspective used for docker exec commands (kafka CLI) -> uses localhost:9092 in your compose
KAFKA_BOOTSTRAP_HOST = "localhost:9092"
IS_WINDOWS = sys.platform.startswith("win")

BASE_PROBE = 500
PROBE_DURATION = 60
SPARK_WARMUP = 35
ANALYZER_GRACE = 20
PRODUCER_STAGGER = 0.08
PARTITION_READY_ATTEMPTS = 30
PARTITION_READY_SLEEP = 0.5

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def run(cmd: str, capture=False, cwd=None):
    print("+", cmd)
    if capture:
        res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=cwd)
        return res.stdout
    else:
        return subprocess.run(cmd, shell=True, cwd=cwd)

# --------------------------
# Kafka helpers
# --------------------------
def safe_delete_topic(topic):
    # best-effort delete; ignore errors
    try:
        run(f"docker exec {KAFKA_CONTAINER} kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP_HOST} --delete --topic {topic}")
    except Exception:
        pass

def create_topic(topic, partitions):
    run(
        f"docker exec {KAFKA_CONTAINER} kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP_HOST} "
        f"--create --topic {topic} --partitions {partitions} --replication-factor 1"
    )

def describe_topic(topic):
    return run(f"docker exec {KAFKA_CONTAINER} kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP_HOST} --describe --topic {topic}", capture=True)

def wait_partitions(topic, partitions):
    """Return True when topic appears to have the requested partition count."""
    for attempt in range(PARTITION_READY_ATTEMPTS):
        desc = describe_topic(topic)
        if not desc:
            time.sleep(PARTITION_READY_SLEEP)
            continue
        # two heuristics: look for "PartitionCount: N" or count "Partition:" occurrences
        if f"PartitionCount: {partitions}" in desc:
            return True
        if desc.count("Partition:") >= partitions:
            return True
        time.sleep(PARTITION_READY_SLEEP)
    return False

def recreate_topics(p):
    print(f"[INFO] Recreating topics with partitions={p}")
    safe_delete_topic("ad_events_topic")
    safe_delete_topic("ctr_results_topic")
    time.sleep(0.5)
    create_topic("ad_events_topic", p)
    create_topic("ctr_results_topic", 1)
    ok = wait_partitions("ad_events_topic", p)
    if not ok:
        print("[WARN] Kafka partitions not confirmed after wait; continuing anyway.")
    else:
        print("[INFO] Kafka partitions ready.")

# --------------------------
# Spark / Flink launchers
# --------------------------
def spark_submit_cmd():
    return (
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--jars /opt/spark/app/spark-sql-kafka-0-10_2.12-3.5.3.jar,"
        "/opt/spark/app/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,"
        "/opt/spark/app/kafka-clients-3.5.2.jar,"
        "/opt/spark/app/postgresql.jar "
        "/opt/spark/app/structured_streaming_ctr.py"
    )

def launch_spark_detached():
    cmd = f'docker exec -d {SPARK_MASTER} bash -lc "{spark_submit_cmd()}"'
    run(cmd)
    time.sleep(2)

def kill_spark_job():
    run(f'docker exec {SPARK_MASTER} bash -lc "pkill -f structured_streaming_ctr.py || true"')
    run(f'docker exec {SPARK_WORKER} bash -lc "pkill -f structured_streaming_ctr.py || true"')
    time.sleep(0.5)

def launch_flink_job(parallelism: int):
    cmd = f'docker exec -d {FLINK_JOBMANAGER} bash -lc "/opt/flink/bin/flink run -py /opt/flink/app/low_watermark_aggregator.py -p {parallelism}"'
    run(cmd)
    time.sleep(2)

def kill_flink_jobs():
    try:
        out = run(f"docker exec {FLINK_JOBMANAGER} /opt/flink/bin/flink list", capture=True)
        for line in out.splitlines():
            parts = line.split()
            if len(parts) > 0 and "-" in parts[0]:
                jobid = parts[0]
                run(f"docker exec {FLINK_JOBMANAGER} /opt/flink/bin/flink cancel {jobid}")
    except Exception:
        pass
    time.sleep(0.5)

# --------------------------
# producers & utilities
# --------------------------
def launch_producers(num_producers: int, per_producer: int, duration: int, pattern: str = "mmpp"):
    procs = []
    for i in range(num_producers):
        if pattern == "mmpp":
            cmd = [
                sys.executable,
                str(ROOT / "producer.py"),
                str(i), "mmpp", str(per_producer), str(duration), "2", "8", "4.0"
            ]
        else:
            cmd = [
                sys.executable,
                str(ROOT / "producer.py"),
                str(i), "spp", str(per_producer), str(duration)
            ]
        print("[LAUNCH-PRODUCER]", " ".join(cmd))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append(p)
        time.sleep(PRODUCER_STAGGER)
    return procs

def kill_producer_process(p: subprocess.Popen):
    if p.poll() is None:
        try:
            if IS_WINDOWS:
                run(f"taskkill /F /PID {p.pid} /T")
            else:
                p.terminate()
                p.wait(timeout=2)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass

def wait_for_producers(procs: list, timeout: int):
    start = time.time()
    for p in procs:
        remaining = max(0, timeout - (time.time() - start))
        try:
            p.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            kill_producer_process(p)

# --------------------------
# latency analyzer helpers
# --------------------------
def start_latency_analyzer(engine: str, results_dir: Path) -> subprocess.Popen:
    lat_file = results_dir / "latency_result.tmp"
    lat_log = results_dir / "latency_result.tmp.log"
    try:
        if lat_file.exists():
            lat_file.unlink()
    except Exception:
        pass
    logf = open(lat_log, "w", encoding="utf-8")
    cmd = [sys.executable, str(ROOT / "latency_analyzer.py"), "--output-file", str(lat_file), "--bootstrap", "localhost:29092"]
    print(f"[INFO] Starting latency_analyzer locally. Log -> {lat_log}")
    p = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT)
    time.sleep(1.5)
    return p

def stop_latency_analyzer(p: subprocess.Popen):
    if p is None:
        return
    try:
        p.terminate()
        p.wait(timeout=5)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass
    time.sleep(0.2)

def wait_for_latency(results_dir: Path, timeout: int = 60) -> float | None:
    lat_file = results_dir / "latency_result.tmp"
    start = time.time()
    while time.time() - start < timeout:
        if lat_file.exists():
            try:
                text = lat_file.read_text(errors="ignore").strip()
            except Exception:
                text = ""
            if text:
                try:
                    data = json.loads(text)
                    if data.get("found"):
                        return float(data.get("measured_latency_s"))
                except Exception:
                    # fallback: try to parse float lines
                    for ln in text.splitlines():
                        try:
                            return float(ln.strip())
                        except Exception:
                            continue
        time.sleep(1.0)
    return None

# --------------------------
# Probe logic
# --------------------------
def probe_once(engine: str, p: int, results_dir: Path) -> float | None:
    recreate_topics(p)
    analyzer_proc = start_latency_analyzer(engine, results_dir)

    if engine == "spark":
        launch_spark_detached()
        time.sleep(SPARK_WARMUP)
    elif engine == "flink":
        launch_flink_job(parallelism=p)
        time.sleep(SPARK_WARMUP)
    else:
        raise ValueError("unknown engine")

    procs = launch_producers(p, BASE_PROBE, PROBE_DURATION, pattern="mmpp")
    wait_for_producers(procs, timeout=PROBE_DURATION + 180)
    time.sleep(ANALYZER_GRACE)
    latency = wait_for_latency(results_dir, timeout=30)

    stop_latency_analyzer(analyzer_proc)
    if engine == "spark":
        kill_spark_job()
    elif engine == "flink":
        kill_flink_jobs()
    for p_ in procs:
        kill_producer_process(p_)
    return latency

def find_threshold_for_p(engine: str, p: int, results_dir: Path):
    per = BASE_PROBE
    latency = probe_once(engine, p, results_dir)
    if latency is None or latency >= 1.0:
        t_l = max(1, per // 2) * p
        t_h = per * p
    else:
        t_l = per * p
        t_h = per * p
    rep = (t_l + t_h) // 2
    print(f"[FOUND] p={p}: t_l={t_l}, t_h={t_h}, rep={rep}, latency={latency}")
    return t_l, t_h, rep

def find_all_thresholds(engine: str, results_dir: Path):
    ensure_dir(results_dir)
    results: dict = {"engine": engine, "partitions": [1, 2, 4], "timestamp": int(time.time())}
    for p in (1, 2, 4):
        t_l, t_h, rep = find_threshold_for_p(engine, p, results_dir)
        results[f"p={p}"] = {"t_l": int(t_l), "t_h": int(t_h), "rep": int(rep)}
        time.sleep(2)
    out_file = results_dir / "thresholds.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[INFO] {out_file} written")
    return results

# --------------------------
# Visualization suite
# --------------------------
def start_collector(outfile: Path) -> subprocess.Popen:
    logf = open(ROOT / "collector.log", "w", encoding="utf-8")
    cmd = [sys.executable, str(ROOT / "collector.py"), "--output", str(outfile), "--bootstrap", "localhost:29092", "--timeout", "600"]
    print("[INFO] Starting collector:", " ".join(cmd))
    p = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT)
    time.sleep(1.0)
    return p

def stop_collector(p: subprocess.Popen):
    if p is None:
        return
    try:
        p.terminate()
        p.wait(timeout=5)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass
    time.sleep(0.2)

def run_visualization_experiment(engine: str, mode: str, p: int, per_producer: int, duration_s: int, out_csv: Path):
    recreate_topics(p)
    ensure_dir(out_csv.parent)
    collector = start_collector(out_csv)

    if engine == "spark":
        launch_spark_detached()
        print(f"[INFO] Spark warmup {SPARK_WARMUP}s")
        time.sleep(SPARK_WARMUP)
    else:
        launch_flink_job(parallelism=p)
        print(f"[INFO] Flink warmup {SPARK_WARMUP}s")
        time.sleep(SPARK_WARMUP)

    procs = launch_producers(p, per_producer, duration_s, pattern=mode)
    wait_for_producers(procs, timeout=duration_s + 180)

    time.sleep(ANALYZER_GRACE)

    for pr in procs:
        kill_producer_process(pr)
    if engine == "spark":
        kill_spark_job()
    else:
        kill_flink_jobs()

    stop_collector(collector)
    print(f"[INFO] Finished visualization run: {out_csv}")

def run_visualization_suite(engine: str, best_p: int, rep_throughput: int, duration_minutes: int = 5):
    per_producer = max(1, rep_throughput // best_p)
    duration_s = duration_minutes * 60

    engine_dir = RESULTS_DIR / engine
    ensure_dir(engine_dir)
    plots_dir = engine_dir / "plots"
    ensure_dir(plots_dir)

    spp_out = engine_dir / f"{engine}_spp_visualization.csv"
    mmpp_out = engine_dir / f"{engine}_mmpp_visualization.csv"

    print(f"[INFO] Running SPP for {engine} p={best_p} per_producer={per_producer} duration={duration_s}s")
    run_visualization_experiment(engine, "spp", best_p, per_producer, duration_s, spp_out)

    print(f"[INFO] Running MMPP for {engine} p={best_p} per_producer={per_producer} duration={duration_s}s")
    run_visualization_experiment(engine, "mmpp", best_p, per_producer, duration_s, mmpp_out)

    # After runs, call create_plots.py from the engine results dir so it finds CSVs
    try:
        print(f"[INFO] Creating plots in {engine_dir}")
        run(f"python {ROOT/'create_plots.py'}", cwd=str(engine_dir))
    except Exception as e:
        print("[WARN] create_plots.py failed:", e)

    print("[INFO] Visualization suite completed.")

# --------------------------
# Find best config
# --------------------------
def find_best_config(engine: str, results_dir: Path):
    f = results_dir / "thresholds.json"
    if not f.exists():
        print(f"[ERROR] thresholds.json not found for engine {engine} at {f}")
        return None
    data = json.loads(f.read_text())
    best = None
    for k, v in data.items():
        if not k.startswith('p='):
            continue
        rep = v.get('rep')
        if best is None or (rep and rep > best['rep']):
            best = {'p': k, 'rep': rep, 't_l': v.get('t_l'), 't_h': v.get('t_h')}
    print(f"[BEST-CONFIG] engine={engine} -> {best}")
    return best

# --------------------------
# CLI
# --------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["find-thresholds", "run-visualization-suite", "find-best-config"])
    parser.add_argument("--engine", choices=["spark", "flink"], default="spark")
    parser.add_argument("--best-p", type=int, default=4, help="Number of producers/partitions/consumers for visualization runs")
    parser.add_argument("--rep", type=int, default=1500, help="Representative total throughput (events/sec) for visualization runs")
    parser.add_argument("--duration-min", type=int, default=5, help="Duration for visualization runs (minutes)")
    args = parser.parse_args()

    engine_dir = RESULTS_DIR / args.engine
    ensure_dir(engine_dir)

    if args.mode == "find-thresholds":
        find_all_thresholds(args.engine, engine_dir)
    elif args.mode == "run-visualization-suite":
        run_visualization_suite(args.engine, args.best_p, args.rep, args.duration_min)
    elif args.mode == "find-best-config":
        find_best_config(args.engine, engine_dir)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
