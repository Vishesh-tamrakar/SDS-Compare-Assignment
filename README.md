# Streaming Systems Comparison — Spark vs Flink  

This repository implements a full experiment pipeline for comparing **Apache Spark Structured Streaming** and **Apache Flink** for CTR aggregation under SPP and MMPP workloads.

Spark vs Flink streaming latency analysis with Kafka

## 🚀 1. System Architecture

```
Producers → Kafka → (Spark or Flink CTR Aggregator) → Kafka (ctr_results_topic) → Collector → CSV → Plots
```

Components:
- **producer.py** — sends  events to Kafka  
- **Spark job** — structured_streaming_ctr.py  
- **Flink job** — low_watermark_aggregator.py  
- **collector.py** — normalizes CTR output into CSV  
- **latency_analyzer.py** — computes end-to-end latency  
- **benchmark_runner_adaptive.py** — orchestrates full assignment workflow  
- **create_plots.py** — generates latency plots & scalability plot  

## ⏱ 2. Watermark Strategy Justification

A **20-second watermark** is used in both Spark and Flink to avoid:
- Late event drops  
- Docker Desktop → Windows clock drift  
- Network jitter  
- Kafka batching delays  

This ensures fairness when comparing Spark and Flink.

---

## ▶️ 3. Running the System

### 1. Start all services
```
docker compose up -d
```

### 2. Ensure Spark & Flink see workers
- Spark: http://localhost:8080  
- Flink: http://localhost:8181
  
serives :

| Service         | URL                                            |
| --------------- | ---------------------------------------------- |
| Spark Master    | [http://localhost:8080](http://localhost:8080) |
| Flink Dashboard | [http://localhost:8181](http://localhost:8181) |
| Kafka           | localhost:9092                                 |
| Postgres        | localhost:5432                                 |

### 2. Ensure tables are ready
```
python create_tables.py
python populate_tables.py
```
### 4. Run benchmark pipeline (thresholds → best-config → viz)

### **Step 1 — Find thresholds**
```
python benchmark_runner_adaptive.py find-thresholds --engine spark
python benchmark_runner_adaptive.py find-thresholds --engine flink
```

### **Step 2 — Best config**
```
python benchmark_runner_adaptive.py find-best-config --engine spark
python benchmark_runner_adaptive.py find-best-config --engine flink
```

### **Step 3 — Visualization suite**
```
python benchmark_runner_adaptive.py run-visualization-suite --engine spark --best-p 4 --rep 1500
python benchmark_runner_adaptive.py run-visualization-suite --engine flink --best-p 4 --rep 1500
```
### **Step 4 — Generate plots**
```
python create_plots.py
```

Outputs appear under:
```
results/spark/
results/flink/
```
## 📁 4. Collected Artifacts (to include in submission)

### Plots:
- `spark/plots/spark_spp_latency.png`
- `spark/plots/spark_mmpp_latency.png`
- `flink/plots/flink_spp_latency.png`
- `flink/plots/flink_mmpp_latency.png`
- `results/plots_scalability.png`

### CSVs:
- `spark_spp_visualization.csv`
- `spark_mmpp_visualization.csv`
- `flink_spp_visualization.csv`
- `flink_mmpp_visualization.csv`
- `scalability_results.csv`

### Logs:
- `collector.log`
- `latency_result.tmp.log`
- Flink JobManager logs
- Spark Master & Worker logs

### Screenshots:
- Spark UI: http://localhost:8080
- Flink UI: http://localhost:8181

---

## 🛠 Troubleshooting

### Spark job not visible?
```
docker exec -it spark-master bash
/opt/spark/bin/spark-submit ...
```

### Flink job not running?
```
docker exec -it flink-jobmanager bash
/opt/flink/bin/flink list
```

---

