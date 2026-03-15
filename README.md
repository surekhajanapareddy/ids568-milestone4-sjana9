# Milestone 4 – Distributed & Streaming Pipeline

## Overview

This project implements a distributed feature engineering pipeline (PySpark) and an optional streaming pipeline (mock queue) for the MLOps Course, Module 5.

---

## Prerequisites (Windows)

### 1. Install Java 11

PySpark requires Java. Download **Java 11 JDK** from:  
https://adoptium.net/temurin/releases/?version=11

- Choose **Windows x64 JDK `.msi`**, install it.
- After installing, set the environment variable:
  1. Search "Environment Variables" in Windows search bar
  2. Under "System variables" → click **New**
  3. Variable name: `JAVA_HOME`
  4. Variable value: `C:\Program Files\Eclipse Adoptium\jdk-11.x.x.x-hotspot` *(adjust to your actual path)*
  5. Find `Path` → Edit → New → add `%JAVA_HOME%\bin`

Verify: open a new Command Prompt and type:
```
java -version
```

### 2. Install WinUtils (required for PySpark on Windows)

Download `winutils.exe` for Hadoop 3.x from:  
https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin

- Create folder: `C:\hadoop\bin`
- Place `winutils.exe` inside it
- Set environment variable:
  - Variable name: `HADOOP_HOME`
  - Variable value: `C:\hadoop`
  - Add `%HADOOP_HOME%\bin` to `Path`

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## Project Structure

```
ids568-milestone4-[netid]/
├── pipeline.py          # Distributed feature engineering (PySpark)
├── generate_data.py     # Synthetic data generator (10 M+ rows)
├── producer.py          # Streaming event producer
├── consumer.py          # Streaming consumer with windowing
├── README.md            # This file
├── REPORT.md            # Performance analysis
├── STREAMING_REPORT.md  # Streaming analysis
└── requirements.txt     # Python dependencies
```

---

## Running the Pipeline (Required Part)

### Step 1 – Generate Synthetic Data (10 M rows)

```bash
python generate_data.py --rows 10000000 --output data/ --seed 42
```

For a quick correctness test first (1 000 rows):

```bash
python generate_data.py --rows 1000 --output test_data/ --seed 42
```

### Step 2 – Run the Pipeline

```bash
python pipeline.py --input data/ --output output/ --workers 4
```

This runs **two phases** back-to-back:
1. Local (1 worker) baseline
2. Distributed (4 workers) run

Metrics are saved to `output/metrics.json`.

### Step 3 – Verify Reproducibility

```bash
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
```

Both runs produce byte-identical Parquet files (seeded NumPy RNG).

---

## Running the Streaming Pipeline (Bonus)

Open **two terminal windows** side by side.

### Terminal 1 – Start Consumer first

```bash
python consumer.py --input queue/ --output results/ --window 10
```

### Terminal 2 – Start Producer

```bash
# Low load (100 evt/s)
python producer.py --rate 100 --duration 60 --output queue/

# Medium load (1 000 evt/s)
python producer.py --rate 1000 --duration 60 --output queue/

# High load (10 000 evt/s)
python producer.py --rate 10000 --duration 60 --output queue/
```

After each run, check `results/latency_report.json` for p50/p95/p99 numbers.

### Simulate Consumer Crash & Recovery

```bash
# Consumer crashes after 30 batches, then auto-restarts from checkpoint
python consumer.py --input queue/ --output results/ --crash-at 30
```

---

## Configuration

| Argument | Default | Description |
|---|---|---|
| `--rows` | 10 000 000 | Rows to generate |
| `--seed` | 42 | Random seed |
| `--workers` | 4 | Distributed worker count |
| `--rate` | 500 | Events per second (producer) |
| `--window` | 10 | Tumbling window size (s) |
| `--watermark` | 5.0 | Late-data tolerance (s) |
| `--crash-at` | None | Simulate crash after N batches |

---

## Reproducing Results

All results can be reproduced by running the commands above in order.  
The random seed is fixed at **42** throughout; no external data is used.

Expected runtime on a typical laptop (4-core, 16 GB RAM):

| Phase | ~Time |
|---|---|
| Data generation (10 M rows) | 2 – 4 min |
| Local pipeline | 8 – 15 min |
| Distributed pipeline (4 workers) | 3 – 6 min |

---

## Troubleshooting

**`java.io.IOException: Could not locate executable …\bin\winutils.exe`**  
→ Re-check your `HADOOP_HOME` environment variable and restart your terminal.

**`OutOfMemoryError`**  
→ Reduce `--rows` or increase `spark.driver.memory` in `pipeline.py`.

**Permission errors on Windows when writing Parquet**  
→ Run your terminal as Administrator, or change the output path to `C:\Users\<you>\Desktop\output`.
