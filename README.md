# Real-Time Streaming Pipeline — Kafka + PySpark + Delta Lake

A production-style streaming data pipeline that runs entirely on your MacBook Air using Docker. Simulates real-world event streaming with schema evolution handling, Parquet/Delta Lake sink, dead letter queue (DLQ), and a live monitoring dashboard.

Mirrors GCP Pub/Sub → Dataflow → BigQuery architectures using open-source equivalents.

---

## Architecture

```
[Event Producer]
  Python script simulating stock tick events (v1 + v2 schema)
        │
        ▼ Kafka Topic: raw-events (3 partitions)
[PySpark Structured Streaming]
  - Parses JSON events
  - Routes invalid events to DLQ
  - Handles schema evolution (new fields detected automatically)
  - Writes micro-batches every 10 seconds
        │
        ├──▶ data/bronze/   ← raw events (partitioned by exchange + schema_version)
        ├──▶ data/silver/   ← valid, enriched events (spread, mid_price added)
        └──▶ data/dlq/      ← malformed events (null symbol, bad price type)
        │
        ▼
[Streamlit Monitoring Dashboard]
  Live metrics: Bronze/Silver counts, DLQ rate, price chart, schema evolution log
```

---

## Tech Stack

| Component | Tool | GCP Equivalent |
|-----------|------|----------------|
| Message Broker | Apache Kafka (Docker) | Cloud Pub/Sub |
| Stream Processing | PySpark Structured Streaming | Cloud Dataflow |
| Storage Format | Delta Lake (Parquet) | BigQuery / GCS |
| Schema Registry | Confluent Schema Registry (Docker) | Pub/Sub schemas |
| Monitoring | Streamlit Dashboard | Cloud Monitoring |
| Kafka Client | kafka-python-ng | — |
| Package Manager | uv | — |

---

## Project Structure

```
streaming-pipeline/
├── README.md
├── pyproject.toml               # uv project config & dependencies
├── uv.lock                      # locked dependency versions
├── requirements.txt             # legacy reference (uv is used instead)
├── start.sh                     # one-command pipeline startup
├── stop.sh                      # stop all pipeline processes
├── docker/
│   └── docker-compose.yml       # Kafka + Zookeeper + Schema Registry + Kafka UI
├── src/
│   ├── __init__.py
│   ├── producer.py              # stock tick event producer (v1 + v2 schema)
│   ├── spark_consumer.py        # PySpark Structured Streaming consumer
│   └── schema_registry.py      # schema validation & evolution handler
├── data/
│   ├── bronze/                  # auto-created: raw event Delta sink
│   ├── silver/                  # auto-created: cleaned + enriched Delta sink
│   ├── dlq/                     # auto-created: dead letter queue
│   └── checkpoints/             # Spark streaming checkpoints
├── dashboards/
│   └── monitor.py               # Streamlit live monitoring dashboard
└── tests/
    ├── __init__.py
    ├── test_producer.py
    └── test_schema_registry.py
```

---

## Setup & Installation

### Prerequisites
- macOS (Apple Silicon or Intel)
- Docker Desktop installed and running
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Java 11 (required for PySpark): `brew install openjdk@11`

### Step 1 — Install Java
```bash
brew install openjdk@11

# Add to ~/.zshrc:
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
export PATH="$JAVA_HOME/bin:$PATH"

source ~/.zshrc
java -version   # should show openjdk 11
```

### Step 2 — Install dependencies
```bash
git clone https://github.com/YOUR_USERNAME/streaming-pipeline.git
cd streaming-pipeline
uv sync
```

### Step 3 — Start the full pipeline
```bash
./start.sh
```

This script automatically:
1. Starts the Kafka Docker stack (Zookeeper, Kafka, Schema Registry, Kafka UI)
2. Creates `raw-events` and `validated-events` topics
3. Starts the PySpark Structured Streaming consumer
4. Launches the Streamlit monitoring dashboard on http://localhost:8503

### Step 4 — Send events
```bash
# v1 schema — 10 events/sec for 5 minutes
uv run python src/producer.py --events-per-second 10 --duration 300
```

### Step 5 — Demo schema evolution (optional)
```bash
# v2 schema adds analyst_rating + market_cap_billions fields
uv run python src/producer.py --schema-version v2 --events-per-second 10 --duration 120
# Watch the dashboard show new fields appearing in silver layer
```

---

## Monitoring

| URL | What you see |
|-----|-------------|
| http://localhost:8503 | Streamlit dashboard — Bronze/Silver/DLQ counts, price chart, schema log |
| http://localhost:8080 | Kafka UI — topic lag, partition offsets, consumer groups |

---

## Running Tests
```bash
uv run pytest tests/ -v
```

---

## Stopping the Pipeline
```bash
./stop.sh
# Kills Spark consumer, producer, dashboard, and brings Docker stack down
```

---

## Key Engineering Concepts Demonstrated

- **Micro-batch streaming** with PySpark's `trigger(processingTime="10 seconds")`
- **Schema evolution** — detecting and handling new fields in incoming events without breaking the pipeline
- **Bronze/Silver/DLQ lakehouse layers** — raw ingestion, cleaned data, and rejected events as first-class citizens
- **Dead letter queue** — ~5% intentionally malformed events are routed to DLQ with a rejection reason
- **Fault tolerance** — checkpoint directories allow the Spark job to resume after restart
- **Derived enrichment** — Silver layer adds `spread` and `mid_price` columns computed from raw tick data

---

## Potential Extensions
- Add a Gold layer with aggregated metrics (VWAP, moving averages)
- Replace local Delta Lake with GCS + BigQuery for GCP deployment
- Add Avro serialization via Confluent Schema Registry client
- Deploy producer as a Cloud Run job, consumer as Dataflow

---

## Author
**Aritra Ghorai** — Senior Data Engineer  
[LinkedIn](https://linkedin.com/in/YOUR_PROFILE) | [GitHub](https://github.com/YOUR_USERNAME)
