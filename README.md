# ⚡ Real-Time Streaming Pipeline — Kafka + PySpark + Delta Lake

A production-style streaming data pipeline that runs entirely on your MacBook Air using Docker. Simulates real-world event streaming with schema evolution handling, schema registry, Parquet/Delta sink, and a live monitoring dashboard.

Mirrors GCP Pub/Sub → Dataflow → BigQuery architectures using open-source equivalents.

---

## 🏗️ Architecture

```
[Event Producer]
  Python script simulating stock tick events
        │
        ▼ Kafka Topic: raw-events
[Schema Registry]          ← validates & enforces Avro schema
        │
        ▼ Kafka Topic: validated-events
[PySpark Structured Streaming]
  - Parses JSON events
  - Applies schema validation
  - Handles schema evolution (new fields, type changes)
  - Writes micro-batches to Delta Lake sink
        │
        ▼
[Delta Lake — Bronze/Silver layers]
  ./data/bronze/   ← raw validated events (Parquet)
  ./data/silver/   ← cleaned, deduplicated, enriched
        │
        ▼
[Streamlit Monitoring Dashboard]
  Live metrics: throughput, lag, schema alerts
```

---

## 🛠️ Tech Stack

| Component | Tool | GCP Equivalent |
|-----------|------|----------------|
| Message Broker | Apache Kafka (Docker) | Cloud Pub/Sub |
| Stream Processing | PySpark Structured Streaming | Cloud Dataflow |
| Storage Format | Delta Lake (Parquet) | BigQuery / GCS |
| Schema Registry | Confluent Schema Registry (Docker) | Pub/Sub schemas |
| Monitoring | Streamlit Dashboard | Cloud Monitoring |
| Orchestration | Python scripts | Cloud Composer |

---

## 📁 Project Structure

```
streaming-pipeline/
├── README.md
├── requirements.txt
├── docker/
│   └── docker-compose.yml       # Kafka + Zookeeper + Schema Registry
├── src/
│   ├── __init__.py
│   ├── producer.py              # Simulates stock tick event producer
│   ├── spark_consumer.py        # PySpark Structured Streaming consumer
│   ├── schema_registry.py       # Schema validation & evolution handler
│   └── delta_writer.py          # Delta Lake sink with bronze/silver layers
├── data/
│   ├── bronze/                  # Auto-created: raw event sink
│   └── silver/                  # Auto-created: cleaned event sink
├── dashboards/
│   └── monitor.py               # Streamlit live monitoring dashboard
├── tests/
│   ├── __init__.py
│   ├── test_producer.py
│   └── test_schema_registry.py
└── scripts/
    ├── start_pipeline.sh        # One-command pipeline startup
    └── reset.sh                 # Clean all data and restart
```

---

## 🚀 Setup & Installation

### Prerequisites
- macOS (Apple Silicon or Intel)
- Docker Desktop installed and running
- Python 3.10+
- Java 11+ (required for PySpark): `brew install openjdk@11`

### Step 1 — Install Java (PySpark dependency)
```bash
brew install openjdk@11

# Add to your shell profile (~/.zshrc or ~/.bash_profile):
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
export PATH="$JAVA_HOME/bin:$PATH"

# Reload shell
source ~/.zshrc
java -version   # Should show openjdk 11
```

### Step 2 — Start Kafka with Docker
```bash
cd docker/
docker-compose up -d

# Verify all containers are running
docker-compose ps
# You should see: zookeeper, kafka, schema-registry all "Up"

# Wait ~30 seconds for Kafka to be fully ready, then create topics
docker exec -it kafka kafka-topics.sh \
  --create --topic raw-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics.sh \
  --create --topic validated-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# Verify topics created
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 3 — Set Up Python Environment
```bash
cd ..   # back to project root
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 4 — Run the Full Pipeline

Open 3 terminal tabs:

**Tab 1 — Start the Spark Consumer (reads from Kafka, writes Delta)**
```bash
source venv/bin/activate
python src/spark_consumer.py
# Wait for "Awaiting first micro-batch..." message
```

**Tab 2 — Start the Event Producer (sends events to Kafka)**
```bash
source venv/bin/activate
python src/producer.py --events-per-second 10 --duration 300
# Sends 10 events/sec for 5 minutes
```

**Tab 3 — Launch the Monitoring Dashboard**
```bash
source venv/bin/activate
streamlit run dashboards/monitor.py
# Open http://localhost:8501
```

### Step 5 — Trigger Schema Evolution (Optional Demo)
```bash
# After pipeline is running, send events with a new field
python src/producer.py --schema-version v2 --events-per-second 5 --duration 60
# Watch the dashboard show schema evolution alerts
```

---

## 🧪 Running Tests
```bash
pytest tests/ -v
```

---

## 🔧 Resetting the Pipeline
```bash
bash scripts/reset.sh
# Stops Spark, clears Delta tables, resets Kafka offsets
```

---

## 📈 Key Engineering Concepts Demonstrated

- **Micro-batch streaming** with PySpark's `trigger(processingTime="10 seconds")`
- **Schema evolution** — detecting and handling new fields in incoming events without breaking the pipeline
- **Exactly-once semantics** — Kafka offset management + Delta Lake ACID transactions
- **Bronze/Silver lakehouse layers** — raw ingestion vs. cleaned, deduplicated data
- **Consumer lag monitoring** — tracking how far behind the consumer is from the producer
- **Fault tolerance** — checkpoint directories allow the Spark job to resume after restart

---

## 🌱 Potential Extensions
- Add a Gold layer with aggregated metrics (VWAP, moving averages)
- Replace local Delta Lake with GCS + BigQuery for GCP deployment
- Add Avro serialization via Confluent Schema Registry client
- Implement dead letter queue (DLQ) for malformed events
- Deploy producer as a Cloud Run job, consumer as Dataflow

---

## 👤 Author
**Aritra Ghorai** — Senior Data Engineer  
[LinkedIn](https://linkedin.com/in/YOUR_PROFILE) | [GitHub](https://github.com/YOUR_USERNAME)
