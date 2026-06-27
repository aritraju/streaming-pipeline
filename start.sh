#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── 1. Kafka stack ────────────────────────────────────────────────────────────
echo "[1/3] Starting Kafka stack via Docker..."
docker compose -f docker/docker-compose.yml up -d

echo "      Waiting for Kafka to be healthy..."
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; do
  sleep 3
done
echo "      Kafka ready."

# Ensure topics exist (idempotent)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic raw-events --partitions 3 --replication-factor 1 --if-not-exists >/dev/null 2>&1 || true
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create \
  --topic validated-events --partitions 3 --replication-factor 1 --if-not-exists >/dev/null 2>&1 || true

# ── 2. Spark consumer ─────────────────────────────────────────────────────────
echo "[2/3] Starting Spark structured streaming consumer..."
JAVA_HOME=/opt/homebrew/opt/openjdk@11 \
  uv run python src/spark_consumer.py > /tmp/spark_consumer.log 2>&1 &
SPARK_PID=$!
echo "      Spark consumer PID: $SPARK_PID (logs: /tmp/spark_consumer.log)"

echo "      Waiting for Spark to connect to Kafka..."
until grep -q "Awaiting first micro-batch" /tmp/spark_consumer.log 2>/dev/null; do sleep 3; done
echo "      Spark consumer ready."

# ── 3. Dashboard ──────────────────────────────────────────────────────────────
echo "[3/3] Starting monitoring dashboard on http://localhost:8503 ..."
uv run streamlit run dashboards/monitor.py \
  --server.port 8503 \
  --server.headless true > /tmp/dashboard.log 2>&1 &
DASH_PID=$!

until curl -sf http://localhost:8503/_stcore/health >/dev/null 2>&1; do sleep 2; done
echo "      Dashboard ready."

echo ""
echo "Pipeline is live:"
echo "  Dashboard  → http://localhost:8503"
echo "  Kafka UI   → http://localhost:8080"
echo ""
echo "To start sending events (v1 schema, 10/sec for 5 min):"
echo "  uv run python src/producer.py --events-per-second 10 --duration 300"
echo ""
echo "To demo schema evolution (v2 adds analyst_rating + market_cap):"
echo "  uv run python src/producer.py --schema-version v2 --events-per-second 10 --duration 120"
echo ""
echo "To stop everything: ./stop.sh"
