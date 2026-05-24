#!/bin/bash
# reset.sh — Clean all pipeline data and restart fresh
set -e

echo "🛑 Stopping any running Spark jobs..."
pkill -f spark_consumer.py 2>/dev/null || true
pkill -f producer.py 2>/dev/null || true

echo "🗑️  Clearing Delta Lake data..."
rm -rf ./data/bronze ./data/silver ./data/dlq ./data/checkpoints ./data/schema_registry.db

echo "🔄 Resetting Kafka topic offsets (delete + recreate topics)..."
docker exec kafka kafka-topics.sh --delete --topic raw-events --bootstrap-server localhost:9092 2>/dev/null || true
docker exec kafka kafka-topics.sh --delete --topic validated-events --bootstrap-server localhost:9092 2>/dev/null || true
sleep 3
docker exec kafka kafka-topics.sh --create --topic raw-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics.sh --create --topic validated-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "✅ Reset complete. You can now restart the pipeline."
echo ""
echo "Start order:"
echo "  1. python src/spark_consumer.py"
echo "  2. python src/producer.py"
echo "  3. streamlit run dashboards/monitor.py"
