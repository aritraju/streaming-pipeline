#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping streaming pipeline..."

pkill -f "src/spark_consumer.py" 2>/dev/null && echo "  Spark consumer stopped" || true
pkill -f "src/producer.py" 2>/dev/null && echo "  Producer stopped" || true
pkill -f "dashboards/monitor.py" 2>/dev/null && echo "  Dashboard stopped" || true
pkill -f "pyspark.daemon" 2>/dev/null || true
pkill -f "SparkSubmit" 2>/dev/null || true

docker compose -f docker/docker-compose.yml down && echo "  Kafka stack stopped"

echo "Done."
