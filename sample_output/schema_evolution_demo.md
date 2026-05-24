# Schema Evolution Demo — Kafka + Spark Streaming Pipeline

This file shows the schema registry behavior when the producer upgrades from v1 to v2.

---

## Schema v1 Event (Baseline)

```json
{
  "event_id": "EVT-00000001",
  "schema_version": "v1",
  "symbol": "AAPL",
  "price": 189.50,
  "volume": 1500,
  "bid": 189.48,
  "ask": 189.52,
  "exchange": "NASDAQ",
  "event_timestamp": "2024-01-15T10:00:00+00:00",
  "producer_id": "stock-feed-v1"
}
```

**Validation result:** ✅ Valid | Schema changes detected: none

---

## Schema v2 Event (Evolution — new fields added)

```json
{
  "event_id": "EVT-00000100",
  "schema_version": "v2",
  "symbol": "AAPL",
  "price": 189.50,
  "volume": 1500,
  "bid": 189.48,
  "ask": 189.52,
  "exchange": "NASDAQ",
  "event_timestamp": "2024-01-15T10:01:00+00:00",
  "producer_id": "stock-feed-v2",
  "analyst_rating": "BUY",
  "market_cap_billions": 2950.0
}
```

**Validation result:** ✅ Valid  
**Schema changes detected:**
```
NEW_FIELD: analyst_rating
NEW_FIELD: market_cap_billions
```

Evolution is logged to SQLite and the Spark consumer writes new fields to the bronze (raw) Delta Lake layer automatically — no pipeline downtime.

---

## Malformed Event (Dead Letter Queue)

```json
{
  "event_id": "EVT-00000099",
  "schema_version": "v1",
  "symbol": null,
  "price": "NaN"
}
```

**Validation result:** ❌ Invalid  
**Errors:**
```
Missing required field: 'symbol'
Field 'price' must be numeric, got: 'NaN'
```

Routed to `data/dlq/` (Delta Lake dead letter queue) for inspection.

---

## Pipeline Architecture

```
Faker-based Producer (stock ticks)
     │ JSON events
     ▼
Kafka Topic: raw-events  (Docker: confluentinc/cp-kafka:7.6.0)
     │
     ├─▶ Schema Registry (SQLite, local)
     │       • validates required fields
     │       • detects v1→v2 evolution
     │       • logs new fields
     │
     ▼
PySpark Structured Streaming (micro-batch: 10s)
     │
     ├─▶ Bronze Layer (Delta Lake) — raw, all fields, partitioned by exchange/schema_version
     │
     ├─▶ Silver Layer (Delta Lake) — validated, deduped, spread/mid_price derived cols
     │
     └─▶ DLQ (Delta Lake)         — null symbol/price events for investigation
```

## Running Locally

```bash
# 1. Start Kafka stack
docker-compose -f docker/docker-compose.yml up -d

# 2. Start Spark consumer (terminal 1)
python src/spark_consumer.py

# 3. Start v1 producer (terminal 2)
python src/producer.py --events-per-second 10 --duration 60

# 4. Upgrade to v2 schema (terminal 2)
python src/producer.py --schema-version v2 --events-per-second 10 --duration 60

# 5. Monitor (terminal 3)
streamlit run dashboards/monitor.py
```
