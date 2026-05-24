"""
producer.py
Simulates a real-time stock tick event producer.
Sends JSON events to a Kafka topic at a configurable rate.
Supports schema versioning to demonstrate schema evolution.

Usage:
    python src/producer.py                              # default: 5 events/sec, 60 sec
    python src/producer.py --events-per-second 20      # faster
    python src/producer.py --schema-version v2         # new field: analyst_rating
    python src/producer.py --duration 0                # run forever
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any

from kafka import KafkaProducer
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()

TOPIC = "raw-events"
BOOTSTRAP_SERVERS = "localhost:9092"

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX", "UBER", "LYFT"]
BASE_PRICES = {
    "AAPL": 189.0, "GOOGL": 175.0, "MSFT": 415.0, "AMZN": 185.0,
    "TSLA": 177.0, "META": 495.0, "NVDA": 875.0, "NFLX": 635.0,
    "UBER": 72.0, "LYFT": 16.0
}

# Track last prices for realistic price movement
last_prices = dict(BASE_PRICES)


def generate_event_v1(event_id: int) -> Dict[str, Any]:
    """Schema v1 — baseline event structure."""
    symbol = random.choice(SYMBOLS)
    price_change = random.uniform(-0.5, 0.5)
    last_prices[symbol] = max(0.01, last_prices[symbol] + price_change)
    price = round(last_prices[symbol], 2)

    return {
        "event_id": f"EVT-{event_id:08d}",
        "schema_version": "v1",
        "symbol": symbol,
        "price": price,
        "volume": random.randint(100, 50000),
        "bid": round(price - random.uniform(0.01, 0.05), 2),
        "ask": round(price + random.uniform(0.01, 0.05), 2),
        "exchange": random.choice(["NASDAQ", "NYSE"]),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "producer_id": "stock-feed-v1"
    }


def generate_event_v2(event_id: int) -> Dict[str, Any]:
    """Schema v2 — adds analyst_rating and market_cap fields (schema evolution demo)."""
    event = generate_event_v1(event_id)
    event["schema_version"] = "v2"
    event["analyst_rating"] = random.choice(["BUY", "HOLD", "SELL"])
    event["market_cap_billions"] = round(random.uniform(10, 3000), 1)
    event["producer_id"] = "stock-feed-v2"
    return event


def generate_malformed_event(event_id: int) -> Dict[str, Any]:
    """Occasionally inject a bad event to test error handling (5% rate)."""
    return {
        "event_id": f"EVT-{event_id:08d}",
        "schema_version": "v1",
        "symbol": None,           # null symbol — should fail validation
        "price": "not-a-number",  # wrong type
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1  # preserve order per partition
    )


def run(events_per_second: int, duration_sec: int, schema_version: str):
    logger.info(f"Starting producer: {events_per_second} events/sec, schema={schema_version}")
    producer = create_producer()
    interval = 1.0 / events_per_second
    event_id = 1
    start = time.time()
    sent = 0
    errors = 0

    def handle_interrupt(sig, frame):
        logger.info(f"\nProducer stopped. Sent: {sent}, Errors: {errors}")
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)

    try:
        while True:
            if duration_sec > 0 and (time.time() - start) > duration_sec:
                break

            # Inject 5% malformed events for error handling demo
            if random.random() < 0.05:
                event = generate_malformed_event(event_id)
            elif schema_version == "v2":
                event = generate_event_v2(event_id)
            else:
                event = generate_event_v1(event_id)

            producer.send(
                topic=TOPIC,
                key=event.get("symbol", "UNKNOWN"),
                value=event
            )
            sent += 1
            event_id += 1

            if sent % 100 == 0:
                logger.info(f"Sent {sent} events | Last symbol: {event.get('symbol')} | Price: {event.get('price')}")

            time.sleep(interval)

    except Exception as e:
        logger.error(f"Producer error: {e}")
        errors += 1
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer finished. Total sent: {sent}, errors: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka event producer — stock tick simulator")
    parser.add_argument("--events-per-second", type=int, default=5)
    parser.add_argument("--duration", type=int, default=60, help="Seconds to run (0=forever)")
    parser.add_argument("--schema-version", choices=["v1", "v2"], default="v1")
    args = parser.parse_args()

    run(args.events_per_second, args.duration, args.schema_version)
