"""
schema_registry.py
Local schema validation and evolution handler.
Tracks schema versions, detects new fields, and logs schema changes.
Mirrors Confluent Schema Registry / GCP Pub/Sub schema behavior.
"""

import json
import logging
import sqlite3
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

logger = logging.getLogger(__name__)

# Expected baseline schema for stock tick events (v1)
BASELINE_SCHEMA_V1 = {
    "event_id": str,
    "schema_version": str,
    "symbol": str,
    "price": float,
    "volume": int,
    "bid": float,
    "ask": float,
    "exchange": str,
    "event_timestamp": str,
    "producer_id": str
}

REQUIRED_FIELDS = {"event_id", "symbol", "price", "event_timestamp"}


class SchemaRegistry:
    """
    Lightweight schema registry backed by SQLite.
    Tracks field additions/removals across schema versions.
    """

    def __init__(self, db_path: str = "./data/schema_registry.db"):
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()
        self._register_baseline()

    def _init_db(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version TEXT NOT NULL,
                fields TEXT NOT NULL,
                registered_at TEXT NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_evolution_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at TEXT NOT NULL,
                event_id TEXT,
                change_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                details TEXT
            )
        """)
        self._conn.commit()

    def _register_baseline(self):
        existing = self._conn.execute(
            "SELECT id FROM schema_versions WHERE version = 'v1'"
        ).fetchone()
        if not existing:
            self._conn.execute(
                "INSERT INTO schema_versions (version, fields, registered_at) VALUES (?, ?, ?)",
                ("v1", json.dumps(list(BASELINE_SCHEMA_V1.keys())), datetime.now(timezone.utc).isoformat())
            )
            self._conn.commit()
            logger.info("Registered baseline schema v1")

    def validate(self, event: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate an event against required fields and basic type checks.

        Returns:
            (is_valid, list_of_error_messages)
        """
        errors = []

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in event or event[field] is None:
                errors.append(f"Missing required field: '{field}'")

        # Type check known fields
        if "price" in event and event["price"] is not None:
            try:
                float(event["price"])
            except (TypeError, ValueError):
                errors.append(f"Field 'price' must be numeric, got: {event['price']!r}")

        if "volume" in event and event["volume"] is not None:
            try:
                int(event["volume"])
            except (TypeError, ValueError):
                errors.append(f"Field 'volume' must be integer, got: {event['volume']!r}")

        return len(errors) == 0, errors

    def detect_evolution(self, event: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Compare event fields against baseline schema.
        Returns a list of detected schema changes (new fields, removed fields).
        """
        latest = self._conn.execute(
            "SELECT fields FROM schema_versions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        known_fields = set(json.loads(latest[0])) if latest else set(BASELINE_SCHEMA_V1.keys())
        event_fields = set(event.keys())

        changes = []

        new_fields = event_fields - known_fields
        for field in new_fields:
            changes.append({"type": "NEW_FIELD", "field": field})

        removed_fields = known_fields - event_fields - {"event_id"}  # event_id always required
        for field in removed_fields:
            changes.append({"type": "REMOVED_FIELD", "field": field})

        return changes

    def log_evolution(self, event_id: str, changes: List[Dict[str, str]]):
        """Persist schema evolution events to the SQLite log."""
        for change in changes:
            self._conn.execute(
                """INSERT INTO schema_evolution_log
                   (detected_at, event_id, change_type, field_name, details)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    event_id,
                    change["type"],
                    change["field"],
                    json.dumps(change)
                )
            )
        if changes:
            # Register new schema version
            version = f"v{self._conn.execute('SELECT COUNT(*) FROM schema_versions').fetchone()[0] + 1}"
            all_fields = list(set(BASELINE_SCHEMA_V1.keys()) | {c["field"] for c in changes if c["type"] == "NEW_FIELD"})
            self._conn.execute(
                "INSERT INTO schema_versions (version, fields, registered_at) VALUES (?, ?, ?)",
                (version, json.dumps(all_fields), datetime.now(timezone.utc).isoformat())
            )
        self._conn.commit()

    def get_evolution_log(self, limit: int = 50) -> List[Dict]:
        rows = self._conn.execute(
            "SELECT detected_at, event_id, change_type, field_name FROM schema_evolution_log ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [
            {"detected_at": r[0], "event_id": r[1], "change_type": r[2], "field_name": r[3]}
            for r in rows
        ]

    def get_versions(self) -> List[Dict]:
        rows = self._conn.execute(
            "SELECT version, fields, registered_at FROM schema_versions ORDER BY id"
        ).fetchall()
        return [{"version": r[0], "fields": json.loads(r[1]), "registered_at": r[2]} for r in rows]
