from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class ProfileViewRepository:
    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path
        Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.sqlite_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS profile_view (
                    customer_id TEXT PRIMARY KEY,
                    risk_score REAL NOT NULL,
                    risk_level TEXT,
                    version INTEGER NOT NULL,
                    event_id TEXT NOT NULL,
                    source_timestamp TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    event_version INTEGER NOT NULL,
                    previous_version INTEGER,
                    decision TEXT NOT NULL,
                    processed_at TEXT NOT NULL
                );
                """
            )

    def get(self, customer_id: str) -> dict | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM profile_view WHERE customer_id = ?", (customer_id,)
            ).fetchone()
        return dict(row) if row else None

    def apply_event(self, event: dict) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        event_id = str(event["eventId"])
        customer_id = str(event["customerId"])
        version = int(event["version"])

        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            duplicate = connection.execute(
                "SELECT decision FROM processed_events WHERE event_id = ?", (event_id,)
            ).fetchone()
            current = connection.execute(
                "SELECT version FROM profile_view WHERE customer_id = ?", (customer_id,)
            ).fetchone()
            previous_version = int(current["version"]) if current else None

            if duplicate:
                connection.rollback()
                return {"decision": "DUPLICATE", "previousVersion": previous_version}

            decision = "APPLIED" if previous_version is None or version > previous_version else "OLD_VERSION"
            if decision == "APPLIED":
                connection.execute(
                    """
                    INSERT INTO profile_view (
                        customer_id, risk_score, risk_level, version, event_id,
                        source_timestamp, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(customer_id) DO UPDATE SET
                        risk_score = excluded.risk_score,
                        risk_level = excluded.risk_level,
                        version = excluded.version,
                        event_id = excluded.event_id,
                        source_timestamp = excluded.source_timestamp,
                        updated_at = excluded.updated_at
                    WHERE excluded.version > profile_view.version
                    """,
                    (
                        customer_id,
                        float(event["riskScore"]),
                        event.get("riskLevel"),
                        version,
                        event_id,
                        str(event["timestamp"]),
                        now,
                    ),
                )

            connection.execute(
                """
                INSERT INTO processed_events (
                    event_id, customer_id, event_version, previous_version,
                    decision, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_id, customer_id, version, previous_version, decision, now),
            )
            connection.commit()
            return {"decision": decision, "previousVersion": previous_version}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def stats(self) -> dict:
        with self.connect() as connection:
            profile_count = connection.execute("SELECT COUNT(*) FROM profile_view").fetchone()[0]
            rows = connection.execute(
                "SELECT decision, COUNT(*) AS total FROM processed_events GROUP BY decision"
            ).fetchall()
        decisions = {row["decision"]: row["total"] for row in rows}
        return {"profiles": profile_count, "decisions": decisions}
