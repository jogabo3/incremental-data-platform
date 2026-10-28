from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path

from incremental_platform.models import CanonicalEvent


class PublishingError(RuntimeError):
    """Raised when records cannot be published safely."""


class SqlitePublisher:
    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS event_history (
                    event_id TEXT PRIMARY KEY,
                    entity_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS current_entities (
                    entity_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL
                );
                """
            )

    def publish(
        self,
        events: Sequence[CanonicalEvent],
    ) -> None:
        try:
            with self._connect() as connection:
                for event in events:
                    values = (
                        event.event_id,
                        event.entity_id,
                        event.event_type,
                        event.event_timestamp.isoformat(),
                        event.batch_id,
                        json.dumps(event.payload, sort_keys=True),
                        event.record_hash,
                    )

                    existing = connection.execute(
                        """
                        SELECT record_hash
                        FROM event_history
                        WHERE event_id = ?
                        """,
                        (event.event_id,),
                    ).fetchone()

                    if existing is not None:
                        if existing[0] != event.record_hash:
                            raise PublishingError(
                                f"Conflicting event ID: {event.event_id}"
                            )

                        continue

                    connection.execute(
                        """
                        INSERT INTO event_history (
                            event_id, entity_id, event_type,
                            event_timestamp, batch_id,
                            payload_json, record_hash
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        values,
                    )

                    connection.execute(
                        """
                        INSERT INTO current_entities (
                            entity_id, event_id, event_type,
                            event_timestamp, batch_id,
                            payload_json, record_hash
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(entity_id) DO UPDATE SET
                            event_id = excluded.event_id,
                            event_type = excluded.event_type,
                            event_timestamp = excluded.event_timestamp,
                            batch_id = excluded.batch_id,
                            payload_json = excluded.payload_json,
                            record_hash = excluded.record_hash
                        WHERE
                            excluded.event_timestamp > current_entities.event_timestamp
                            OR (
                                excluded.event_timestamp = current_entities.event_timestamp
                                AND (
                                    excluded.batch_id,
                                    excluded.event_id,
                                    excluded.record_hash
                                ) > (
                                    current_entities.batch_id,
                                    current_entities.event_id,
                                    current_entities.record_hash
                                )
                            )
                        """,
                        (
                            event.entity_id,
                            event.event_id,
                            event.event_type,
                            event.event_timestamp.isoformat(),
                            event.batch_id,
                            json.dumps(event.payload, sort_keys=True),
                            event.record_hash,
                        ),
                    )
        except sqlite3.Error as exc:
            raise PublishingError(
                f"Database publishing failed: {exc}"
            ) from exc