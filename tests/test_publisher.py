import sqlite3
from pathlib import Path

import pytest

from incremental_platform.models import CanonicalEvent
from incremental_platform.publisher import (
    PublishingError,
    SqlitePublisher,
)
from incremental_platform.transforms import normalize_timestamp


def make_event(
    event_id: str,
    timestamp: str,
    *,
    record_hash: str | None = None,
) -> CanonicalEvent:
    return CanonicalEvent(
        entity_id="customer-A",
        event_id=event_id,
        event_type="update",
        event_timestamp=normalize_timestamp(timestamp),
        batch_id="001",
        payload={"status": event_id},
        record_hash=record_hash or event_id,
    )


def test_replaying_same_event_does_not_duplicate_history(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "platform.db"
    publisher = SqlitePublisher(db_path)

    event = make_event(
        "evt-001",
        "2026-10-27T10:00:00Z",
    )

    publisher.publish([event])
    publisher.publish([event])

    with sqlite3.connect(db_path) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM event_history"
        ).fetchone()

    assert count is not None
    assert count[0] == 1


def test_older_event_cannot_replace_newer_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "platform.db"
    publisher = SqlitePublisher(db_path)

    newer = make_event(
        "evt-002",
        "2026-10-27T11:00:00Z",
    )

    older = make_event(
        "evt-001",
        "2026-10-27T10:00:00Z",
    )

    publisher.publish([newer])
    publisher.publish([older])

    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT event_id
            FROM current_entities
            WHERE entity_id = ?
            """,
            ("customer-A",),
        ).fetchone()

    assert row is not None
    assert row[0] == "evt-002"


def test_conflicting_event_id_is_rejected(
    tmp_path: Path,
) -> None:
    publisher = SqlitePublisher(tmp_path / "platform.db")

    first = make_event(
        "evt-001",
        "2026-10-27T10:00:00Z",
        record_hash="hash-A",
    )

    conflicting = make_event(
        "evt-001",
        "2026-10-27T10:00:00Z",
        record_hash="hash-B",
    )

    publisher.publish([first])

    with pytest.raises(
        PublishingError,
        match="Conflicting event ID",
    ):
        publisher.publish([conflicting])

def test_conflict_rolls_back_entire_publish(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "platform.db"
    publisher = SqlitePublisher(db_path)

    original = make_event(
        "evt-001",
        "2026-10-27T10:00:00Z",
        record_hash="original",
    )

    publisher.publish([original])

    new_event = make_event(
        "evt-002",
        "2026-10-27T11:00:00Z",
    )

    conflicting = make_event(
        "evt-001",
        "2026-10-27T10:00:00Z",
        record_hash="changed",
    )

    with pytest.raises(PublishingError):
        publisher.publish([new_event, conflicting])

    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT event_id
            FROM event_history
            ORDER BY event_id
            """
        ).fetchall()

    assert rows == [("evt-001",)]