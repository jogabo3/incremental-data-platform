from datetime import UTC, datetime

import pytest

from incremental_platform.models import CanonicalEvent
from incremental_platform.transforms import (
    TransformationError,
    deduplicate_entities,
    generate_record_hash,
    normalize_timestamp,
    transform_batch,
)


def make_event(
    *,
    entity_id: str,
    event_id: str,
    timestamp: str,
    batch_id: str,
) -> CanonicalEvent:
    return CanonicalEvent(
        entity_id=entity_id,
        event_id=event_id,
        event_type="update",
        event_timestamp=normalize_timestamp(timestamp),
        batch_id=batch_id,
        payload={"status": "active"},
        record_hash=event_id,
    )


def test_hash_is_independent_of_key_order() -> None:
    first = {"event_id": "123", "status": "active"}
    second = {"status": "active", "event_id": "123"}

    assert generate_record_hash(first) == generate_record_hash(second)


def test_hash_changes_when_record_changes() -> None:
    first = {"event_id": "123", "status": "active"}
    second = {"event_id": "123", "status": "inactive"}

    assert generate_record_hash(first) != generate_record_hash(second)


def test_timestamp_normalizes_to_utc() -> None:
    result = normalize_timestamp("2026-10-27T10:00:00-05:00")

    assert result == datetime(2026, 10, 27, 15, 0, tzinfo=UTC)


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(
        TransformationError,
        match="timezone",
    ):
        normalize_timestamp("2026-10-27T10:00:00")


def test_newest_event_wins_regardless_of_input_order() -> None:
    older = make_event(
        entity_id="A",
        event_id="evt-001",
        timestamp="2026-10-27T10:00:00Z",
        batch_id="001",
    )

    newer = make_event(
        entity_id="A",
        event_id="evt-002",
        timestamp="2026-10-27T11:00:00Z",
        batch_id="002",
    )

    first = deduplicate_entities([older, newer])
    second = deduplicate_entities([newer, older])

    assert first[0].event_id == "evt-002"
    assert second[0].event_id == "evt-002"


def test_transform_batch_preserves_event_history() -> None:
    records = [
        {
            "entity_id": "A",
            "event_id": "evt-001",
            "event_type": "update",
            "event_timestamp": "2026-10-27T10:00:00Z",
            "payload": {"status": "active"},
        },
        {
            "entity_id": "A",
            "event_id": "evt-002",
            "event_type": "update",
            "event_timestamp": "2026-10-27T11:00:00Z",
            "payload": {"status": "inactive"},
        },
    ]

    result = transform_batch(records, batch_id="001")

    assert result.input_count == 2
    assert len(result.events) == 2
    assert result.current_entity_count == 1
    assert result.current_entities[0].event_id == "evt-002"

def test_transform_rejects_missing_entity_id() -> None:
    records = [
        {
            "event_id": "evt-001",
            "event_type": "update",
            "event_timestamp": "2026-10-27T10:00:00Z",
            "payload": {"status": "active"},
        }
    ]

    with pytest.raises(
        TransformationError,
        match="Missing transformation field",
    ):
        transform_batch(records, batch_id="001")