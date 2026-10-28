from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from incremental_platform.models import (
    CanonicalEvent,
    TransformationResult,
)


class TransformationError(ValueError):
    """Raised when an event cannot be transformed safely."""


def generate_record_hash(
    record: Mapping[str, Any],
) -> str:
    serialized = json.dumps(
        dict(record),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )

    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()

def normalize_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
        
    except ValueError as exc:
        raise TransformationError(
            f"Invalid event timestamp: {value}"
        ) from exc

    if parsed.tzinfo is None:
        raise TransformationError(
            "Event timestamps must include timezone information"
        )

    return parsed.astimezone(UTC)

def normalize_event(
    record: Mapping[str, Any],
    *,
    batch_id: str,
) -> CanonicalEvent:
    try:
        entity_id = record["entity_id"]
        event_id = record["event_id"]
        event_type = record["event_type"]
        timestamp = record["event_timestamp"]
        payload = record["payload"]
    except KeyError as exc:
        raise TransformationError(
            f"Missing transformation field: {exc}"
        ) from exc

    if not all(
        isinstance(value, str)
        for value in (
            entity_id,
            event_id,
            event_type,
            timestamp,
        )
    ):
        raise TransformationError(
            "Event identifiers, type, and timestamp must be strings"
        )

    if not isinstance(payload, dict):
        raise TransformationError(
            "Event payload must be a JSON object"
        )

    return CanonicalEvent(
        entity_id=entity_id,
        event_id=event_id,
        event_type=event_type,
        event_timestamp=normalize_timestamp(timestamp),
        batch_id=batch_id,
        payload=payload,
        record_hash=generate_record_hash(record),
    )

def deduplicate_entities(
    events: Sequence[CanonicalEvent],
) -> list[CanonicalEvent]:
    latest: dict[str, CanonicalEvent] = {}

    for event in events:
        current = latest.get(event.entity_id)

        if current is None:
            latest[event.entity_id] = event
            continue

        candidate_order = (
            event.event_timestamp,
            event.batch_id,
            event.event_id,
            event.record_hash,
        )

        current_order = (
            current.event_timestamp,
            current.batch_id,
            current.event_id,
            current.record_hash,
        )

        if candidate_order > current_order:
            latest[event.entity_id] = event

    return sorted(
        latest.values(),
        key=lambda event: event.entity_id,
    )

def transform_batch(
    records: Sequence[Mapping[str, Any]],
    *,
    batch_id: str,
) -> TransformationResult:
    events = [
        normalize_event(record, batch_id=batch_id)
        for record in records
    ]

    current_entities = deduplicate_entities(events)

    return TransformationResult(
        events=events,
        current_entities=current_entities,
        input_count=len(records),
        current_entity_count=len(current_entities),
    )

