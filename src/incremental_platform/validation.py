from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from incremental_platform.models import (
    SchemaContract,
    ValidationResult,
    ValidationStatus,
)


def validate_records(
    records: Sequence[Mapping[str, Any]],
    *,
    contract: SchemaContract,
) -> ValidationResult:
    missing_field_counts = {
        field: 0
        for field in contract.required_fields
    }

    invalid_records = 0

    for record in records:
        record_is_valid = True

        for field in contract.required_fields:
            value = record.get(field)

            if value is None or value == "":
                missing_field_counts[field] += 1
                record_is_valid = False

        if not record_is_valid:
            invalid_records += 1

    total_records = len(records)
    valid_records = total_records - invalid_records

    status = (
        ValidationStatus.VALID
        if invalid_records == 0
        else ValidationStatus.INVALID
    )

    return ValidationResult(
        status=status,
        total_records=total_records,
        valid_records=valid_records,
        invalid_records=invalid_records,
        missing_field_counts=missing_field_counts,
    )

def partition_records(
    records: Sequence[Mapping[str, Any]],
    *,
    contract: SchemaContract,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []

    for record in records:
        normalized = dict(record)

        is_valid = all(
            field in normalized
            and normalized[field] is not None
            and normalized[field] != ""
            for field in contract.required_fields
        )

        if is_valid:
            valid.append(normalized)
        else:
            invalid.append(normalized)

    return valid, invalid

def test_partition_records_separates_invalid_data() -> None:
    records = [
        {
            "event_id": "evt-001",
            "event_type": "view",
        },
        {
            "event_id": "evt-002",
            "event_type": None,
        },
    ]

    contract = SchemaContract(
        required_fields=[
            "event_id",
            "event_type",
        ]
    )

    valid, invalid = partition_records(
        records,
        contract=contract,
    )

    assert len(valid) == 1
    assert len(invalid) == 1
    assert valid[0]["event_id"] == "evt-001"
    assert invalid[0]["event_id"] == "evt-002"