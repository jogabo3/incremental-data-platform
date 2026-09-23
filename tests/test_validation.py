from incremental_platform.models import (
    SchemaContract,
    ValidationStatus,
)
from incremental_platform.validation import validate_records


def test_valid_records_pass_validation() -> None:
    records = [
        {
            "event_id": "evt-001",
            "event_type": "page_view",
            "event_timestamp": "2026-08-30T10:00:00Z",
        },
        {
            "event_id": "evt-002",
            "event_type": "click",
            "event_timestamp": "2026-08-30T10:01:00Z",
        },
    ]

    contract = SchemaContract(
        required_fields=[
            "event_id",
            "event_type",
            "event_timestamp",
        ]
    )

    result = validate_records(
        records,
        contract=contract,
    )

    assert result.status == ValidationStatus.VALID
    assert result.passed is True
    assert result.total_records == 2
    assert result.valid_records == 2
    assert result.invalid_records == 0


def test_missing_required_field_fails_validation() -> None:
    records = [
        {
            "event_id": "evt-001",
            "event_type": "page_view",
            "event_timestamp": "2026-08-30T10:00:00Z",
        },
        {
            "event_id": "evt-002",
            "event_type": "",
            "event_timestamp": "2026-08-30T10:01:00Z",
        },
    ]

    contract = SchemaContract(
        required_fields=[
            "event_id",
            "event_type",
            "event_timestamp",
        ]
    )

    result = validate_records(
        records,
        contract=contract,
    )

    assert result.status == ValidationStatus.INVALID
    assert result.passed is False
    assert result.invalid_records == 1
    assert result.missing_field_counts["event_type"] == 1


def test_record_missing_multiple_fields_is_counted_once_as_invalid() -> None:
    records = [
        {
            "event_id": "evt-001",
            "event_type": None,
        }
    ]

    contract = SchemaContract(
        required_fields=[
            "event_id",
            "event_type",
            "event_timestamp",
        ]
    )

    result = validate_records(
        records,
        contract=contract,
    )

    assert result.invalid_records == 1
    assert result.missing_field_counts["event_type"] == 1
    assert result.missing_field_counts["event_timestamp"] == 1


def test_empty_batch_is_valid() -> None:
    contract = SchemaContract(
        required_fields=[
            "event_id",
            "event_timestamp",
        ]
    )

    result = validate_records(
        [],
        contract=contract,
    )

    assert result.status == ValidationStatus.VALID
    assert result.total_records == 0
    assert result.invalid_records == 0