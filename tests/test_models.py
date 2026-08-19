import pytest
from pydantic import ValidationError

from incremental_platform.models import (
    PipelineConfig,
    PipelineState,
    PipelineStatus,
    SchemaContract,
)


def test_pipeline_state_starts_without_watermark() -> None:
    state = PipelineState(
        pipeline_name="event_ingestion",
    )

    assert state.watermark is None
    assert state.last_run_id is None
    assert state.last_status == PipelineStatus.NEVER_RUN


def test_pipeline_config_accepts_schema_contract() -> None:
    config = PipelineConfig(
        name="event_ingestion",
        landing_path="examples/landing",
        schema_contract=SchemaContract(
            required_fields=[
                "event_id",
                "event_timestamp",
            ]
        ),
    )

    assert config.name == "event_ingestion"
    assert config.batch_prefix == "batch_"
    assert "event_id" in config.schema_contract.required_fields


def test_schema_contract_rejects_empty_required_fields() -> None:
    with pytest.raises(ValidationError):
        SchemaContract(required_fields=[])