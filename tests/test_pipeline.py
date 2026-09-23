import json
from pathlib import Path

from incremental_platform.models import (
    PipelineConfig,
    PipelineRunStatus,
    PipelineStatus,
    SchemaContract,
)
from incremental_platform.pipeline import IncrementalPipeline
from incremental_platform.state import JsonPipelineStateStore


def create_batch(
    landing: Path,
    batch_id: str,
    records: list[dict[str, object]],
) -> None:
    batch = landing / f"batch_{batch_id}"
    batch.mkdir()

    (batch / "records.json").write_text(
        json.dumps(records),
        encoding="utf-8",
    )


def make_config(landing: Path) -> PipelineConfig:
    return PipelineConfig(
        name="event_ingestion",
        landing_path=str(landing),
        schema_contract=SchemaContract(
            required_fields=[
                "event_id",
                "event_timestamp",
            ]
        ),
    )


def test_pipeline_processes_next_batch_and_advances_watermark(
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(
        landing,
        "001",
        [
            {
                "event_id": "evt-001",
                "event_timestamp": "2026-09-22T10:00:00Z",
            }
        ],
    )

    store = JsonPipelineStateStore(tmp_path / "state")
    pipeline = IncrementalPipeline(state_store=store)

    result = pipeline.run(
        make_config(landing),
        run_id="run-001",
    )

    assert result.status == PipelineRunStatus.SUCCEEDED
    assert result.batch_id == "001"

    state = store.get("event_ingestion")

    assert state.watermark == "001"
    assert state.last_status == PipelineStatus.SUCCEEDED

def test_failed_batch_does_not_advance_watermark(
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(
        landing,
        "001",
        [
            {
                "event_id": "evt-001",
                "event_timestamp": "2026-09-22T10:00:00Z",
            }
        ],
    )

    create_batch(
        landing,
        "002",
        [
            {
                "event_id": "evt-002",
            }
        ],
    )

    store = JsonPipelineStateStore(tmp_path / "state")
    pipeline = IncrementalPipeline(state_store=store)
    config = make_config(landing)

    first = pipeline.run(config, run_id="run-001")
    second = pipeline.run(config, run_id="run-002")

    assert first.status == PipelineRunStatus.SUCCEEDED
    assert second.status == PipelineRunStatus.FAILED
    assert second.batch_id == "002"

    state = store.get("event_ingestion")

    assert state.watermark == "001"
    assert state.last_status == PipelineStatus.FAILED

def test_fixed_failed_batch_is_reprocessed(
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(
        landing,
        "001",
        [
            {
                "event_id": "evt-001",
                "event_timestamp": "2026-09-22T10:00:00Z",
            }
        ],
    )

    create_batch(
        landing,
        "002",
        [
            {
                "event_id": "evt-002",
            }
        ],
    )

    store = JsonPipelineStateStore(tmp_path / "state")
    pipeline = IncrementalPipeline(state_store=store)
    config = make_config(landing)

    pipeline.run(config, run_id="run-001")

    failed = pipeline.run(
        config,
        run_id="run-002",
    )

    assert failed.status == PipelineRunStatus.FAILED

    create_batch_path = landing / "batch_002" / "records.json"

    create_batch_path.write_text(
        json.dumps(
            [
                {
                    "event_id": "evt-002",
                    "event_timestamp": "2026-09-22T10:05:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    recovered = pipeline.run(
        config,
        run_id="run-003",
    )

    assert recovered.status == PipelineRunStatus.SUCCEEDED
    assert recovered.batch_id == "002"

    state = store.get("event_ingestion")

    assert state.watermark == "002"
    assert state.last_status == PipelineStatus.SUCCEEDED

def test_pipeline_returns_no_data_when_caught_up(
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(
        landing,
        "001",
        [
            {
                "event_id": "evt-001",
                "event_timestamp": "2026-09-22T10:00:00Z",
            }
        ],
    )

    store = JsonPipelineStateStore(tmp_path / "state")
    pipeline = IncrementalPipeline(state_store=store)
    config = make_config(landing)

    pipeline.run(config, run_id="run-001")

    result = pipeline.run(
        config,
        run_id="run-002",
    )

    assert result.status == PipelineRunStatus.NO_DATA
    assert result.batch_id is None

    state = store.get("event_ingestion")

    assert state.watermark == "001"
