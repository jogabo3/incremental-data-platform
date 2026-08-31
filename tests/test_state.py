from pathlib import Path

from incremental_platform.models import PipelineStatus
from incremental_platform.state import JsonPipelineStateStore


def test_missing_state_returns_default_state(
    tmp_path: Path,
) -> None:
    store = JsonPipelineStateStore(tmp_path / "state")

    state = store.get("event_ingestion")

    assert state.pipeline_name == "event_ingestion"
    assert state.watermark is None
    assert state.last_status == PipelineStatus.NEVER_RUN


def test_success_advances_watermark(
    tmp_path: Path,
) -> None:
    store = JsonPipelineStateStore(tmp_path / "state")

    state = store.mark_success(
        pipeline_name="event_ingestion",
        run_id="run-001",
        watermark="001",
        row_count=125,
    )

    assert state.watermark == "001"
    assert state.last_run_id == "run-001"
    assert state.last_status == PipelineStatus.SUCCEEDED
    assert state.last_row_count == 125
    assert state.last_error is None
    assert state.updated_at is not None


def test_failure_does_not_advance_watermark(
    tmp_path: Path,
) -> None:
    store = JsonPipelineStateStore(tmp_path / "state")

    store.mark_success(
        pipeline_name="event_ingestion",
        run_id="run-001",
        watermark="001",
        row_count=100,
    )

    failed_state = store.mark_failure(
        pipeline_name="event_ingestion",
        run_id="run-002",
        error="schema validation failed",
    )

    assert failed_state.watermark == "001"
    assert failed_state.last_run_id == "run-002"
    assert failed_state.last_status == PipelineStatus.FAILED
    assert failed_state.last_error == "schema validation failed"


def test_success_after_failure_advances_from_existing_watermark(
    tmp_path: Path,
) -> None:
    store = JsonPipelineStateStore(tmp_path / "state")

    store.mark_success(
        pipeline_name="event_ingestion",
        run_id="run-001",
        watermark="001",
        row_count=100,
    )

    store.mark_failure(
        pipeline_name="event_ingestion",
        run_id="run-002",
        error="temporary failure",
    )

    recovered = store.mark_success(
        pipeline_name="event_ingestion",
        run_id="run-003",
        watermark="002",
        row_count=120,
    )

    assert recovered.watermark == "002"
    assert recovered.last_status == PipelineStatus.SUCCEEDED
    assert recovered.last_error is None


def test_state_persists_across_store_instances(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"

    first_store = JsonPipelineStateStore(state_dir)

    first_store.mark_success(
        pipeline_name="event_ingestion",
        run_id="run-001",
        watermark="003",
        row_count=250,
    )

    second_store = JsonPipelineStateStore(state_dir)
    state = second_store.get("event_ingestion")

    assert state.watermark == "003"
    assert state.last_run_id == "run-001"
    assert state.last_row_count == 250