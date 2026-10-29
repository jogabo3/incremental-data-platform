import sqlite3
from pathlib import Path

import pytest

from incremental_platform.models import (
    PipelineConfig,
    PipelineRunStatus,
    SchemaContract,
)
from incremental_platform.pipeline import IncrementalPipeline
from incremental_platform.publisher import SqlitePublisher
from incremental_platform.state import JsonPipelineStateStore


def test_crash_after_publish_replays_without_duplicates(
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    batch = landing / "batch_001"
    batch.mkdir(parents=True)

    (batch / "records.json").write_text(
        """
        [
            {
                "entity_id": "customer-A",
                "event_id": "evt-001",
                "event_type": "update",
                "event_timestamp": "2026-10-27T10:00:00Z",
                "payload": {"status": "active"}
            }
        ]
        """,
        encoding="utf-8",
    )

    config = PipelineConfig(
        name="event_ingestion",
        landing_path=str(landing),
        schema_contract=SchemaContract(
            required_fields=[
                "entity_id",
                "event_id",
                "event_type",
                "event_timestamp",
                "payload",
            ]
        ),
    )

    state_store = JsonPipelineStateStore(tmp_path / "state")
    db_path = tmp_path / "platform.db"
    publisher = SqlitePublisher(db_path)

    def simulate_crash() -> None:
        raise RuntimeError("simulated crash after publish")

    crashing_pipeline = IncrementalPipeline(
        state_store=state_store,
        publisher=publisher,
        after_publish=simulate_crash,
    )

    with pytest.raises(
        RuntimeError,
        match="simulated crash",
    ):
        crashing_pipeline.run(config, run_id="run-001")

    # The database committed, but the watermark did not.
    assert state_store.get("event_ingestion").watermark is None

    with sqlite3.connect(db_path) as connection:
        before = connection.execute(
            "SELECT COUNT(*) FROM event_history"
        ).fetchone()

    assert before is not None
    assert before[0] == 1

    # Simulate a fresh process using the same persisted resources.
    restarted_pipeline = IncrementalPipeline(
        state_store=JsonPipelineStateStore(tmp_path / "state"),
        publisher=SqlitePublisher(db_path),
    )

    result = restarted_pipeline.run(
        config,
        run_id="run-002",
    )

    assert result.status == PipelineRunStatus.SUCCEEDED
    assert result.batch_id == "001"

    with sqlite3.connect(db_path) as connection:
        history_count = connection.execute(
            "SELECT COUNT(*) FROM event_history"
        ).fetchone()

        current_count = connection.execute(
            "SELECT COUNT(*) FROM current_entities"
        ).fetchone()

    assert history_count is not None
    assert current_count is not None

    assert history_count[0] == 1
    assert current_count[0] == 1

    assert state_store.get("event_ingestion").watermark == "001"