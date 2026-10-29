from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

from incremental_platform.discovery import discover_next_batch
from incremental_platform.models import (
    PipelineConfig,
    PipelineRunResult,
    PipelineRunStatus,
)
from incremental_platform.publisher import PublishingError, SqlitePublisher
from incremental_platform.state import JsonPipelineStateStore
from incremental_platform.transforms import (
    TransformationError,
    transform_batch,
)
from incremental_platform.validation import validate_records


class PipelineExecutionError(RuntimeError):
    """Raised when a discovered batch cannot be processed."""


class IncrementalPipeline:
    """Coordinates incremental processing and restart-safe publishing."""

    def __init__(
        self,
        *,
        state_store: JsonPipelineStateStore | None = None,
        publisher: SqlitePublisher | None = None,
        after_publish: Callable[[], None] | None = None,
    ) -> None:
        self.state_store = state_store or JsonPipelineStateStore()
        self.publisher = publisher or SqlitePublisher(
            ".pipeline-state/platform.db"
        )
        self.after_publish = after_publish

    def run(
        self,
        config: PipelineConfig,
        *,
        run_id: str | None = None,
    ) -> PipelineRunResult:
        effective_run_id = run_id or str(uuid4())

        state = self.state_store.get(config.name)

        batch = discover_next_batch(
            config.landing_path,
            watermark=state.watermark,
            batch_prefix=config.batch_prefix,
        )

        if batch is None:
            return PipelineRunResult(
                run_id=effective_run_id,
                pipeline_name=config.name,
                status=PipelineRunStatus.NO_DATA,
            )

        try:
            records = self._load_records(batch.location)

            validation = validate_records(
                records,
                contract=config.schema_contract,
            )

            if not validation.passed:
                raise PipelineExecutionError(
                    f"Batch '{batch.batch_id}' failed schema validation: "
                    f"{validation.invalid_records} invalid record(s)"
                )

            transformed = transform_batch(
                records,
                batch_id=batch.batch_id,
            )

            self.publisher.publish(transformed.events)

            if self.after_publish is not None:
                self.after_publish()

            self.state_store.mark_success(
                pipeline_name=config.name,
                run_id=effective_run_id,
                watermark=batch.batch_id,
                row_count=transformed.input_count,
            )

            return PipelineRunResult(
                run_id=effective_run_id,
                pipeline_name=config.name,
                status=PipelineRunStatus.SUCCEEDED,
                batch_id=batch.batch_id,
                total_records=transformed.input_count,
                valid_records=transformed.input_count,
                invalid_records=0,
            )

        except (
            PipelineExecutionError,
            TransformationError,
            PublishingError,
        ) as exc:
            self.state_store.mark_failure(
                pipeline_name=config.name,
                run_id=effective_run_id,
                error=str(exc),
            )

            return PipelineRunResult(
                run_id=effective_run_id,
                pipeline_name=config.name,
                status=PipelineRunStatus.FAILED,
                batch_id=batch.batch_id,
                error=str(exc),
            )

    @staticmethod
    def _load_records(
        batch_location: str,
    ) -> list[dict[str, Any]]:
        records_path = Path(batch_location) / "records.json"

        if not records_path.exists():
            raise PipelineExecutionError(
                f"Batch records do not exist: {records_path}"
            )

        try:
            payload = json.loads(
                records_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineExecutionError(
                f"Unable to load batch records: {exc}"
            ) from exc

        if not isinstance(payload, list):
            raise PipelineExecutionError(
                "Batch records must contain a JSON array"
            )

        if not all(isinstance(record, dict) for record in payload):
            raise PipelineExecutionError(
                "Every batch record must be a JSON object"
            )

        return payload