from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from incremental_platform.discovery import discover_next_batch
from incremental_platform.models import (
    PipelineConfig,
    PipelineRunResult,
    PipelineRunStatus,
)
from incremental_platform.state import JsonPipelineStateStore
from incremental_platform.validation import validate_records


class PipelineExecutionError(RuntimeError):
    """Raised when a discovered batch cannot be processed."""


class IncrementalPipeline:
    """Coordinates discovery, validation, and restart-safe state."""

    def __init__(
        self,
        *,
        state_store: JsonPipelineStateStore | None = None,
    ) -> None:
        self.state_store = state_store or JsonPipelineStateStore()

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
        except PipelineExecutionError as exc:
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

        validation = validate_records(
            records,
            contract=config.schema_contract,
        )

        if not validation.passed:
            error = (
                f"Batch '{batch.batch_id}' failed schema validation: "
                f"{validation.invalid_records} invalid record(s)"
            )

            self.state_store.mark_failure(
                pipeline_name=config.name,
                run_id=effective_run_id,
                error=error,
            )

            return PipelineRunResult(
                run_id=effective_run_id,
                pipeline_name=config.name,
                status=PipelineRunStatus.FAILED,
                batch_id=batch.batch_id,
                total_records=validation.total_records,
                valid_records=validation.valid_records,
                invalid_records=validation.invalid_records,
                error=error,
            )

        self.state_store.mark_success(
            pipeline_name=config.name,
            run_id=effective_run_id,
            watermark=batch.batch_id,
            row_count=validation.total_records,
        )

        return PipelineRunResult(
            run_id=effective_run_id,
            pipeline_name=config.name,
            status=PipelineRunStatus.SUCCEEDED,
            batch_id=batch.batch_id,
            total_records=validation.total_records,
            valid_records=validation.valid_records,
            invalid_records=0,
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