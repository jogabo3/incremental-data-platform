from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from incremental_platform.models import PipelineState, PipelineStatus


class StateStoreError(RuntimeError):
    """Raised when pipeline state cannot be read or persisted."""


class JsonPipelineStateStore:
    """Persists pipeline state to the local filesystem."""

    def __init__(self, base_dir: str | Path = ".pipeline-state") -> None:
        self.base_dir = Path(base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def get(self, pipeline_name: str) -> PipelineState:
        state_path = self._state_path(pipeline_name)

        if not state_path.exists():
            return PipelineState(
                pipeline_name=pipeline_name,
            )

        try:
            return PipelineState.model_validate_json(
                state_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise StateStoreError(
                f"Unable to read state for pipeline "
                f"'{pipeline_name}': {exc}"
            ) from exc

    def save(self, state: PipelineState) -> PipelineState:
        persisted_state = state.model_copy(
            update={
                "updated_at": datetime.now(UTC),
            }
        )

        state_path = self._state_path(
            persisted_state.pipeline_name
        )

        try:
            state_path.write_text(
                persisted_state.model_dump_json(indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            raise StateStoreError(
                f"Unable to save state for pipeline "
                f"'{persisted_state.pipeline_name}': {exc}"
            ) from exc

        return persisted_state

    def mark_success(
        self,
        *,
        pipeline_name: str,
        run_id: str,
        watermark: str,
        row_count: int,
    ) -> PipelineState:
        current = self.get(pipeline_name)

        updated = current.model_copy(
            update={
                "watermark": watermark,
                "last_run_id": run_id,
                "last_status": PipelineStatus.SUCCEEDED,
                "last_error": None,
                "last_row_count": row_count,
            }
        )

        return self.save(updated)

    def mark_failure(
        self,
        *,
        pipeline_name: str,
        run_id: str,
        error: str,
    ) -> PipelineState:
        current = self.get(pipeline_name)

        updated = current.model_copy(
            update={
                "last_run_id": run_id,
                "last_status": PipelineStatus.FAILED,
                "last_error": error,
            }
        )

        return self.save(updated)

    def _state_path(self, pipeline_name: str) -> Path:
        return self.base_dir / f"{pipeline_name}.json"