from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class PipelineStatus(StrEnum):
    NEVER_RUN = "never_run"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class BatchStatus(StrEnum):
    DISCOVERED = "discovered"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class BatchDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(min_length=1)
    location: str = Field(min_length=1)


class PipelineState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_name: str = Field(min_length=1)
    watermark: str | None = None
    last_run_id: str | None = None
    last_status: PipelineStatus = PipelineStatus.NEVER_RUN
    last_error: str | None = None
    last_row_count: int | None = Field(default=None, ge=0)
    updated_at: datetime | None = None


class SchemaContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_fields: list[str] = Field(min_length=1)


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    landing_path: str = Field(min_length=1)
    batch_prefix: str = "batch_"
    schema_contract: SchemaContract