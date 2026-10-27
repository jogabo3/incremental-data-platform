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

class ValidationStatus(StrEnum):
    VALID = "valid"
    INVALID = "invalid"


class ValidationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: ValidationStatus
    total_records: int = Field(ge=0)
    valid_records: int = Field(ge=0)
    invalid_records: int = Field(ge=0)
    missing_field_counts: dict[str, int]

    @property
    def passed(self) -> bool:
        return self.status == ValidationStatus.VALID

class SchemaContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_fields: list[str] = Field(min_length=1)


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    landing_path: str = Field(min_length=1)
    batch_prefix: str = "batch_"
    schema_contract: SchemaContract

class PipelineRunStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    NO_DATA = "no_data"


class PipelineRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    pipeline_name: str
    status: PipelineRunStatus
    batch_id: str | None = None
    total_records: int = Field(default=0, ge=0)
    valid_records: int = Field(default=0, ge=0)
    invalid_records: int = Field(default=0, ge=0)
    error: str | None = None

