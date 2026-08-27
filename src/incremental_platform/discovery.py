from __future__ import annotations

from pathlib import Path

from incremental_platform.models import BatchDescriptor


class BatchDiscoveryError(RuntimeError):
    """Raised when landing batches cannot be discovered safely."""


def discover_batches(
    landing_path: str | Path,
    *,
    batch_prefix: str = "batch_",
) -> list[BatchDescriptor]:
    root = Path(landing_path).resolve()

    if not root.exists():
        raise BatchDiscoveryError(
            f"Landing path does not exist: {root}"
        )

    if not root.is_dir():
        raise BatchDiscoveryError(
            f"Landing path is not a directory: {root}"
        )

    batches: list[BatchDescriptor] = []

    for path in root.iterdir():
        if not path.is_dir():
            continue

        if not path.name.startswith(batch_prefix):
            continue

        batch_id = path.name.removeprefix(batch_prefix)

        if not batch_id:
            continue

        batches.append(
            BatchDescriptor(
                batch_id=batch_id,
                location=str(path),
            )
        )

    return sorted(
        batches,
        key=lambda batch: batch.batch_id,
    )


def discover_next_batch(
    landing_path: str | Path,
    *,
    watermark: str | None,
    batch_prefix: str = "batch_",
) -> BatchDescriptor | None:
    batches = discover_batches(
        landing_path,
        batch_prefix=batch_prefix,
    )

    for batch in batches:
        if watermark is None or batch.batch_id > watermark:
            return batch

    return None