from pathlib import Path

import pytest

from incremental_platform.discovery import (
    BatchDiscoveryError,
    discover_batches,
    discover_next_batch,
)


def create_batch(root: Path, name: str) -> Path:
    batch = root / name
    batch.mkdir()
    return batch


def test_discovers_batches_in_order(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(landing, "batch_003")
    create_batch(landing, "batch_001")
    create_batch(landing, "batch_002")

    batches = discover_batches(landing)

    assert [batch.batch_id for batch in batches] == [
        "001",
        "002",
        "003",
    ]


def test_first_run_selects_oldest_batch(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(landing, "batch_001")
    create_batch(landing, "batch_002")
    create_batch(landing, "batch_003")

    batch = discover_next_batch(
        landing,
        watermark=None,
    )

    assert batch is not None
    assert batch.batch_id == "001"


def test_watermark_selects_next_batch(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(landing, "batch_001")
    create_batch(landing, "batch_002")
    create_batch(landing, "batch_003")

    batch = discover_next_batch(
        landing,
        watermark="001",
    )

    assert batch is not None
    assert batch.batch_id == "002"


def test_watermark_does_not_skip_ahead(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(landing, "batch_001")
    create_batch(landing, "batch_002")
    create_batch(landing, "batch_003")
    create_batch(landing, "batch_004")

    batch = discover_next_batch(
        landing,
        watermark="002",
    )

    assert batch is not None
    assert batch.batch_id == "003"


def test_returns_none_when_no_new_batch_exists(
    tmp_path: Path,
) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(landing, "batch_001")
    create_batch(landing, "batch_002")

    batch = discover_next_batch(
        landing,
        watermark="002",
    )

    assert batch is None


def test_ignores_unrelated_directories(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()

    create_batch(landing, "batch_001")
    create_batch(landing, "archive")
    create_batch(landing, "temporary")

    batches = discover_batches(landing)

    assert [batch.batch_id for batch in batches] == ["001"]


def test_missing_landing_path_raises_clear_error(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing"

    with pytest.raises(
        BatchDiscoveryError,
        match="does not exist",
    ):
        discover_batches(missing)