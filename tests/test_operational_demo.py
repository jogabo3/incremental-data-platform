import json
import sqlite3
from pathlib import Path

from incremental_platform.cli import main


def test_operational_demo_end_to_end(
    tmp_path: Path,
    capsys,
) -> None:
    landing = tmp_path / "landing"

    batches = [
        [
            {
                "entity_id": "customer-A",
                "event_id": "evt-001",
                "event_type": "created",
                "event_timestamp": "2026-10-29T10:00:00Z",
                "payload": {"status": "new"},
            }
        ],
        [
            {
                "entity_id": "customer-A",
                "event_id": "evt-002",
                "event_type": "updated",
                "event_timestamp": "2026-10-29T11:00:00Z",
                "payload": {"status": "active"},
            }
        ],
    ]

    for index, records in enumerate(batches, start=1):
        batch_dir = landing / f"batch_{index:03d}"
        batch_dir.mkdir(parents=True)
        (batch_dir / "records.json").write_text(
            json.dumps(records),
            encoding="utf-8",
        )

    config = tmp_path / "demo.yml"
    config.write_text(
        "\n".join(
            [
                "name: operational_demo",
                f"landing_path: '{landing.as_posix()}'",
                "batch_prefix: batch_",
                "schema_contract:",
                "  required_fields:",
                "    - entity_id",
                "    - event_id",
                "    - event_type",
                "    - event_timestamp",
                "    - payload",
            ]
        ),
        encoding="utf-8",
    )

    db_path = tmp_path / "platform.db"
    state_dir = tmp_path / "state"

    args = [
        "--state-dir",
        str(state_dir),
        "--database",
        str(db_path),
        "run",
        "--config",
        str(config),
    ]

    for expected_batch in ("001", "002"):
        assert main(args) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["status"] == "succeeded"
        assert result["batch_id"] == expected_batch

    assert main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "no_data"

    with sqlite3.connect(db_path) as connection:
        history_count = connection.execute(
            "SELECT COUNT(*) FROM event_history"
        ).fetchone()

        current = connection.execute(
            """
            SELECT event_id
            FROM current_entities
            WHERE entity_id = 'customer-A'
            """
        ).fetchone()

    assert history_count is not None
    assert history_count[0] == 2
    assert current == ("evt-002",)