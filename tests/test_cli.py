import json
from pathlib import Path

from incremental_platform.cli import main


def test_status_command_returns_default_state(
    tmp_path: Path,
    capsys,
) -> None:
    exit_code = main(
        [
            "--state-dir",
            str(tmp_path / "state"),
            "status",
            "--pipeline",
            "event_ingestion",
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert exit_code == 0
    assert payload["pipeline_name"] == "event_ingestion"
    assert payload["watermark"] is None
    assert payload["last_status"] == "never_run"


def test_run_command_processes_batch(
    tmp_path: Path,
    capsys,
) -> None:
    landing = tmp_path / "landing"
    batch = landing / "batch_001"
    batch.mkdir(parents=True)

    (batch / "records.json").write_text(
        json.dumps(
            [
                {
                    "entity_id": "customer-A",
                    "event_id": "evt-001",
                    "event_type": "update",
                    "event_timestamp": "2026-10-29T10:00:00Z",
                    "payload": {"status": "active"},
                }
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "pipeline.yml"

    config_path.write_text(
        f"""
name: event_ingestion
landing_path: '{landing.as_posix()}'
batch_prefix: batch_
schema_contract:
  required_fields:
    - entity_id
    - event_id
    - event_type
    - event_timestamp
    - payload
""",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--state-dir",
            str(tmp_path / "state"),
            "--database",
            str(tmp_path / "platform.db"),
            "run",
            "--config",
            str(config_path),
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert exit_code == 0
    assert payload["status"] == "succeeded"
    assert payload["batch_id"] == "001"


def test_missing_config_returns_error(
    tmp_path: Path,
    capsys,
) -> None:
    exit_code = main(
        [
            "--state-dir",
            str(tmp_path / "state"),
            "run",
            "--config",
            str(tmp_path / "missing.yml"),
        ]
    )

    output = capsys.readouterr()

    assert exit_code == 1
    assert "Unable to load pipeline configuration" in output.err