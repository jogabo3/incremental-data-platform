from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from incremental_platform.models import PipelineConfig, PipelineRunStatus
from incremental_platform.pipeline import IncrementalPipeline
from incremental_platform.publisher import SqlitePublisher
from incremental_platform.state import JsonPipelineStateStore


def load_config(path: str | Path) -> PipelineConfig:
    config_path = Path(path)

    try:
        with config_path.open(encoding="utf-8") as file:
            payload: Any = yaml.safe_load(file)

        return PipelineConfig.model_validate(payload)

    except (OSError, yaml.YAMLError, ValidationError) as exc:
        raise ValueError(
            f"Unable to load pipeline configuration: {exc}"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="incremental-platform",
        description="Run and inspect incremental data pipelines.",
    )

    parser.add_argument(
        "--state-dir",
        default=".pipeline-state",
        help="Directory containing pipeline checkpoints.",
    )

    parser.add_argument(
        "--database",
        default=".pipeline-state/platform.db",
        help="SQLite database path.",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Process the next available batch.",
    )

    run_parser.add_argument(
        "--config",
        required=True,
        help="Path to pipeline YAML configuration.",
    )

    status_parser = subparsers.add_parser(
        "status",
        help="Display persisted pipeline state.",
    )

    status_parser.add_argument(
        "--pipeline",
        required=True,
        help="Pipeline name.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        state_store = JsonPipelineStateStore(args.state_dir)

        if args.command == "status":
            state = state_store.get(args.pipeline)
            print(state.model_dump_json(indent=2))
            return 0

        if args.command == "run":
            config = load_config(args.config)

            publisher = SqlitePublisher(args.database)

            pipeline = IncrementalPipeline(
                state_store=state_store,
                publisher=publisher,
            )

            result = pipeline.run(config)

            print(result.model_dump_json(indent=2))

            if result.status == PipelineRunStatus.FAILED:
                return 1

            return 0

    except (OSError, ValueError, RuntimeError) as exc:
        print(
            json.dumps({"error": str(exc)}),
            file=sys.stderr,
        )
        return 1

    return 1


if __name__ == "__main__":
    sys.exit(main())