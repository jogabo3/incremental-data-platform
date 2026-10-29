# Incremental Data Platform

A Python-based incremental batch processing platform demonstrating persistent checkpoints, schema validation, deterministic transformations, transactional publishing, and crash recovery.

The project focuses on the engineering mechanics required to build reliable stateful data-processing applications.

## Architecture

```text
Landing Directory
       |
       v
Ordered Batch Discovery
       |
       v
Schema Validation
       |
       v
Canonical Transformations
       |
       v
SQLite Transactional Publisher
       |
       +----> Event History
       |
       +----> Current Entity State
       |
       v
JSON Watermark Checkpoint
```

## Key Features

- Ordered discovery of incremental batches
- Persistent JSON watermarks
- Schema validation and rejection of invalid batches
- Deterministic event normalization and hashing
- Latest-event selection per entity
- Transactional SQLite publishing
- Event-ID-based idempotency and conflict detection
- Safe replay after a crash between publishing and checkpointing
- Atomic checkpoint file replacement
- Command-line execution and status inspection
- Automated unit and integration tests

## Requirements

- Python 3.11 or newer
- Git

## Installation

Clone the repository and install the package:

```bash
python -m pip install -e ".[dev]"
```

## Running the Demo

The demo configuration processes batches from `examples/demo_landing`.

```bash
incremental-platform \
  --state-dir .pipeline-runs/demo-state \
  --database .pipeline-runs/demo-platform.db \
  run --config configs/pipeline.demo.yml
```

Run the command repeatedly to process the next available batch.

Inspect the checkpoint:

```bash
incremental-platform \
  --state-dir .pipeline-runs/demo-state \
  status --pipeline operational_demo
```

Each successful execution advances the watermark after publishing completes.

When no unprocessed batches remain, the CLI returns `no_data`.

The committed demo fixtures are valid. Invalid-batch recovery is demonstrated through the integration tests or by intentionally modifying an unprocessed demo batch.

## Failure Recovery

The pipeline uses a publish-before-checkpoint strategy.

1. Discover the next batch after the committed watermark.
2. Validate and transform the records.
3. Publish events inside a SQLite transaction.
4. Advance the JSON watermark after successful publishing.

If execution stops after the database commit but before the checkpoint update, the same batch is discovered again.

The publisher recognizes previously committed event IDs and skips identical events. Conflicting content for an existing event ID is rejected.

This provides idempotent replay under the project's documented single-writer and stable-input assumptions.

## Testing

Run the complete test suite:

```bash
python -m pytest -v
```

Run static analysis:

```bash
python -m ruff check src tests
python -m mypy src
```

The tests cover batch discovery, validation, transformations, persistent state, publishing, checkpoint behavior, CLI execution, and crash recovery.

## Design Decisions

**SQLite instead of a remote database:** SQLite provides real transactional behavior while keeping the project runnable without additional infrastructure.

**JSON checkpoints:** Checkpoint files make pipeline progress inspectable and demonstrate the separation between processing state and published data.

**Publish before checkpoint:** Publishing first allows safe retries when the publisher is idempotent.

**Deterministic entity selection:** Event timestamps and stable tie-breakers determine which event represents an entity's current state.

**One batch per execution:** This keeps the processing lifecycle explicit and makes retry behavior easier to observe.

## Current Limitations

This is an engineering demonstration rather than a fully managed production data platform.

- Designed for a single pipeline writer.
- No distributed locking or leader election.
- SQLite publishing and JSON checkpointing do not share one atomic transaction.
- Landing data is assumed stable during retries.
- Invalid batches are rejected; a durable quarantine store is not implemented.
- No metrics exporter, alerting, or operational dashboard.
- No automated checkpoint/database reconciliation.
- No retention or compaction policy for event history.

## Future Improvements

- Move checkpoint and publish state into one transactional database.
- Add durable quarantine and rejection reporting.
- Introduce structured logging and operational metrics.
- Add pipeline concurrency controls.
- Support configurable storage backends.
- Add fault-injection tests for additional crash boundaries.

## Engineering Focus

This project demonstrates state management, idempotency, transactional boundaries, deterministic processing, failure recovery, and testable application design.

The emphasis is on understanding and validating reliability guarantees rather than relying on infrastructure complexity.