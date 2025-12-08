# Iceberg DataFusion UPDATE Operation

This folder contains design documentation, architecture decision records (ADRs), and sprint execution summaries for the UPDATE SQL operation implementation in iceberg-datafusion.

## Documentation Index

| Document | Description |
|----------|-------------|
| [DESIGN.md](./DESIGN.md) | Technical design and architecture overview |
| [ADR-001-cow-semantics.md](./adr/ADR-001-cow-semantics.md) | Copy-on-Write vs Merge-on-Read decision |
| [ADR-002-partition-evolution.md](./adr/ADR-002-partition-evolution.md) | Partition evolution handling decision |
| [ADR-003-atomic-commit.md](./adr/ADR-003-atomic-commit.md) | Atomic commit via RowDelta decision |
| [SPRINT-SUMMARY.md](./SPRINT-SUMMARY.md) | Sprint 1-3 execution summary |

## Quick Links

- **Source Code**: `crates/integrations/datafusion/src/`
  - [update.rs](../../crates/integrations/datafusion/src/update.rs) - UpdateBuilder API
  - [physical_plan/update.rs](../../crates/integrations/datafusion/src/physical_plan/update.rs) - IcebergUpdateExec
  - [physical_plan/update_commit.rs](../../crates/integrations/datafusion/src/physical_plan/update_commit.rs) - IcebergUpdateCommitExec
  - [position_delete_task_writer.rs](../../crates/integrations/datafusion/src/position_delete_task_writer.rs) - Position delete writer

- **Tests**: `crates/integrations/datafusion/tests/integration_datafusion_test.rs`

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| Unpartitioned UPDATE | Complete | Full CoW support |
| Partitioned UPDATE | Complete | Identity + transform partitions |
| Partition Evolution | Not Supported | Guard with clear error message |
| SQL Interface | Planned | Sprint 4 |
| Merge-on-Read | Not Planned | CoW only |

## Usage

```rust
use iceberg_datafusion::IcebergTableProvider;
use datafusion::prelude::*;

// Programmatic UPDATE
let count = provider
    .update().await?
    .set("status", lit("shipped"))
    .set("updated_at", current_timestamp())
    .filter(col("id").eq(lit(42)))
    .execute(&session_state)
    .await?;
```
