# Work Package Execution Prompts

Individual prompts for executing each Work Package from the Production Parity Hardening Plan.

**Plan Location:** `docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md`
**Branch:** `feature/table-maintenance-2.0`

---

## How to Use These Prompts

1. Copy the prompt for the WP you want to execute
2. Start a new Claude Code session
3. Paste the prompt
4. Follow the structured execution flow

---

## P0 Work Packages (Execute First)

### WP0 - Cross-Engine Test Infrastructure (PREREQUISITE)

```markdown
# Execute WP0: Cross-Engine Test Infrastructure

## Context
You are executing WP0 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Problem
Current infrastructure has Spark run `provision.py` once at startup then idle with `tail -f`.
There's no mechanism to execute Spark queries AFTER Rust modifies tables.

WP1+ requires: Spark provisions -> Rust does DML -> Spark validates

## Objective
Enable Rust tests to invoke Spark for validation after Rust operations complete.

## Key Files
- Docker setup: `crates/integration_tests/testdata/docker-compose.yaml`
- Provision script: `crates/integration_tests/testdata/spark/provision.py`
- Existing tests: `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`

## Deliverables

### 1. Create validate.py Script
```python
#!/usr/bin/env python3
"""Spark validation script - invoked via docker exec"""
import sys
import json
from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder \
        .appName("IcebergValidation") \
        .getOrCreate()

def validate_count(spark, table_path):
    return {"count": spark.read.format("iceberg").load(table_path).count()}

def validate_full(spark, table_path):
    df = spark.read.format("iceberg").load(table_path)
    return {
        "count": df.count(),
        "checksum": df.selectExpr("sum(hash(*))").collect()[0][0],
        "columns": df.columns,
    }

def validate_metadata(spark, table_path):
    snapshots = spark.sql(f"SELECT * FROM iceberg.`{table_path}`.snapshots")
    files = spark.sql(f"SELECT * FROM iceberg.`{table_path}`.files")
    return {
        "snapshot_count": snapshots.count(),
        "file_count": files.count(),
        "current_snapshot": snapshots.orderBy("committed_at", ascending=False).first().asDict() if snapshots.count() > 0 else None
    }

VALIDATORS = {
    "count": validate_count,
    "full": validate_full,
    "metadata": validate_metadata,
}

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Usage: validate.py <table_path> <validation_type>"}))
        sys.exit(1)

    table_path = sys.argv[1]
    validation_type = sys.argv[2]

    spark = get_spark()
    try:
        if validation_type not in VALIDATORS:
            result = {"error": f"Unknown validation type: {validation_type}"}
        else:
            result = VALIDATORS[validation_type](spark, table_path)
        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)
    finally:
        spark.stop()
```

### 2. Create Rust Test Helper
```rust
// In crates/integration_tests/src/spark_validator.rs

use iceberg_integration_tests::spark_validator::{
    spark_validate_with_container, ValidationResult, ValidationType,
};

let container_name = fixture.spark_container_name();
let result = spark_validate_with_container(
    &container_name,
    "test_table",
    ValidationType::Count,
)
.await?;
```

### 3. Proof of Concept Test
```rust
#[tokio::test]
async fn test_rust_delete_spark_validates() {
    // 1. Table already provisioned by Spark
    let table_name = "test_table";
    let spark_container = fixture.spark_container_name();

    // 2. Perform DELETE with Rust
    let table = load_table(table_path).await.unwrap();
    table.delete()
        .filter(col("id").eq(lit(5)))
        .commit()
        .await
        .unwrap();

    // 3. Validate with Spark
    let before_count = 100; // known from provision.py
    let result = spark_validate_with_container(
        &spark_container,
        table_name,
        ValidationType::Count,
    )
    .await
    .unwrap();

    assert_eq!(result.count.unwrap(), before_count - 1);
}
```

## Execution Steps

1. **Explore current infrastructure**
   ```bash
   cat crates/integration_tests/testdata/docker-compose.yaml
   cat crates/integration_tests/testdata/spark/provision.py
   ```

2. **Create validate.py**
   - Place in `crates/integration_tests/testdata/spark/validate.py`
   - Ensure it's mounted in Docker container

3. **Test validate.py manually**
   ```bash
   cd crates/integration_tests/testdata
   docker compose up -d
   docker exec "$(docker compose ps -q spark-iceberg)" spark-submit /home/validate.py \
       test_partition_evolution_delete count
   ```

4. **Create Rust helper**
   - Add to integration tests crate
   - Handle JSON parsing and errors

5. **Create proof of concept test**
   - Pick simplest operation (DELETE or UPDATE)
   - Verify full round-trip works

6. **Document the pattern**
   - Add comments showing how to use for other operations

## Acceptance Criteria
- [ ] `validate.py` can be invoked via `docker exec` and returns valid JSON
- [ ] Rust helper successfully parses Spark validation results
- [ ] At least one Rust operation validated end-to-end with Spark
- [ ] Pattern documented for reuse across WP1 tests
- [ ] Error handling works (Spark errors surface in Rust tests)

## Commands
```bash
# Start infrastructure
cd crates/integration_tests/testdata && docker compose up -d

# Test validate.py manually
docker exec "$(docker compose ps -q spark-iceberg)" spark-submit \
    /home/validate.py \
    test_partition_evolution_delete count

# Run Rust integration tests
cargo test --package iceberg-integration-tests

# View Spark logs if needed
docker compose logs spark-iceberg
```

This WP is the foundation for all other interop tests. Complete it first before starting WP1.
```

---

### WP1 - Bidirectional Interop Validation

```markdown
# Execute WP1: Bidirectional Interop Validation (Rust -> Spark)

## Context
You are executing WP1 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Known Blockers (from WP0 validation)

The following DML limitations on **partitioned tables** were discovered during WP0:

| Operation | Error | Status |
| --------- | ----- | ------ |
| DELETE | "DELETE on partitioned tables is not yet supported" | Needs fix |
| UPDATE | "Partition key required for partitioned position delete writer" | Needs fix |
| MERGE | Arrow type mismatch (RunEndEncoded vs Utf8) | Needs investigation |

**Workaround:** Start with **unpartitioned tables** for initial WP1 progress, then address partitioned table support.

## Objective
Prove that every Rust write/maintenance operation produces Iceberg tables that Spark reads correctly.

## Key Files
- Cross-engine tests: `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`
- Spark harness: `crates/integration_tests/testdata/spark/provision.py`
- Docker setup: `crates/integration_tests/testdata/docker-compose.yaml`

## Deliverables
Create Rust -> Spark interop tests for each operation:

1. DELETE (simple predicate)
2. DELETE (complex predicate with NULL semantics)
3. UPDATE (single column)
4. MERGE (insert + update + delete)
5. Compaction (binpack)
6. Rewrite manifests
7. Expire snapshots (metadata correctness)
8. Remove orphan files (delete mode)

## Test Pattern
For each operation:
```rust
// 1. Create table with Spark (or Rust)
// 2. Perform operation with Rust
// 3. Read with Spark and validate:
//    - count(*)
//    - count(distinct key)
//    - checksum aggregate
//    - min/max bounds
// 4. Query metadata tables and verify consistency
```

## Acceptance Criteria (Per Operation)
- [ ] Spark read succeeds (no errors)
- [ ] At least 2 correctness validations pass (count, checksum, etc.)
- [ ] Metadata sanity check passes (snapshots/files/manifests consistent)
- [ ] Test is deterministic (re-run yields identical results)

## Execution Steps
1. Read existing cross-engine test infrastructure
2. Identify current Rust -> Spark coverage gaps
3. Create TodoWrite tracking list for each operation
4. Implement tests one operation at a time
5. Run tests locally with Docker
6. Document any failures or unexpected behaviors

## Commands
```bash
# Start test infrastructure
cd crates/integration_tests && docker-compose up -d

# Run cross-engine tests
cargo test --package iceberg-integration-tests partition_evolution

# Run specific test
cargo test --package iceberg-integration-tests test_name
```

Begin by exploring the existing test infrastructure and identifying gaps.
```

---

### WP1.5 - Semantic Parity Validation

```markdown
# Execute WP1.5: Semantic Parity Validation (Metadata + Behavior)

## Context
You are executing WP1.5 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Validate that Rust-written metadata matches Iceberg-Java/Spark expectations EXACTLY, not just "is readable". This validates existing implementations produce semantically correct output.

## Why This Matters
Interop tests prove Spark can read Rust output, but don't validate that metadata fields, commit structure, and edge case behaviors are identical to what Spark would produce. Subtle differences break monitoring tools, audit systems, and downstream consumers.

## Deliverables

### 1. Snapshot Summary Field Validation
Compare Rust vs Spark snapshot summaries for equivalent operations:
- `added-data-files`
- `deleted-data-files`
- `added-records`
- `deleted-records`
- `added-files-size`
- `removed-files-size`
- `operation` type

### 2. Manifest Entry Structure Comparison
- Verify manifest entry ordering
- Validate all required fields populated
- Check partition value serialization format

### 3. Commit Metadata Consistency
- Operation type in snapshot summary
- Parent snapshot linkage
- Timestamp format and precision

### 4. Edge Case Behavior Matrix
Test these scenarios produce identical behavior:
- NULL handling in predicates and partition values
- Empty result sets (DELETE that matches nothing)
- Boundary values (min/max integers, empty strings)
- Empty partitions after DELETE

### 5. Error Rejection Parity
Verify Rust rejects what Spark rejects:
- Invalid schema changes
- Incompatible type promotion
- Constraint violations

## Test Approach
For each operation (DELETE, UPDATE, MERGE, compaction, expire):
1. Create identical initial table state
2. Perform operation with Spark on table A
3. Perform identical operation with Rust on table B
4. Compare metadata JSON structures field-by-field
5. Document and justify any differences

## Key Files
- Snapshot structure: `crates/iceberg/src/spec/snapshot.rs`
- Manifest structure: `crates/iceberg/src/spec/manifest/`
- Transaction commits: `crates/iceberg/src/transaction/`

## Acceptance Criteria
- [ ] Snapshot summary fields match Spark (or differences documented with rationale)
- [ ] Manifest entries contain all required fields with correct values
- [ ] Edge case behaviors produce identical outcomes
- [ ] Error rejection is consistent

## Output
Create a document: `docs/parity/semantic-parity-findings.md` with:
- Field-by-field comparison results
- Any divergences with rationale
- Recommended fixes if needed

Begin by reading the snapshot and manifest structures, then create comparison tests.
```

---

### WP1.10 - Concurrency Test Suite

```markdown
# Execute WP1.10: Synthetic Concurrency Test Suite

## Context
You are executing WP1.10 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Build confidence against corruption under concurrent writers/maintenance operations.

## Key Files
- Transaction commits: `crates/iceberg/src/transaction/`
- Catalog interface: `crates/iceberg/src/catalog/`
- Existing tests: `crates/iceberg/src/transaction/*/tests`

## Deliverables

### Synthetic Rust Concurrency Tests
Create tests for these concurrent scenarios:

1. **append vs append**
   - Two concurrent appends to same table
   - Both should succeed or one retries

2. **rewrite_data_files vs append**
   - Compaction running while append occurs
   - Conflict detection should work correctly

3. **RowDelta vs append**
   - Delete/update running while append occurs
   - Proper sequence number handling

4. **expire_snapshots vs append**
   - Expiration running while new data written
   - Should not delete newly referenced files

### Optional Cross-Engine Concurrency (Nightly)
- Spark append while Rust compaction runs
- Spark append while Rust expire_snapshots runs

## Test Pattern
```rust
#[tokio::test]
async fn test_concurrent_append_vs_append() {
    // 1. Create table
    // 2. Spawn two concurrent append tasks
    // 3. Wait for both to complete
    // 4. Assert:
    //    - At least one succeeded
    //    - If both succeeded, data is correct
    //    - If one failed, it was a clean conflict error
    //    - Table is readable and consistent
}
```

## Acceptance Criteria
- [ ] Exactly one conflicting commit succeeds OR operation retries safely
- [ ] Loser fails with clear conflict error (not corruption)
- [ ] Table remains readable by both Rust and Spark afterwards
- [ ] No metadata corruption (snapshot lineage valid, manifest references consistent)

## Commands
```bash
# Run concurrency tests
cargo test --package iceberg concurrent

# Run with increased parallelism to stress test
RUST_TEST_THREADS=8 cargo test --package iceberg concurrent
```

Begin by exploring existing transaction tests and catalog conflict handling.
```

---

### WP2 - Expire Snapshots File Deletion

```markdown
# Execute WP2: Expire Snapshots File Deletion

## Context
You are executing WP2 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Extend expire snapshots from metadata-only to safe file deletion, matching operational expectations.

## Current State
Expire snapshots currently only removes metadata. File deletion is not implemented.

## Key Files
- Main implementation: `crates/iceberg/src/transaction/expire_snapshots/`
- Cleanup logic: `crates/iceberg/src/transaction/expire_snapshots/cleanup.rs`
- Retention logic: `crates/iceberg/src/transaction/expire_snapshots/retention.rs`

## Deliverables

### 1. Plan + Execute File Deletion
Implement deletion of unreferenced:
- Manifest lists
- Manifests
- Data files
- Delete files

### 2. Support Modes
- `dry_run` (strongly recommended default)
- `delete_files` (explicit opt-in)
- Optional "Spark-like mode" via config

### 3. Safety Guarantees
- NEVER delete files referenced by retained snapshots
- NEVER delete files referenced by ANY refs (branches/tags)
- Handle concurrent modifications safely

## Implementation Pattern
```rust
impl ExpireSnapshots {
    pub fn plan(&self) -> ExpireSnapshotsPlan {
        // Identify snapshots to expire
        // Identify files to delete (unreferenced)
        // Return plan without executing
    }

    pub async fn execute(&self, plan: ExpireSnapshotsPlan, options: ExpireOptions) -> Result<ExpireResult> {
        if options.dry_run {
            return Ok(plan.to_dry_run_result());
        }
        // Execute deletions
        // Return counts
    }
}
```

## Acceptance Criteria
- [ ] Safety: Never deletes files referenced by retained snapshots or refs
- [ ] Dry-run fidelity: Dry-run lists exactly what would be deleted
- [ ] Concurrency: If table changes, operation retries safely or fails cleanly
- [ ] Interop: After expire+delete, Spark reads retained snapshots correctly
- [ ] Observability: Returns counts for deleted snapshots/manifests/data files/delete files

## Testing
1. Create table with multiple snapshots
2. Run expire with dry_run=true, capture plan
3. Run expire with delete_files=true
4. Verify deleted files match dry-run plan
5. Verify Spark can still read retained snapshots
6. Test with branches/tags present - verify they're respected

Begin by reading the current expire_snapshots implementation and cleanup.rs.
```

---

### WP3.1 - Equality Delete Interop (Spark -> Rust)

```markdown
# Execute WP3.1: Equality Delete Interop (Spark -> Rust)

## Context
You are executing WP3.1 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Add cross-engine test coverage to ensure Rust reads match Spark when Spark writes equality deletes.

## Current State (Verified)
Equality delete read path is implemented:
- `delete_file_index.rs` - indexing and sequence filtering
- `caching_delete_file_loader.rs` - parsing to predicates
- `delete_filter.rs` - predicate application
- `reader.rs` - integration with scan

## Key Files
- Cross-engine tests: `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`
- Delete filter: `crates/iceberg/src/arrow/delete_filter.rs`
- Delete loader: `crates/iceberg/src/arrow/caching_delete_file_loader.rs`

## Deliverables
Create interop tests covering:

### Test Cases
1. **Basic equality delete (int key)**
   - Spark writes equality delete on integer column
   - Rust reads and applies correctly

2. **String key equality delete**
   - Spark writes equality delete on string column
   - Rust reads and applies correctly

3. **NULL handling**
   - Spark writes equality delete with NULL in key column
   - Rust handles correctly (should match or not match based on spec)

4. **Multi-column equality key**
   - Spark writes equality delete with composite key
   - Rust applies all columns correctly

## Test Pattern
```python
# In provision.py - Spark side
spark.sql("""
    DELETE FROM test_table
    WHERE id = 5
""")
# This creates equality delete file
```

```rust
// In Rust test
// Read table and verify row with id=5 is not returned
// Compare count with Spark's count
```

## Acceptance Criteria
- [ ] Covers at least two data types (int + string)
- [ ] Covers NULL cases in delete key columns
- [ ] Covers multi-column equality keys
- [ ] Validations include at least 2 of: count(*), count(distinct key), checksum
- [ ] Tests pass in CI

Begin by exploring how Spark writes equality deletes and extend the cross-engine test harness.
```

---

### WP3.2 - Memory Bounds Validation

```markdown
# Execute WP3.2: Memory Bounds Validation for Equality Deletes

## Context
You are executing WP3.2 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Prove we do not OOM when equality delete files are large, plus implement guard mechanism if memory is unbounded.

## Known Risk
Current implementation accumulates one predicate per deleted row:
```rust
// caching_delete_file_loader.rs:333
let mut row_predicates = Vec::new();
// Each deleted row adds a predicate
row_predicates.push(row_predicate.not().rewrite_not());
```

For N deleted rows, memory grows O(N predicates).

## Key Files
- Predicate accumulation: `crates/iceberg/src/arrow/caching_delete_file_loader.rs:329-400`
- Delete filter: `crates/iceberg/src/arrow/delete_filter.rs`

## Deliverables

### 1. Stress Test
Create test that:
- Generates equality delete file with 100k+ deleted rows
- Scans a representative data file
- Measures peak memory usage
- Asserts memory stays bounded OR operation fails gracefully

### 2. Guard Mechanism (If Memory Unbounded)
Implement one or more of:
- Configurable predicate limit per delete file (table property)
- Explicit memory budget with `MemoryLimitExceeded` error
- More compact internal representation (later optimization)

## Test Implementation
```rust
#[tokio::test]
async fn test_equality_delete_memory_bounds() {
    // 1. Create table with data
    // 2. Generate equality delete file with 100k+ rows
    // 3. Measure memory before scan
    // 4. Execute scan with delete application
    // 5. Measure peak memory during scan
    // 6. Assert: peak_memory < threshold OR graceful error
}
```

## Memory Measurement Options
- Use `jemalloc` stats if enabled
- Use `/proc/self/status` VmPeak on Linux
- Use custom allocator wrapper for tracking

## Acceptance Criteria (Non-Negotiable)
- [ ] Delete application does NOT OOM the process
- [ ] Either:
  - Bounded memory: peak RSS stays below threshold, OR
  - Graceful failure: returns `MemoryLimitExceeded` / `PredicateLimitExceeded` with remediation guidance

Begin by creating the stress test to measure current memory behavior.
```

---

**WP3.2 Verification Notes (Completed):**
- Stress test: 150k deletes -> 100 data rows, full ArrowReader scan, ~767ms, 0 rows remaining.
- Linux guard: VmHWM delta <= 512MB (skipped on non-Linux).
- Guard behavior: `PreconditionFailed` with remediation guidance when row limit exceeded; limit disabled passes.

## P1 Work Packages

### WP4 - INSERT OVERWRITE Semantics

```markdown
# Execute WP4: INSERT OVERWRITE Semantics

## Context
You are executing WP4 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Implement Spark-aligned INSERT OVERWRITE semantics.

## Key Files
- Replace partitions: `crates/iceberg/src/transaction/replace_partitions.rs`
- Overwrite: `crates/iceberg/src/transaction/overwrite.rs`

## Deliverables

### Phase 1: Dynamic Overwrite
Overwrite only partitions touched by incoming data.
```sql
INSERT OVERWRITE table SELECT * FROM source
-- Only replaces partitions that appear in source data
```

### Phase 2: Static Overwrite
Overwrite specified partitions regardless of insert content.
```sql
INSERT OVERWRITE table PARTITION (date='2024-01-01') SELECT * FROM source
-- Replaces partition even if source has no rows for it
```

## Implementation Pattern
```rust
pub struct InsertOverwrite {
    mode: OverwriteMode,  // Dynamic or Static
    partition_filter: Option<Expression>,  // For static mode
}

impl InsertOverwrite {
    pub async fn execute(&self, data: RecordBatchStream) -> Result<()> {
        match self.mode {
            OverwriteMode::Dynamic => {
                // Identify touched partitions from data
                // Delete files in those partitions
                // Write new files
            }
            OverwriteMode::Static => {
                // Delete files matching partition_filter
                // Write new files (may be empty for some partitions)
            }
        }
    }
}
```

## Acceptance Criteria
- [ ] Dynamic: Only touched partitions replaced; untouched unchanged
- [ ] Static: Specified partitions replaced even with no incoming rows
- [ ] Atomicity: Commit is atomic; no partial state visible
- [ ] Interop: Spark reads correct results after overwrite

Begin by exploring replace_partitions.rs to understand existing infrastructure.
```

---

### WP5 - WAP / Branch-Tag Ergonomics

```markdown
# Execute WP5: WAP / Branch-Tag Ergonomics

## Context
You are executing WP5 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Add high-level APIs for Write-Audit-Publish workflows on top of existing low-level actions.

## Current State (VERIFIED)

### What EXISTS:
| Component | Location | Description |
|-----------|----------|-------------|
| `ManageSnapshotsAction` | `manage_snapshots.rs` | `rollback_to_snapshot`, `rollback_to_timestamp`, `set_current_snapshot`, `on_branch` |
| `SetSnapshotRef` TableUpdate | `table_update.rs` | Low-level update for setting refs |
| Ref-safety in expire_snapshots | `expire_snapshots/retention.rs:183-226` | Already protects branches/tags |

### What's MISSING:
| Component | Impact |
|-----------|--------|
| `CreateBranchAction` | Cannot create branches via high-level API |
| `CreateTagAction` | Cannot create tags via high-level API |
| `FastForwardAction` | Cannot fast-forward refs via high-level API |
| High-level Table helpers | No ergonomic WAP workflow API |

## Key Files
- ManageSnapshots: `crates/iceberg/src/transaction/manage_snapshots.rs`
- Table updates: `crates/iceberg/src/spec/table_update.rs`
- Refs metadata: `crates/iceberg/src/inspect/refs.rs`
- Ref-safety: `crates/iceberg/src/transaction/expire_snapshots/retention.rs`

## Deliverables

### 1. Create Low-Level Actions
```rust
// In manage_snapshots.rs or new files

/// Create a branch pointing to a snapshot
pub struct CreateBranchAction {
    branch_name: String,
    snapshot_id: i64,
    retention: Option<SnapshotRetention>,
}

/// Create a tag pointing to a snapshot
pub struct CreateTagAction {
    tag_name: String,
    snapshot_id: i64,
}

/// Fast-forward a ref to match another ref's snapshot
pub struct FastForwardAction {
    target_ref: String,
    source_ref: String,
}
```

### 2. High-Level Table Helpers
```rust
impl Table {
    /// Create branch from current or specified snapshot
    pub fn create_branch(&self, name: &str, snapshot_id: Option<i64>) -> Result<()> {
        let tx = Transaction::new(self);
        tx.create_branch(name, snapshot_id.unwrap_or(self.current_snapshot_id()?))
            .apply(tx)?
            .commit(&self.catalog()).await
    }

    /// Create tag from current or specified snapshot
    pub fn create_tag(&self, name: &str, snapshot_id: Option<i64>) -> Result<()> {
        // Similar pattern
    }

    /// Fast-forward target ref to source ref's snapshot
    pub fn fast_forward(&self, target: &str, source: &str) -> Result<()> {
        // Validate source exists, get its snapshot, update target
    }

    /// Builder for writing to a specific branch
    pub fn write_to_branch(&self, branch: &str) -> BranchWriteBuilder {
        BranchWriteBuilder::new(self, branch)
    }
}
```

### 3. Ref-Safety Audit
```bash
# Verify expire_snapshots respects refs
grep -n "branch\|tag\|ref" crates/iceberg/src/transaction/expire_snapshots/retention.rs

# Verify remove_orphan_files respects refs
grep -n "branch\|tag\|ref" crates/iceberg/src/transaction/remove_orphan_files/
```

Document findings and add tests if gaps found.

## Execution Steps

1. **Explore existing ManageSnapshotsAction**
   ```bash
   cat crates/iceberg/src/transaction/manage_snapshots.rs | head -200
   ```

2. **Understand SetSnapshotRef update**
   ```bash
   grep -A 20 "SetSnapshotRef" crates/iceberg/src/spec/table_update.rs
   ```

3. **Create CreateBranchAction**
   - Add to manage_snapshots.rs or new file
   - Implement TransactionAction trait
   - Add tests

4. **Create CreateTagAction**
   - Similar pattern to CreateBranchAction

5. **Create FastForwardAction**
   - Validate source ref exists
   - Update target ref to source's snapshot

6. **Add Table helpers**
   - Ergonomic methods on Table struct
   - Use actions internally

7. **Verify ref-safety**
   - Review expire_snapshots retention logic
   - Add interop tests with Spark

## Acceptance Criteria
- [ ] `CreateBranchAction` creates branch readable by Spark
- [ ] `CreateTagAction` creates tag readable by Spark
- [ ] `FastForwardAction` updates refs correctly
- [ ] Table helpers provide ergonomic API
- [ ] expire_snapshots respects refs (verify existing behavior)
- [ ] `$refs` metadata table reflects expected state

Begin by exploring manage_snapshots.rs to understand the action pattern.
```

---

### WP6 - Schema Evolution Interop

```markdown
# Execute WP6: Schema Evolution Interoperability Suite

## Context
You are executing WP6 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Ensure schema evolution works cross-engine.

## Key Files
- Schema: `crates/iceberg/src/spec/schema.rs`
- Evolution: `crates/iceberg/src/transaction/` (schema evolution actions)
- Existing tests: `crates/integration_tests/`

## Deliverables

### Spark DDL -> Rust Reads
Test each operation:
1. ADD COLUMN - Spark adds column, Rust reads correctly
2. RENAME COLUMN - Spark renames, Rust resolves by field ID
3. DROP COLUMN - Spark drops, Rust handles missing column

### Rust DDL -> Spark Reads
Same operations in reverse:
1. ADD COLUMN - Rust adds, Spark reads
2. RENAME COLUMN - Rust renames, Spark reads
3. DROP COLUMN - Rust drops, Spark reads

### Historical Snapshot Reads
- Evolve schema
- Read historical snapshot with prior schema
- Verify correct column resolution

## Test Pattern
```rust
#[tokio::test]
async fn test_spark_add_column_rust_reads() {
    // 1. Create table with Spark (columns: id, name)
    // 2. Insert data with Spark
    // 3. Spark: ALTER TABLE ADD COLUMN age INT
    // 4. Insert more data with Spark (with age column)
    // 5. Rust: Read all data
    // 6. Verify: Old rows have NULL for age, new rows have values
}
```

## Acceptance Criteria
- [ ] Correct column resolution after rename/drop (field ID semantics)
- [ ] Reads correct for historical snapshots with prior schema
- [ ] Cross-engine correctness validated by count + checksum

Begin by exploring schema evolution capabilities and existing DDL support.
```

---

### WP7 - Time Travel SQL Syntax

```markdown
# Execute WP7: Time Travel Query Syntax (DataFusion)

## Context
You are executing WP7 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
DataFusion SQL parity for time travel queries.

## Key Files
- DataFusion table: `crates/integrations/datafusion/src/table/mod.rs`
- SQL parsing: DataFusion's SQL parser (may need extension)

## Deliverables

### Supported Syntax
```sql
-- Version-based time travel
SELECT * FROM table VERSION AS OF 12345678901234

-- Timestamp-based time travel
SELECT * FROM table TIMESTAMP AS OF '2024-01-15 10:30:00'

-- In subqueries
SELECT * FROM current_table c
JOIN table VERSION AS OF 123 AS historical ON c.id = historical.id

-- In CTEs
WITH historical AS (
    SELECT * FROM table TIMESTAMP AS OF '2024-01-01'
)
SELECT * FROM historical
```

### Implementation Approach
1. Extend DataFusion SQL parsing (or use table functions)
2. Resolve snapshot ID from version/timestamp
3. Create TableProvider configured for that snapshot
4. Execute query against historical snapshot

## Acceptance Criteria
- [ ] Works in SELECT, JOIN, subquery/CTE
- [ ] Resolves to correct snapshot
- [ ] Results equal core API scan at that snapshot
- [ ] Clear errors for invalid snapshot/timestamp

Begin by exploring how DataFusion handles table references and whether SQL extensions are needed.
```

---

### WP8 - Incremental Scan API

```markdown
# Execute WP8: Incremental Scan API ("Changes Between Snapshots")

## Context
You are executing WP8 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Provide CDC-friendly API that returns changed files between snapshots.

## Key Files
- Snapshot utilities: `crates/iceberg/src/spec/snapshot_util.rs`
- Scan: `crates/iceberg/src/scan/`
- Table: `crates/iceberg/src/table.rs`

## Deliverables

### API Design
```rust
impl Table {
    /// Get changes between two snapshots
    pub fn incremental_scan(
        &self,
        from_snapshot: SnapshotId,
        to_snapshot: SnapshotId,
    ) -> IncrementalScanBuilder {
        // Returns builder for incremental scan
    }
}

pub struct IncrementalChanges {
    pub added_data_files: Vec<DataFile>,
    pub removed_data_files: Vec<DataFile>,
    pub added_delete_files: Vec<DataFile>,
    pub removed_delete_files: Vec<DataFile>,
}
```

### Supported Commit Types
Must handle correctly:
- Append commits (only additions)
- Row-delta commits (data + delete files)
- Rewrite/compaction commits (removes old, adds new)

## Acceptance Criteria
- [ ] Correct change sets for each commit type
- [ ] Efficient - doesn't require full manifest scan when possible
- [ ] Cross-engine validation via metadata tables where possible

Begin by exploring snapshot_util.rs and understanding how snapshots track changes.
```

---

### WP9 - Metadata Tables ($partitions, $entries)

```markdown
# Execute WP9: Metadata Tables ($partitions and $entries)

## Context
You are executing WP9 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Add high-value operational metadata tables missing from current set.

## Current State
Implemented: `snapshots`, `manifests`, `files`, `history`, `refs`, `properties`
Missing: `partitions`, `entries`

## Key Files
- Inspect module: `crates/iceberg/src/inspect/`
- Existing tables: `crates/iceberg/src/inspect/files.rs`, `snapshots.rs`, etc.

## Deliverables

### $partitions Table
Partition-level summaries:
```sql
SELECT * FROM table$partitions
-- Returns:
-- partition_value, file_count, record_count, total_bytes
```

Schema:
- `partition` (struct) - partition value
- `spec_id` (int) - partition spec ID
- `file_count` (long) - number of data files
- `record_count` (long) - total records
- `file_size_in_bytes` (long) - total bytes

### $entries Table
Low-level manifest entries for debugging:
```sql
SELECT * FROM table$entries
-- Returns all manifest entries with full details
```

Schema:
- `status` (int) - 0=existing, 1=added, 2=deleted
- `snapshot_id` (long)
- `sequence_number` (long)
- `file_sequence_number` (long)
- `data_file` (struct) - full data file info

## Acceptance Criteria
- [ ] Schemas consistent and stable
- [ ] Results match Spark expectations on same table
- [ ] Performance: $partitions operates from manifests, not row-level data

Begin by exploring the inspect module and understanding how existing metadata tables are implemented.
```

---

### WP10 - Operational CLI Tool

```markdown
# Execute WP10: Operational CLI Tool (iceberg-rust-cli)

## Context
You are executing WP10 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Provide production-friendly interface for maintenance without embedding DataFusion SQL.

## Deliverables

### CLI Commands
```bash
# Compaction
iceberg-cli compact --table s3://bucket/table --strategy binpack

# Rewrite manifests
iceberg-cli rewrite-manifests --table s3://bucket/table

# Expire snapshots (dry-run)
iceberg-cli expire-snapshots --table s3://bucket/table --older-than 7d --dry-run

# Expire snapshots (execute)
iceberg-cli expire-snapshots --table s3://bucket/table --older-than 7d --delete-files

# Remove orphan files (dry-run)
iceberg-cli remove-orphans --table s3://bucket/table --dry-run

# Remove orphan files (execute)
iceberg-cli remove-orphans --table s3://bucket/table --delete

# Rewrite position delete files
iceberg-cli rewrite-deletes --table s3://bucket/table
```

### Output Formats
```bash
# Human-readable (default)
iceberg-cli expire-snapshots --table ... --dry-run
# Output: Would expire 5 snapshots, delete 100 files (1.5 GB)

# JSON for orchestration
iceberg-cli expire-snapshots --table ... --dry-run --output json
# Output: {"snapshots_to_expire": 5, "files_to_delete": 100, "bytes_to_free": 1500000000}
```

## Implementation
Use `clap` for CLI parsing:
```rust
#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Compact { ... },
    ExpireSnapshots { ... },
    RemoveOrphans { ... },
    RewriteManifests { ... },
    RewriteDeletes { ... },
}
```

## Acceptance Criteria
- [ ] Works in CI using same harness tables
- [ ] Supports dry-run and explicit opt-in for destructive actions
- [ ] Returns non-zero exit code on partial failures
- [ ] JSON output for orchestration integration

Begin by creating a new crate for the CLI and wiring up basic command parsing.
```

---

### WP11 - Manifest Cache Benchmarking

```markdown
# Execute WP11: Manifest Cache Benchmarking and Tuning

## Context
You are executing WP11 from the Production Parity Hardening Plan.
Plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md
Branch: feature/table-maintenance-2.0

## Objective
Validate cache effectiveness and tune defaults for production.

## Current State
Manifest caching implemented in `crates/iceberg/src/io/object_cache.rs` with property-based configuration.

## Key Files
- Object cache: `crates/iceberg/src/io/object_cache.rs`
- Existing benchmark: `crates/iceberg/benches/scan_planning_benchmark.rs`
- Scan planning: `crates/iceberg/src/scan/mod.rs`

## Deliverables

### Benchmark Profiles
Create profiles with varying metadata sizes:
- **Small**: 10 manifests, 100 files
- **Medium**: 100 manifests, 10,000 files
- **Large**: 1,000 manifests, 100,000 files

### Measurements
For each profile, measure:
- Planning latency p50/p95 (with and without cache)
- Cache hit ratio
- Memory footprint
- Invalidation correctness after commits

### Benchmark Implementation
```rust
#[bench]
fn bench_planning_small_no_cache(b: &mut Bencher) { ... }

#[bench]
fn bench_planning_small_with_cache(b: &mut Bencher) { ... }

#[bench]
fn bench_planning_medium_no_cache(b: &mut Bencher) { ... }

#[bench]
fn bench_planning_medium_with_cache(b: &mut Bencher) { ... }

// etc.
```

## Acceptance Criteria
- [ ] Demonstrate measurable planning improvement with cache on medium/large profiles
- [ ] No stale reads after commits (invalidation correct)
- [ ] Document recommended default properties for production

## Output
Create document: `docs/performance/manifest-cache-benchmark-results.md` with:
- Benchmark methodology
- Results table (latency, hit ratio, memory)
- Recommended production settings

Begin by exploring the existing benchmark and object cache implementation.
```

---

## Quick Reference: All WPs

| WP | Priority | Focus | Est. Effort |
|----|----------|-------|-------------|
| WP1 | P0 | Rust -> Spark interop | 3-5 days |
| WP1.5 | P0 | Semantic parity validation | 2-3 days |
| WP1.10 | P0 | Concurrency tests | 2-3 days |
| WP2 | P0 | Expire snapshots file deletion | 3-5 days |
| WP3.1 | P0 | Equality delete interop | 1-2 days |
| WP3.2 | P0 | Memory bounds validation | 1-2 days |
| WP4 | P1 | INSERT OVERWRITE | 3-5 days |
| WP5 | P1 | WAP helpers | 2-3 days |
| WP6 | P1 | Schema evolution interop | 2-3 days |
| WP7 | P1 | Time travel SQL | 2-3 days |
| WP8 | P1 | Incremental scan API | 2-3 days |
| WP9 | P1 | Metadata tables | 2-3 days |
| WP10 | P1/P2 | CLI tool | 3-5 days |
| WP11 | P1 | Cache benchmarking | 2-3 days |
