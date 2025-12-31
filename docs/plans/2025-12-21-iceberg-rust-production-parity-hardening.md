# Iceberg-Rust Production Parity Hardening Plan
## Spark / Iceberg-Java Alignment Roadmap (Execution-Ready)

**Version:** 3.3
**Date:** 2025-12-21
**Primary branch:** `feature/table-maintenance-2.0`
**Primary parity target:** Iceberg-Java semantics as exposed via Apache Spark (procedures/actions)
**Primary deployment target:** GCP (GCS storage + SQL/REST catalog), while remaining storage/catalog-agnostic.

---

## 1) Executive summary

The `feature/table-maintenance-2.0` branch is already significantly ahead of earlier parity-roadmap assumptions. The remaining work is no longer "build the big features", but instead:

1) **Prove bidirectional interoperability** (Rust writes -> Spark reads) with strict CI gates
2) **Harden safety semantics** for destructive maintenance (expire + orphan deletion)
3) **Close the final parity gaps** (equality deletes, overwrite semantics, refs/WAP ergonomics, schema evolution interop, time travel SQL, incremental scan API)
4) **Quantify performance and memory safety** (delete application bounds, manifest cache benchmarks)

This plan defines a set of **Work Packages (WPs)** with **strict acceptance criteria**, explicit **dependencies**, and recommended **PR slicing**.

---

## 2) Current state (branch maturity snapshot)

### 2.1 Implemented in branch (per team review)
| Area | Status |
|---|---|
| DELETE / UPDATE / MERGE DML | Implemented (Copy-on-Write) |
| Position delete writer | Implemented |
| RowDelta commits | Implemented |
| Compaction | Implemented (binpack + sorted) |
| Rewrite manifests | Implemented |
| Expire snapshots | Implemented (metadata-only) |
| Remove orphan files | Implemented |
| Metadata tables | Implemented: `snapshots`, `manifests`, `files`, `history`, `refs`, `properties` |
| Manifest cache | Implemented (property-based config) |
| Dynamic partition pruning | Merged |
| Cross-engine harness | Docker + Spark exists |
| Spark writes -> Rust reads | Covered by existing tests |

### 2.2 Primary gaps to close for "Spark alignment"
| Gap | Why it matters |
|---|---|
| Rust writes -> Spark reads interop tests | This is the parity gate; catches subtle spec/metadata issues |
| Expire snapshots file deletion | Current metadata-only behavior does not match operational expectations; must be safe |
| Equality deletes | Read path verified; remaining: bidirectional interop tests + memory-bound guard (predicate growth caveat) |
| INSERT OVERWRITE semantics | Common ETL pattern; expected by Spark-aligned users |
| Branch/tag/WAP high-level APIs | Actions exist, but ergonomics + ref-safe maintenance are needed |
| Schema evolution interop | Spark DDL -> Rust reads and Rust DDL -> Spark reads must work |
| Time travel SQL syntax | `VERSION AS OF` / `TIMESTAMP AS OF` parity for DataFusion |
| Incremental scan API | Required for CDC "changes between snapshots" patterns |
| `$partitions` and `$entries` metadata tables | High-value operational visibility |
| CLI tool | Operational interface for actions without embedding DataFusion |
| Manifest cache benchmarking | Validate cache effectiveness on production-scale metadata |
| Broader concurrency tests | Confidence against corruption under real concurrent writers |
| Semantic parity validation | Validate metadata fields, commit structure, and edge case behavior match Spark exactly |

---

## 3) Parity definition and non-negotiables

### 3.1 What "Spark / Iceberg-Java parity" means in this plan
Parity is not "we support similar SQL strings"; parity is:

- **Spec-visible correctness**: metadata, snapshots, manifests, delete semantics, refs, and maintenance outcomes match Spark expectations.
- **Bidirectional interoperability**: Spark can read and validate what Rust writes, and vice versa.
- **Operational safety**: destructive actions are safe by default (or require explicit opt-in), with dry-run and ref-awareness.
- **Predictable behavior across table states**: partition evolution, schema evolution, multiple specs/schemas, and time travel.

### 3.2 Non-negotiables
1) **No OOM delete application**: delete filtering must be memory-bounded (or fail gracefully with explicit error).
2) **No bypass paths**: DataFusion scans must use the same delete-aware core scan execution as library scans.
3) **Ref-awareness**: expire/orphan cleanup must not delete snapshots/files referenced by branches/tags (even if created externally).
4) **Safety defaults**: destructive operations must support dry-run, retention windows, and clear logs/metrics.

---

## 4) Execution model

### 4.1 CI tiers (recommended)
- **PR gate (fast):** small tables, deterministic datasets, "smoke" interop checks, minimal Spark job runs.
- **Nightly (thorough):** edge cases, concurrency tests, bigger datasets, memory-bound tests, performance checks.
- **Release candidate (RC):** full matrix + benchmark thresholds.

### 4.2 Test philosophy
- Prefer stable validations (counts + checksums) over full dataset comparisons.
- Validate both **data results** and **metadata invariants** (snapshots/files/manifests consistency).
- For destructive actions: require dry-run parity with execution.

---

## 5) Work Packages (WPs)

### WP0 -- Cross-Engine Test Infrastructure **P0 (Prerequisite)**

**Objective:** Enable Rust tests to invoke Spark for validation after Rust operations complete.

**Problem:** Current infrastructure has Spark run `provision.py` once at startup then idle. There's no mechanism to execute Spark queries after Rust modifies tables.

**Key files:**
- Docker setup: `crates/integration_tests/testdata/docker-compose.yaml`
- Provision script: `crates/integration_tests/testdata/spark/provision.py`
- Existing tests: `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`

**Deliverables:**

1. **Validation script infrastructure**
   - Create `validate.py` that accepts table path and validation type as arguments
   - Returns JSON with validation results (counts, checksums, metadata)
   - Can be invoked via `docker exec`

2. **Rust test helper**
   - Create helper function to invoke Spark validation from Rust tests
   - Parse JSON results and assert correctness
   - Handle timeouts and errors gracefully

3. **Proof of concept**
   - Implement one complete Rust -> Spark validation (e.g., simple DELETE)
   - Verify pattern works end-to-end before scaling to all operations

**Implementation approach:**

```python
# validate.py - Spark validation script
import sys
import json
from pyspark.sql import SparkSession

def validate_table(table_path, validation_type):
    spark = SparkSession.builder.getOrCreate()
    table = spark.read.format("iceberg").load(table_path)

    if validation_type == "count":
        return {"count": table.count()}
    elif validation_type == "full":
        return {
            "count": table.count(),
            "checksum": table.selectExpr("sum(hash(*))").collect()[0][0],
            "snapshot_count": spark.sql(f"SELECT * FROM {table_path}.snapshots").count()
        }
    # etc.

if __name__ == "__main__":
    result = validate_table(sys.argv[1], sys.argv[2])
    print(json.dumps(result))
```

```rust
// Rust test helper
fn spark_validate(table_path: &str, validation_type: &str) -> Result<ValidationResult> {
    let output = Command::new("docker")
        .args(["exec", "spark-iceberg", "spark-submit",
               "/home/validate.py", table_path, validation_type])
        .output()?;

    let result: ValidationResult = serde_json::from_slice(&output.stdout)?;
    Ok(result)
}
```

**Acceptance criteria:**
- [ ] `validate.py` can be invoked via `docker exec` and returns JSON
- [ ] Rust helper function successfully parses Spark validation results
- [ ] At least one Rust operation (DELETE) validated end-to-end with Spark
- [ ] Pattern is documented for reuse across all WP1 tests

---

### WP1 -- Bidirectional interop validation (Rust writes -> Spark reads) **P0**
**Objective:** Make interoperability symmetric and enforce it continuously in CI.

**Key files:**
- `partition_evolution_crossengine.rs` (extend coverage)
- `provision.py` (Spark harness orchestration)

**Known Blockers (discovered during WP0 validation):**

The following DML limitations on **partitioned tables** must be addressed before full WP1 completion:

| Operation | Error | Root Cause |
| --------- | ----- | ---------- |
| DELETE | "DELETE on partitioned tables is not yet supported" | DataFusion `TableProvider::delete()` rejects partitioned tables |
| UPDATE | "Partition key required for partitioned position delete writer" | Position delete writer needs partition key for partitioned tables |
| MERGE | Arrow type mismatch (RunEndEncoded vs Utf8) | Schema handling issue with dictionary-encoded columns |

**Workaround for initial WP1 progress:** Test DML operations on **unpartitioned tables** first, then address partitioned table support incrementally.

**Deliverables (P0 coverage):**
- After each of these Rust operations, Spark reads the table and validations pass:
  - DELETE (simple predicate)
  - DELETE (complex predicate including NULL semantics)
  - UPDATE (single column)
  - MERGE (insert + update + delete)
  - Compaction (binpack)
  - Rewrite manifests
  - Expire snapshots (metadata correctness)
  - Remove orphan files (delete mode)

**Acceptance criteria (strict, per operation):**
- Spark read succeeds (no errors, no broken metadata).
- Correctness validated using at least **two** of:
  - `count(*)`
  - `count(distinct key)`
  - deterministic checksum aggregate over key columns
  - min/max bounds on key columns
- Metadata sanity:
  - Spark can query table metadata (snapshots/files/manifests) and results are internally consistent.
- Deterministic:
  - re-run yields identical results (no flaky timing assumptions).

#### WP1.11 -- DataFusion-only validation tests (optional follow-up) **P2**

**Objective:** Add fast, Spark-free validation tests that verify internal DataFusion consistency without external interop overhead.

**Rationale:** While Spark validation proves external interoperability (the primary goal), DataFusion-only tests provide:

- Faster CI feedback (no Docker/Spark startup)
- Regression testing for DataFusion-specific behavior
- Unit-level validation of the integration layer

**Deliverables:**

- DataFusion read-back tests for each operation (DELETE, UPDATE, MERGE, compaction, etc.)
- Validate results match expected outcomes using pure Rust/DataFusion
- Can run in parallel with Spark tests or as fast-path CI gate

**Acceptance criteria:**

- Tests pass without Spark container
- Results consistent with Spark validation results for same operations
- Execution time significantly faster than Spark-based tests

**Note:** This is complementary to (not a replacement for) Spark interop validation.

#### WP1.10 -- Synthetic concurrency test suite (expanded) **P0**
**Objective:** Build confidence against corruption under concurrent writers/maintenance.

**Deliverables:**
- Synthetic Rust tests for:
  1) append vs append
  2) rewrite_data_files vs append
  3) RowDelta vs append
  4) expire_snapshots vs append (if not covered elsewhere)
- Optional cross-engine concurrency (nightly):
  - Spark append while Rust compaction runs
  - Spark append while Rust expire_snapshots runs

**Acceptance criteria:**
- Exactly one of conflicting commits succeeds OR operation retries safely; the loser fails with a clear conflict error.
- Table remains readable by both Rust and Spark afterwards.
- No metadata corruption (snapshot lineage valid, manifest references consistent).

**Status:** Complete (tests in `crates/iceberg/src/transaction/concurrent.rs`).

**WP1.10 Results:**
- Implemented 9 synthetic concurrency tests covering append, RowDelta, expire snapshots, and compaction-like replace partitions.
- All tests pass; conflicts surface as `CatalogCommitConflicts` and table consistency is verified.

#### WP1.5 -- Semantic parity validation (metadata + behavior) **P0**

**Objective:** Validate that Rust-written metadata matches Iceberg-Java/Spark expectations exactly, not just "is readable". This validates existing implementations produce semantically correct output.

**Why this matters:** Interop tests prove Spark can read Rust output, but don't validate that metadata fields, commit structure, and edge case behaviors are identical to what Spark would produce. Subtle differences can break monitoring tools, audit systems, and downstream consumers.

**Deliverables:**

1. **Snapshot summary field validation**
   - Compare Rust-produced vs Spark-produced snapshots for equivalent operations
   - Validate fields: `added-data-files`, `deleted-data-files`, `added-records`, `deleted-records`, `added-files-size`, `removed-files-size`, `operation` type
   - Document any intentional divergences with rationale

2. **Manifest entry structure comparison**
   - Verify manifest entry ordering matches Spark expectations
   - Validate all required fields are populated correctly
   - Check partition value serialization format

3. **Commit metadata consistency**
   - Operation type in snapshot summary
   - Parent snapshot linkage
   - Timestamp format and precision

4. **Edge case behavior matrix**
   - NULL handling in predicates and partition values
   - Empty result sets (DELETE that matches nothing)
   - Boundary values (min/max integers, empty strings)
   - Empty partitions after DELETE

5. **Error rejection parity**
   - Verify Rust rejects operations that Spark would reject
   - Invalid schema changes (incompatible type promotion)
   - Constraint violations where applicable

**Test approach:**

- For each operation (DELETE, UPDATE, MERGE, compaction, expire):
  1. Perform identical operation with Spark on table A
  2. Perform identical operation with Rust on table B (same initial state)
  3. Compare metadata JSON structures field-by-field
  4. Document and justify any differences

**Acceptance criteria (strict):**

- Snapshot summary fields match Spark output for equivalent operations (or differences are documented and justified)
- Manifest entries contain all required fields with correct values
- Edge case behaviors produce identical outcomes to Spark
- Error rejection is consistent (Rust doesn't allow operations Spark would reject)

---

### WP2 -- Expire snapshots file deletion (integrated, safe) **P0**
**Objective:** Extend expire snapshots from metadata-only to safe file deletion, matching operational expectations.

**Key file:** `expire_snapshots/cleanup.rs`

**Deliverables:**
- Plan + execute deletion of unreferenced:
  - manifest lists and manifests
  - data files and delete files that are no longer referenced
- Support modes:
  - `dry_run` (strongly recommended default)
  - `delete_files` (explicit opt-in)
  - optional "Spark-like mode" via CLI/SQL config if desired

**Acceptance criteria (strict):**
- Safety:
  - Never deletes any file referenced by retained snapshots or any refs (branches/tags).
- Dry-run fidelity:
  - Dry-run lists exactly what would be deleted; execution matches plan.
- Concurrency:
  - If table changes between plan and commit, operation retries safely or fails cleanly; no partial corruption.
- Interop:
  - After expire+delete, Spark reads retained snapshots correctly; results match pre-expire state for retained snapshots.
- Observability:
  - Returns counts for deleted snapshots/manifests/data files/delete files.

#### WP2.2 -- Concurrency test: expire_snapshots vs append **P0**
(If not fully covered by WP1.10, keep as separate explicit gate.)

---

### WP3 -- Equality deletes: interop + memory bounds (READ-COMPLETE) **P0 (read), P1 (write)**

**Status:** **Read-complete** with **memory caveat**.

Based on the verification report, the equality delete path is present end-to-end:
- Delete file indexing + sequence/partition matching is implemented and spec-consistent
- Equality delete rows are parsed into predicates (with explicit NULL handling via `is_null()`)
- Predicates are AND-ed with the scan predicate in the reader
- Write support exists (`RowDelta.add_equality_delete_files()`), with unit tests

**Memory caveat (P0 risk):** while delete files are **streamed** in record batches, the loader currently accumulates **one predicate per deleted row** (e.g. `row_predicates: Vec<_>`). For large equality delete files with **N rows**, memory can grow roughly with **O(N predicates)**. This must be proven safe under expected workloads or guarded with an explicit budget/limit + graceful failure.

**Key files (from verification)**
- `delete_file_index.rs` -- equality delete indexing; sequence filtering; partition matching; global equality deletes
- `caching_delete_file_loader.rs` -- streams record batches; builds per-row predicates; NULL handling
- `delete_filter.rs` / `arrow/delete_filter.rs` -- predicate caching; binding; **TODO** case-insensitive matching
- `reader.rs` -- integrates equality delete predicate with scan predicate
- `row_delta.rs` -- `add_equality_delete_files()` write path exists

#### WP3.1 -- Interop test: Spark equality deletes -> Rust reads **(P0)**
**Deliverable:** Add cross-engine test coverage to ensure Rust reads match Spark when Spark writes equality deletes.

**Where to extend:** `partition_evolution_crossengine.rs` (and shared harness utilities)

**Acceptance criteria (strict):**
- Covers at least:
  - two data types (e.g., int + string)
  - NULL cases in delete key columns
  - multi-column equality keys
- Validations include at least two of:
  - `count(*)`
  - `count(distinct key)`
  - deterministic checksum aggregate
- Must pass in CI (PR gate or nightly, depending on Spark job cost)

#### WP3.2 -- Memory bounds validation + guard for equality delete application **(P0)**
**Deliverable:** A stress test that proves we do not OOM when equality delete files are large, plus a guard mechanism if memory is unbounded.

**Test approach:**
- Generate equality delete file with **100k+ deleted rows** (or many delete files totaling 100k+ rows)
- Scan a representative data file
- Assert **peak memory** stays under a threshold **OR** the operation fails **gracefully** with a clear error (not an OOM crash)

**Acceptance criteria (non-negotiable):**
- Delete application does **not** OOM the process.
- Either:
  1) **Bounded memory:** peak RSS stays below configured threshold for the stress profile, **or**
  2) **Graceful failure:** operation returns an explicit error such as `MemoryLimitExceeded` / `PredicateLimitExceeded` with remediation guidance.

**If memory proves unbounded:** implement one (or more) of:
- **Configurable predicate limit** per equality delete file (table property or session option), with a safe default and clear error message.
- **Explicit memory budget** for delete predicate construction; abort with `MemoryLimitExceeded`.
- (Optional later) more compact internal representation (e.g., keyed membership filter / dictionary encodings) to reduce per-row expression overhead.

**Status:** Complete (tests in `crates/iceberg/src/arrow/caching_delete_file_loader.rs`).

**WP3.2 Results:**
- `test_equality_delete_memory_bounds_100k_rows`: 150k deletes -> 100 data rows via full ArrowReader scan; VmHWM delta guard <= 512MB on Linux; 0 rows remaining; ~767ms.
- `test_equality_delete_memory_bounds_multicolumn`: 50k rows x 3 columns with nulls; pass.
- `test_equality_delete_memory_bounds_string_columns`: 100k string values; pass.
- `test_equality_delete_predicate_limit_guard`: `PreconditionFailed` with remediation guidance when limit exceeded.
- `test_equality_delete_limit_disabled`: no limit configured; pass.

#### WP3.3 -- Reverse interop: Rust equality deletes -> Spark reads **(P1)**
**Deliverable:** Validate Spark can read and correctly apply equality deletes written by Rust.

**Notes:** Write support exists already via `RowDelta.add_equality_delete_files()`; this work is primarily *interop validation* plus any small fixes discovered by Spark.

**Acceptance criteria:**
- Spark reads match expected results and Rust reads, using the same validation strategy as WP3.1.

#### WP3.4 -- Case-insensitive matching TODO **(P2)**
**Deliverable:** Resolve the known TODO in `delete_filter.rs` regarding case-insensitive binding/matching.

**Acceptance criteria:**
- Tables with column names that differ only by case (where applicable) behave consistently and match Spark expectations.

---

### WP4 -- INSERT OVERWRITE semantics **P1**
**Objective:** Implement Spark-aligned overwrite semantics.

**Key file:** `replace_partitions.rs` (existing infrastructure likely present)

**Deliverables:**
- Phase 1: dynamic overwrite (overwrite touched partitions only)
- Phase 2: static overwrite (overwrite specified partitions regardless of insert content)

**Acceptance criteria (strict):**
- Dynamic overwrite:
  - Only touched partitions replaced; untouched partitions unchanged (file references stable).
- Static overwrite:
  - Specified partitions replaced even if no incoming rows exist for a partition.
- Atomicity:
  - Commit is atomic; no partial state visible.
- Interop:
  - Spark reads correct results after overwrite.

---

### WP5 -- WAP / branch-tag ergonomics **P1**

**Objective:** Add high-level APIs for WAP workflows on top of existing low-level actions.

**Current state (verified):**

| What exists | Location |
|-------------|----------|
| `ManageSnapshotsAction` with `rollback_to_snapshot`, `rollback_to_timestamp`, `set_current_snapshot`, `on_branch` | `manage_snapshots.rs` |
| `SetSnapshotRef` TableUpdate (low-level) | `table_update.rs` |
| Ref-safety in expire_snapshots (protects branches/tags) | `expire_snapshots/retention.rs:183-226` |

| What's missing | Impact |
|----------------|--------|
| `CreateBranchAction` | Cannot create branches via high-level API |
| `CreateTagAction` | Cannot create tags via high-level API |
| `FastForwardAction` | Cannot fast-forward refs via high-level API |
| High-level Table helpers | No ergonomic WAP workflow API |

**Key files:**
- `manage_snapshots.rs` - existing ManageSnapshotsAction
- `expire_snapshots/retention.rs` - ref-safety logic (already correct)
- `table_update.rs` - SetSnapshotRef update type

**Deliverables:**

1. **Create low-level actions:**
   - `CreateBranchAction` - create branch pointing to snapshot
   - `CreateTagAction` - create tag pointing to snapshot
   - `FastForwardAction` - update ref to match another ref's snapshot

2. **High-level Table helpers:**
   - `Table::create_branch(name, snapshot_id)`
   - `Table::create_tag(name, snapshot_id)`
   - `Table::fast_forward(target_ref, source_ref)`
   - `write_to_branch(ref)` builder pattern

3. **Ref-safety audit:**
   - Document that expire_snapshots already respects refs
   - Verify remove_orphan_files respects refs
   - Add tests proving ref-safety

**Acceptance criteria (strict):**
- Spark reads refs created/modified by Rust correctly.
- Maintenance actions do not delete snapshots referenced by branches/tags.
- `$refs` metadata table reflects expected state.

---

### WP6 -- Schema evolution interoperability suite **P1**
**Objective:** Ensure schema evolution works cross-engine.

**Deliverables:**
- Spark DDL -> Rust reads:
  - ADD COLUMN, RENAME COLUMN, DROP COLUMN
- Rust DDL -> Spark reads:
  - same operations where supported

**Acceptance criteria (strict):**
- Correct column resolution after rename/drop (field ID semantics preserved).
- Reads correct for historical snapshots with prior schema.
- Cross-engine read correctness validated by count + checksum.

---

### WP7 -- Time travel query syntax (DataFusion) **P1**
**Objective:** DataFusion SQL parity for time travel:
- `VERSION AS OF <snapshot_id>`
- `TIMESTAMP AS OF <timestamp>`

**Acceptance criteria:**
- Works in SELECT, JOIN, subquery/CTE.
- Resolves to correct snapshot and produces results equal to core API scan at that snapshot.
- Clear errors for invalid snapshot/timestamp.

---

### WP8 -- Incremental scan API ("changes between snapshots") **P1**
**Objective:** Provide CDC-friendly API that returns changed files between snapshots.

**Deliverables:**
- `incremental_scan(from,to)` returns:
  - added data files
  - removed data files
  - added/removed delete files (if applicable)
- Validate correctness across:
  - append commits
  - row-delta commits
  - rewrite/compaction commits

**Acceptance criteria:**
- Correct change sets for each commit type.
- Cross-engine validation via metadata tables/snapshot summaries where possible.

---

### WP9 -- Metadata tables: `$partitions` and `$entries` **P1**
**Objective:** Add high-value operational metadata tables missing from current set.

**Deliverables:**
- `$partitions`: partition-level summaries (file counts, record counts, bytes)
- `$entries`: low-level manifest entries view for debugging

**Acceptance criteria:**
- Schemas consistent and stable.
- Results match Spark expectations on the same table (at least for core fields).
- Performance:
  - `$partitions` does not require loading all row-level data; must operate from manifests.

---

### WP10 -- Operational CLI tool (`iceberg-rust-cli`) **P1/P2**
**Objective:** Provide a production-friendly interface for maintenance without embedding DataFusion SQL.

**Deliverables:**
- CLI commands for:
  - rewrite_data_files
  - rewrite_manifests
  - expire_snapshots (dry-run + delete)
  - remove_orphan_files (dry-run + delete)
  - rewrite_position_delete_files
- Output formats:
  - human-readable summary
  - JSON output for orchestration systems

**Acceptance criteria:**
- Works in CI using the same harness tables.
- Supports dry-run and explicit opt-in for destructive actions.
- Returns non-zero exit code on partial failures.

---

### WP11 -- Manifest cache benchmarking and tuning **P1**
**Objective:** Validate cache effectiveness and tune defaults.

**Deliverables:**
- Benchmark profiles:
  - small, medium, large metadata sizes
- Measure:
  - planning latency p50/p95
  - cache hit ratio
  - memory footprint
  - invalidation correctness on commit

**Acceptance criteria:**
- Demonstrate measurable planning improvement with cache on "medium" and "large" profiles.
- No stale reads after commits (cache invalidation correct).
- Document recommended default properties for production.

---

## 6) Recommended execution order (dependency-aware)

**P0 first (hard gates):**
0) WP0 cross-engine test infrastructure (prerequisite for all interop tests)
1) WP1 P0 interop tests (Rust -> Spark) for existing ops (breadth-first smoke)
2) WP1.5 semantic parity validation (metadata + edge cases) - validates existing implementations
3) WP3.1 interop: Spark equality deletes -> Rust reads
4) WP3.2 memory bounds validation + guard for equality delete application
5) WP2 expire snapshots file deletion (dry-run + opt-in)
6) WP1.10 concurrency tests (append vs append, rewrite vs append, RowDelta vs append)

**Then P1:**
7) WP4 INSERT OVERWRITE dynamic -> static
8) WP5 WAP helpers + ref-safety audit
9) WP6 schema evolution interop
10) WP7 time travel SQL syntax
11) WP8 incremental scan API
12) WP9 metadata tables ($partitions, $entries)
13) WP11 cache benchmarking
14) WP10 CLI (can be parallel once action APIs stable)

---

## 7) Definition of Done (release gates)

**P0 parity gates**
- WP0 cross-engine test infrastructure enables Spark validation from Rust tests
- WP1 P0 interop tests pass in CI (Rust -> Spark reads)
- WP1.5 semantic parity validation confirms metadata + edge case behavior matches Spark
- WP2 supports safe file deletion with dry-run and ref-awareness
- WP3 proves equality delete read correctness + memory-bounded delete application
- WP1.10 concurrency suite passes (no corruption, clean conflicts)

**P1 parity gates**
- INSERT OVERWRITE dynamic (and ideally static) verified with Spark
- WAP helpers exist and maintenance respects refs
- Schema evolution interop suite passes
- Time travel SQL syntax supported
- Incremental scan API available and validated
- `$partitions` and `$entries` available (if required for ops)
- Manifest cache benchmark results documented with recommended defaults
- CLI available (if chosen as part of parity deliverable)

---

## 8) Upstreaming strategy
Prefer incremental upstreaming:
- Each WP is broken into PR-sized issues with tests
- Avoid large monolithic merges
- Keep fork viable, but reduce long-term maintenance cost by upstreaming early and often
