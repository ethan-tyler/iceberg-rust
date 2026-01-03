# Iceberg-Rust Production Parity Hardening Comprehensive Plan

Version: 1.0
Date: 2025-12-31
Baseline: main @ 0390e2e6
Target parity: Iceberg-Java semantics via Spark actions and metadata expectations
Source plan: docs/plans/2025-12-21-iceberg-rust-production-parity-hardening.md

---

## Executive Summary

This plan closes the remaining gaps identified in the parity hardening audit and raises the implementation to a best-in-class, production-ready standard. The focus is on verified bidirectional interoperability, strict semantic parity enforcement, operational safety for destructive maintenance, and clear CLI and benchmarking guarantees.

Key outcomes targeted:
- Enforced semantic parity (tests fail on unexpected divergences).
- Complete interop coverage (including Rust equality deletes -> Spark reads).
- Safety and ref-awareness in destructive maintenance (expire snapshots and remove orphan files) verified by tests.
- Operational CLI aligned to documented behavior and integration tests.
- Manifest cache benchmarking with clear, reproducible results and tuned defaults.

---

## Objectives and Success Criteria

Primary objectives align to the production parity hardening plan and add explicit quality gates.

1) Parity correctness
- Spark reads all Rust-written tables for all supported operations.
- Metadata parity is enforced and documented (snapshot summaries, manifest entry structure, edge cases).

2) Safety and reliability
- No OOM or unbounded memory growth on equality delete application.
- Expire snapshots and remove orphan files never delete data referenced by refs.
- Dry-run results match execution results for destructive maintenance.

3) Operational readiness
- CLI commands support required flags, error codes, JSON outputs, and dry-run modes.
- Benchmark results are reproducible with clear recommended defaults.

4) Professional engineering standards
- Tests are deterministic and provide clear failures.
- Docs capture decisions, results, and known limitations.
- CI tiers are defined with repeatable evidence.

---

## Scope and Non-Goals

In scope:
- All WP0 through WP11 gaps, issues, and hardening tasks.
- Tests and documentation updates required to enforce parity gates.
- Benchmarking and recommended default configuration.

Out of scope:
- New features beyond the parity plan.
- Large refactors unrelated to the listed gaps.
- Changes to upstream Iceberg-Java behavior.

---

## Current State Snapshot (Audit Evidence)

Completed evidence on main:
- Target branch confirmed: `main` (Decision Log).
- WP0 cross-engine infrastructure verified (`validate.py`, `spark_validator.rs`, Docker harness).
- WP1 interop cross-engine tests pass: `partition_evolution_crossengine.rs` (17 tests).
- WP1.5 semantic parity tests pass but do not fail on mismatches.
- WP1.10 concurrency tests pass.
- WP3.2 equality delete memory bounds tests pass.
- WP4 INSERT OVERWRITE (dynamic + static) cross-engine tests pass.
- WP5 WAP action tests pass (create branch/tag/fast-forward).
- WP6 schema evolution cross-engine tests pass.
- WP7 time travel SQL tests pass.
- WP8 incremental scan unit tests pass.
- WP9 entries/partitions unit tests pass.
- WP11 benchmark sweep documented with recommendations; large profile reaches 100% hit rate at 160MB+ cache; default aligned to 160MB.

Branch-specific updates (feature/semantic-parity-hardening @ b1735ad0):
- G1 closed: parity tests enforce allowlist assertions (`crates/integration_tests/tests/shared_tests/parity_utils.rs`) and findings doc complete (`docs/parity/semantic-parity-findings.md`).
- G2 closed for integration contract: CLI wired to maintenance actions with catalog flags and JSON/human outputs (`crates/iceberg-cli/src/cli.rs`, `crates/iceberg-cli/src/table_ref.rs`) and CLI tests/provisioning aligned (`crates/integration_tests/tests/shared_tests/cli_test.rs`, `crates/integration_tests/testdata/spark/provision.py`).
- G3 closed: Rust equality delete -> Spark read interop test added (`crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`).
- G5 closed: ref-safety + dry-run fidelity tests added (`crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`).
- G4 closed: delete predicate binding honors case sensitivity and scan predicate binding uses configured case sensitivity (`crates/iceberg/src/arrow/delete_filter.rs`, `crates/iceberg/src/scan/mod.rs`).
- G6 closed: full validations added to delete/update/merge/rewrite-manifests interop tests (`crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`).
- G7 closed: Spark parity checks added for `$partitions` and `$entries` (`crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`).
- G8 closed: default manifest cache size aligned to 160MB and docs updated (`docs/performance/manifest-cache-benchmark-results.md`).
- Tests run: `cargo test -p iceberg test_case_insensitive_equality_delete_binding`, `cargo test -p iceberg test_equality_delete_load_failure_notifies`, `cargo test -p iceberg test_scan_case_sensitive_binding`, `cargo test -p iceberg-integration-tests partition_evolution_crossengine`.
- Evidence: local workspace changes only (no commit/PR yet).

Known failing area on main:
- WP10 CLI integration tests fail due to flag mismatch (`--catalog-type` expected by tests, not in CLI) and output/exit-code differences.

---

## Gap Inventory (Prioritized)

| Gap ID | Gap | Impact | Evidence | Priority | Status |
| --- | --- | --- | --- | --- | --- |
| G1 | Semantic parity tests log divergences instead of failing; findings doc incomplete | P0 parity gate not met | `crates/integration_tests/tests/shared_tests/parity_utils.rs`, `docs/parity/semantic-parity-findings.md` | P0 | Closed (feature/semantic-parity-hardening @ b1735ad0) |
| G2 | CLI contract mismatched; tests fail; exit codes inconsistent | Operational readiness gate not met | `crates/iceberg-cli/src/cli.rs`, `crates/iceberg-cli/src/table_ref.rs`, `crates/integration_tests/tests/shared_tests/cli_test.rs`, `crates/integration_tests/testdata/spark/provision.py` | P1 | Closed (feature/semantic-parity-hardening @ b1735ad0) |
| G3 | Rust equality deletes -> Spark interop missing | Bidirectional equality delete parity incomplete | `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs` (test_crossengine_rust_equality_delete_spark_reads) | P1 | Closed (feature/semantic-parity-hardening @ b1735ad0) |
| G4 | Case-insensitive delete predicate binding TODO | Correctness risk in mixed-case schemas | `crates/iceberg/src/arrow/delete_filter.rs:144` | P2 | Closed |
| G5 | Ref-safety and dry-run fidelity tests missing for expire/remove orphan files | Destructive safety not fully verified | `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs` | P0 | Closed (feature/semantic-parity-hardening @ b1735ad0) |
| G6 | Interop validations lack checksum/min-max in most cases | Reduced confidence for subtle data issues | `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs` | P1 | Closed |
| G7 | $partitions/$entries lack Spark parity validation | Metadata table parity not proven | `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs` | P1 | Closed |
| G8 | Default manifest cache size aligned to 160MB with updated guidance | Performance risk for large-table workloads | `docs/performance/manifest-cache-benchmark-results.md` | P2 | Closed |
| G9 | Target branch confirmed as `main`; drift risk closed if execution stays on main | Execution alignment | Decision Log | N/A | Closed |

---

## Pre-Execution Checklist

- Confirm target branch is `main` and record commit hash.
- Ensure Docker environment is available for integration tests (`crates/integration_tests/testdata`).
- Verify toolchain matches `rust-toolchain.toml`.
- Run P0 gate commands on the starting commit to establish baseline.

## Workstreams and Detailed Plan

### WS0: Branch Alignment and Audit Reproducibility (P0)
Objective: ensure parity plan is executed on the correct target branch and all evidence is reproducible.

Deliverables:
- Target branch confirmed: `main`; re-run key tests on the baseline commit for reproducibility.
- Record commit hashes and toolchain versions.

Acceptance criteria:
- Audit results reproduced on the target branch with the same test commands.
- Evidence recorded in a stable location (log excerpts or CI artifacts).

Validation:
- `git rev-parse HEAD`
- Re-run core P0 tests listed in the Validation Plan section.

---

### WS1: Semantic Parity Enforcement (WP1.5) (P0)
Objective: convert semantic parity from observational to enforcement with documented expected divergences.

Status (feature/semantic-parity-hardening @ b1735ad0): complete; allowlist assertions in `crates/integration_tests/tests/shared_tests/parity_utils.rs` and findings in `docs/parity/semantic-parity-findings.md`.

Deliverables:
- Replace divergence logging with assertions; allowlist only expected divergences.
- Populate `docs/parity/semantic-parity-findings.md` with actual results.
- Add error-rejection parity tests (invalid schema changes, incompatible type promotions).
- Add missing edge cases: zero-length binary, three-valued logic.

Acceptance criteria:
- Parity tests fail on unexpected divergences.
- Findings doc is fully populated, with resolved and documented divergences.
- Error rejection parity is covered for at least two invalid operations.

Validation:
- `cargo test -p iceberg-integration-tests semantic_parity_test -- --nocapture`
- Code review confirms divergences trigger assertions (no println-only success path).
- Add a controlled negative case or explicit assertion on `parity_result.semantic_parity` to guarantee failure on mismatch.

---

### WS2: Interop Validation Hardening (WP1) (P1)
Objective: strengthen Rust->Spark validations with deterministic checksums or min/max bounds.

Status (feature/semantic-parity-hardening @ b1735ad0): full validations added to delete/update/merge/rewrite-manifests interop tests in `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`.

Deliverables:
- Add checksum or min/max validation to each WP1 interop test.
- Ensure deterministic Spark settings are applied in validation and provisioning.

Acceptance criteria:
- Each interop test validates at least two of: count, distinct count, checksum, min/max bounds.
- Tests remain deterministic (no flaky results).

Validation:
- `cargo test -p iceberg-integration-tests partition_evolution_crossengine`

---

### WS3: Destructive Operations Safety (WP2 + remove orphan files) (P0)
Objective: verify ref-awareness and dry-run fidelity for expire snapshots and remove orphan files (destructive operations).
Scope: ref safety and dry-run fidelity for maintenance actions only (Spark ref interop is handled in WS6).

Status (feature/semantic-parity-hardening @ b1735ad0): tests added in `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`.

Deliverables:
- Add tests that create branches/tags, run expire/remove, and assert ref-protected snapshots/files remain.
- Add dry-run vs execution comparison tests (plan should match executed deletions).

Acceptance criteria:
- No refs are deleted or invalidated by expire/remove actions.
- Dry-run output matches actual deleted files and counts.

Validation:
- New integration tests under `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs` or dedicated tests.

---

### WS4: Equality Deletes Full Interop (WP3.3 + WP3.4) (P1/P2)
Objective: complete bidirectional equality delete parity and address case-insensitive binding TODO.

Deliverables:
- Add Rust equality delete write -> Spark read interop test.
- Implement case-insensitive binding in delete predicate processing.

Acceptance criteria:
- Spark reads tables with Rust-written equality deletes and matches expected results.
- Case-insensitive columns behave consistently with Spark for equality delete application.

Validation:
- New cross-engine tests in `partition_evolution_crossengine.rs`.
- Unit tests for case-insensitive binding behavior.

---

### WS5: Operational CLI Completion (WP10) (P1)
Objective: align CLI behavior with test expectations and operational requirements.

Status (feature/semantic-parity-hardening @ b1735ad0): integration contract complete; CLI wiring + outputs updated in `crates/iceberg-cli/src/cli.rs` and tests/provisioning aligned in `crates/integration_tests/tests/shared_tests/cli_test.rs` and `crates/integration_tests/testdata/spark/provision.py`.

Deliverables:
- Align CLI to tests (tests are the spec); add `--catalog-type` flag (or backward-compatible alias) and required mode flags with mutual exclusivity.
- Ensure correct exit codes for error conditions (1 for validation errors).
- Implement JSON and human-readable outputs for all commands.
- Update CLI help text and user-facing docs to reflect the new flags.

Acceptance criteria:
- All CLI integration tests pass.
- CLI returns stable, documented outputs.

Validation:
- `cargo test -p iceberg-integration-tests cli_test`

---

### WS6: WAP + Ref Interop (WP5) (P1)
Objective: ensure Spark can read refs created by Rust and that ref visibility/fast-forward semantics are consistent.

Deliverables:
- Add Spark interop tests for branch/tag creation and fast-forward.
- Add read-from-ref validations (Spark queries against branch/tag snapshots).

Acceptance criteria:
- Spark can read branch/tag refs created by Rust.
- Fast-forward updates are visible in Spark and read results match expected snapshots.

Validation:
- New cross-engine tests in integration suite.

---

### WS7: Metadata Table Parity ($partitions/$entries) (WP9) (P1)
Objective: verify metadata tables match Spark expectations.

Deliverables:
- Add Spark parity checks for `$partitions` and `$entries`.
- Validate schema shape and core field values.

Acceptance criteria:
- Spark and Rust return matching schemas and core fields for both tables.
- `$partitions` remains manifest-driven (no data file scanning).

Validation:
- Integration tests comparing Spark vs Rust outputs.

---

### WS8: Manifest Cache Default Alignment (WP11) (P2)
Objective: align defaults and documentation with benchmark findings for large tables.

Deliverables:
- Update default manifest cache size (e.g., 160MB) or add explicit production guidance if defaults must remain conservative.
- Add configuration guidance to user-facing docs and table properties where applicable.
- Consider optional auto-tuning based on manifest count/size.

Acceptance criteria:
- Recommended cache size is reflected in defaults or documented prominently.
- Benchmark doc remains the source of truth for sizing and reproducibility.
- Large profile shows improved hit rate under the recommended configuration.

Validation:
- `cargo bench -p iceberg scan_planning_benchmark`
- Review documentation updates that surface recommended settings.

---

## Execution Orchestration (OhMyOpenCode + Ultrawork)

Execution goal: maximize throughput while preserving auditability and correctness. Each workstream is run as an ultrawork task with explicit evidence capture and verification gates.

Ultrawork task card template:

```
Workstream:
Goal:
Scope:
Evidence to collect (files/tests/logs):
Atomic tasks:
Acceptance criteria:
Validation commands:
Risks/mitigations:
Owner:
ETA:
```

Agent usage (OhMyOpenCode plugin):
- Use `explore` agents in parallel for codebase discovery (tests, call sites, config) before edits.
- Use `librarian` agents only when external specs/examples are required; otherwise stay local.
- Use `code-architect` for workstreams that change behavior or add new tests/interop flows.
- Use `code-reviewer` after significant changes to validate safety and regressions.

Execution cadence:
1) Kickoff: run Pre-Execution Checklist, confirm baseline commit, and capture initial test status.
2) For each workstream: create an ultrawork task card, run discovery, implement changes, run validations, and update plan status.
3) Close-out: record test results and link evidence in the plan or PR description.

Evidence discipline:
- Every acceptance criterion must map to a test run or explicit code evidence.
- Destructive operations require both dry-run and execution evidence.
- Cross-engine interop requires Spark validation output or test logs.

## Workstream Dependencies

WS0 is the required starting point for reproducibility and branch alignment. Dependencies:

WS0 ─────┬─────> WS1 ─────> WS2
         │
         ├─────> WS3 ─────> WS6
         │
         ├─────> WS7
         │
         └─────> WS4

WS5 (independent)
WS8 (independent)

## Validation Plan (Evidence and Commands)

P0 gate commands (must pass before release candidate):
- `cargo test -p iceberg-integration-tests partition_evolution_crossengine`
- `cargo test -p iceberg-integration-tests semantic_parity_test -- --nocapture`
- `cargo test -p iceberg concurrent`
- `cargo test -p iceberg equality_delete_memory_bounds`

P1 gate commands:
- `cargo test -p iceberg-integration-tests schema_evolution_crossengine`
- `cargo test -p iceberg-datafusion time_travel`
- `cargo test -p iceberg incremental_scan`
- `cargo test -p iceberg entries_table`
- `cargo test -p iceberg partitions_table`
- `cargo test -p iceberg-integration-tests cli_test`
- `cargo bench -p iceberg scan_planning_benchmark`

---

## CI and Release Gates

PR gate (fast):
- Semantic parity (subset), concurrency smoke, key interop tests, CLI argument parsing tests.

Nightly (thorough):
- Full cross-engine interop suite and semantic parity matrix.
- Equality delete stress tests.
- Schema evolution cross-engine.

Release candidate:
- Full matrix + benchmark thresholds + documentation updates.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Parity tests still allow silent divergence | Hidden spec mismatch | Enforce assertions and allowlist expected differences only |
| CLI remains mismatched | Operational failures | Align CLI flags and outputs to tests; add docs |
| Default cache size too small for large tables | Performance risk for large-table workloads | Update defaults or document recommended configuration |
| Target branch changes without updating plan | Invalidated audit | Update Decision Log and re-run baseline tests |
| Ref-safety regression | Data loss | Add explicit ref protection tests |

---

## Decision Log

| Date | Decision | Rationale | Owner |
| --- | --- | --- | --- |
| 2025-12-31 | Target branch confirmed: `main` | Execution and validation will remain on main | User |
| 2025-12-31 | CLI contract aligned to tests (Option A) | CLI is pre-release; tests define better spec | User |

## Effort Estimates (Initial)

| Workstream | Estimate | Dependencies |
| --- | --- | --- |
| WS0 | 0.5 days | None |
| WS1 | 2-3 days | WS0 |
| WS2 | 1-2 days | WS1 |
| WS3 | 2-3 days | WS0 |
| WS4 | 2-3 days | WS0 |
| WS5 | 2-3 days | None |
| WS6 | 1-2 days | WS3 |
| WS7 | 1-2 days | WS0 |
| WS8 | 0.5 days | None |

## Milestones (Suggested)

Milestone 1 (P0 gates): WS0, WS1, WS3
Milestone 2 (P1 parity completion): WS2, WS4, WS5, WS6, WS7
Milestone 3 (Performance + defaults alignment): WS8 + documentation sync

---

## Definition of Done

P0 completion:
- WP1.5 parity enforced and documented.
- WP2 safety and ref-awareness verified.
- WP3.1 and WP3.2 complete with tests.
- WP1.10 concurrency suite passes.

P1 completion:
- CLI passes integration tests and meets output contracts.
- Metadata tables parity validated with Spark.
- Manifest cache default updated or documented with recommended production guidance.
- All acceptance criteria in the plan doc are backed by test evidence.

---

## Appendix: Evidence References

- Cross-engine interop tests: `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs`
- Semantic parity tests: `crates/integration_tests/tests/shared_tests/semantic_parity_test.rs`
- Semantic parity findings: `docs/parity/semantic-parity-findings.md`
- Equality delete memory bounds tests: `crates/iceberg/src/arrow/caching_delete_file_loader.rs`
- Concurrency tests: `crates/iceberg/src/transaction/concurrent.rs`
- Schema evolution cross-engine tests: `crates/integration_tests/tests/shared_tests/schema_evolution_crossengine.rs`
- Time travel tests: `crates/integrations/datafusion/tests/integration_datafusion_test.rs`
- Incremental scan: `crates/iceberg/src/scan/incremental.rs`
- Metadata tables: `crates/iceberg/src/inspect/entries.rs`, `crates/iceberg/src/inspect/partitions.rs`
- CLI tests: `crates/integration_tests/tests/shared_tests/cli_test.rs`
- Manifest cache benchmark: `crates/iceberg/benches/scan_planning_benchmark.rs`

### Test File Conventions

- Ref-safety destructive maintenance: `crates/integration_tests/tests/shared_tests/ref_safety_test.rs`
- Equality delete interop: add to `crates/integration_tests/tests/shared_tests/partition_evolution_crossengine.rs` or new `crates/integration_tests/tests/shared_tests/equality_delete_interop_test.rs`
- WAP ref interop: `crates/integration_tests/tests/shared_tests/ref_interop_test.rs`
- CLI: extend `crates/integration_tests/tests/shared_tests/cli_test.rs`
