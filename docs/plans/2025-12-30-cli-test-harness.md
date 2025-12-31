# CLI Test Harness Reliability Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make CLI integration tests fast and reliable by resolving the CLI binary once, implementing a safe timeout without double-waiting, and adding deterministic unit tests for exit-code mapping.

**Architecture:** Resolve the CLI binary via `ICEBERG_CLI_BIN` or a build-once fallback, run CLI commands via a timeout loop using `try_wait()`, and add a helper in `iceberg-cli` to map partial failures to exit codes with unit tests.

**Tech Stack:** Rust, std::process, std::sync, integration tests, unit tests.

---

### Task 1: Add failing unit tests for exit-code mapping

**Files:**
- Modify: `crates/iceberg-cli/src/lib.rs`
- Test: `crates/iceberg-cli/src/lib.rs`

**Step 1: Write the failing test**

Add a `#[cfg(test)]` module that asserts:

```rust
#[test]
fn exit_code_for_partial_failure_true_is_two() {
    assert_eq!(exit_code_for_partial_failure(true), ExitCode::from(2));
}

#[test]
fn exit_code_for_partial_failure_false_is_success() {
    assert_eq!(exit_code_for_partial_failure(false), ExitCode::SUCCESS);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg-cli exit_code_for_partial_failure`

Expected: FAIL (missing function or wrong behavior).

**Step 3: Write minimal implementation**

Add:

```rust
fn exit_code_for_partial_failure(partial_failure: bool) -> ExitCode {
    if partial_failure { ExitCode::from(2) } else { ExitCode::SUCCESS }
}
```

Replace direct `ExitCode::from(2)` usage in `expire-snapshots` and `remove-orphans` with this helper.

**Step 4: Run test to verify it passes**

Run: `cargo test -p iceberg-cli exit_code_for_partial_failure`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/iceberg-cli/src/lib.rs
git commit -m "test(cli): cover partial failure exit code mapping"
```

---

### Task 2: Add timeout helper tests (red)

**Files:**
- Modify: `crates/integration_tests/tests/shared_tests/cli_test.rs`
- Test: `crates/integration_tests/tests/shared_tests/cli_test.rs`

**Step 1: Write failing tests**

Add tests for a new helper `run_command_with_timeout`:

```rust
#[test]
fn run_command_with_timeout_succeeds() {
    let mut cmd = Command::new("cargo");
    cmd.arg("--version");
    let result = run_command_with_timeout(cmd, Duration::from_secs(10));
    assert!(result.success());
}
```

And a platform-specific timeout test that expects a panic:

```rust
#[cfg(unix)]
#[test]
#[should_panic(expected = "timed out")]
fn run_command_with_timeout_times_out_unix() {
    let mut cmd = Command::new("sh");
    cmd.args(["-c", "sleep 5"]);
    let _ = run_command_with_timeout(cmd, Duration::from_millis(100));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg-integration-tests run_command_with_timeout`

Expected: FAIL (helper missing).

---

### Task 3: Implement CLI binary resolution and timeout helper

**Files:**
- Modify: `crates/integration_tests/tests/shared_tests/cli_test.rs`
- Modify: `crates/integration_tests/Cargo.toml`

**Step 1: Implement binary resolution**

- Add `ICEBERG_CLI_BIN` override and `OnceLock<PathBuf>` caching.
- Derive repo root from `CARGO_MANIFEST_DIR`.
- Respect `CARGO_TARGET_DIR` when locating the binary.
- If binary missing, run `cargo build -p iceberg-cli --quiet` once and assert the binary exists.

**Step 2: Implement `run_command_with_timeout`**

- Use `child.try_wait()` in a loop with short sleep.
- On timeout: `kill`, `wait`, then panic with args.
- On success: read stdout/stderr from pipes into `CliResult`.

**Step 3: Replace CLI runner to use binary path + timeout helper**

- `base_command` should use resolved binary path.
- `run_with_timeout` should delegate to `run_command_with_timeout`.

**Step 4: Remove `wait-timeout` dependency**

Delete from `crates/integration_tests/Cargo.toml`.

**Step 5: Run tests to verify pass**

Run:
- `cargo test -p iceberg-integration-tests run_command_with_timeout`

Expected: PASS.

**Step 6: Commit**

```bash
git add crates/integration_tests/tests/shared_tests/cli_test.rs crates/integration_tests/Cargo.toml
git commit -m "test(cli): resolve binary once and add safe timeout"
```

---

### Task 4: Full targeted verification

**Step 1: Run CLI tests (optional if docker available)**

Run: `cargo test -p iceberg-integration-tests cli_test`

Expected: PASS (if docker is available).

**Step 2: Commit (if any changes)**

Only if new changes are introduced.
