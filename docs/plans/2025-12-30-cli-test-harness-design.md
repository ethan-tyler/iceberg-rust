# CLI Test Harness Reliability Design

## Goal

Make `crates/integration_tests/tests/shared_tests/cli_test.rs` reliable and fast by removing per-test `cargo run`, implementing a safe timeout without double-waiting, and adding a minimal, deterministic unit test to prove the exit-code mapping for partial failures. The design should keep CI stable, avoid flaky process handling, and preserve local developer ergonomics.

## Approach

The CLI tests will resolve the CLI binary in a deterministic, CI-friendly way. The harness will first check an environment override (`ICEBERG_CLI_BIN`). If set, it will run that path directly. Otherwise, it will derive the repository root from `CARGO_MANIFEST_DIR`, look for `target/debug/iceberg-cli` (with `EXE_SUFFIX` for Windows), and build it once using `cargo build -p iceberg-cli` guarded by a `Once`. This avoids per-test `cargo run` contention and keeps the build deterministic. The resolved path will be cached for reuse by all tests in the process.

For timeouts, the harness will avoid `wait-timeout` and use a polling loop with `child.try_wait()` plus a short sleep interval. On timeout, the child will be killed and then reaped via `child.wait()` to avoid zombies. On success, stdout and stderr will be read from the pipes after the process has exited; the implementation will not call `wait_with_output()` after a wait because that can double-wait on Unix.

To cover the “partial failure => exit code 2” requirement without brittle integration setup, a small helper in `crates/iceberg-cli/src/lib.rs` will map `partial_failure` to `ExitCode`. Unit tests in `crates/iceberg-cli/tests/` will assert the mapping for `true` and `false`, providing deterministic coverage.

## Non-Goals

- Simulating storage failures in integration tests.
- Changing CLI behavior or output schemas beyond the exit-code mapping helper.
