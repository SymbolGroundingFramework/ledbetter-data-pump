# Phase 1 checklist

Executable form: `scripts/verify_dod.py` (37 checks). This file is the human
view. All items verified 2026-11-10 on this machine.

## Contract Definition of Done

- [x] **1.** `ldp run pipeline.yaml` executes a Postgres → Parquet job with
      range partitioning.
      → 8 partitions, 1,000,000 rows, 1,000,000 distinct ids, ~5 s (debug).
- [x] **2.** filter + derive work using the Phase 1 expression subset, vectorized
      where possible (column-wise evaluation; row-wise only on error).
      → 900,000 rows, equal to the same predicate in SQL.
- [x] **3.** Restarting a failed job skips completed chunks (verified by killing
      the process mid-run).
      → deterministic abort at 3/8 chunks → resume skips 3, total still exact;
      real `Stop-Process -Force` at 2/8 chunks → resume to 1,000,000 distinct.
- [x] **4.** Empty source or fully-filtered result → success, 0 rows, valid
      (possibly empty) Parquet file.
- [x] **5.** `max_batch_rows` / `max_batch_bytes` are enforced.
      → 20k rows / 1 MB limits produced 96 files with zero row loss.
- [x] **6.** Atomic checkpoint writes (tmp + rename).
      → unit test + no `.tmp` sidecar after any run.
- [x] **6b.** Output integrity: every run recounts the rows it published
      (Parquet footers, CSV re-parse) and exits 2 on a mismatch.
      → `verified_rows` = 1,000,000 on the full run; a mock sink that
      under-reports fails the run in `runner.rs` tests.
- [x] **7.** Semantic validation rejects invalid YAML before any query.
      → 4 invalid pipelines, exit 1, all errors reported together.
- [x] **8.** Structured JSON logs for chunk start/complete and final summary.
      → `scripts/check_logs.py`: 18/18 lines match the specified schema.
- [x] **9.** At least one integration test with a real Postgres, including a
      restart test. → `ldp/ldp-cli/tests/integration_postgres.rs`, 3 tests.
- [x] **10.** `cargo test` and `cargo clippy` pass cleanly.
      → 65 unit + 3 integration pass; `clippy` clean.

## Addendum edge cases required in Phase 1

- [x] Expression isolation (basic): bad rows are dropped and counted, not fatal.
- [x] Partition fallback: `partitioning.type: none`, `chunks: 1`; empty range →
      zero partitions.
- [x] Source retries: 3 attempts with exponential backoff per partition.
- [x] Atomic file writes: `*.tmp` then rename; failed runs publish nothing.
- [x] Empty-result handling: zero-row batch is valid; sinks finalize cleanly.
- [x] Atomic checkpoint writes + corruption/foreign-state guards.
- [x] All-at-once semantic YAML validation.
- [x] Basic memory limits.

## Operational details

- [x] Range-partition algorithm exactly as specified (MIN/MAX, integer division,
      half-open ranges), non-integer columns rejected at planning time.
- [x] Log event schema (`chunk_started`, `chunk_completed`, `job_summary`,
      `error`).
- [x] CLI contract: `run`, `--threads`, `--state-file`, `--log-level`; exit
      codes 0/1/2.
- [x] Connection strings: `env://VAR` preferred, plain `postgres://` accepted,
      passwords redacted everywhere, missing env var → exit 1.
- [x] Minimal guaranteed-working example runs (example 01 + 02).

## Deliberately deferred (Phase 2+)

- [ ] Full DLQ + error budgets
- [ ] Schema policy (strict/lenient)
- [ ] Multi-target partial-failure policy
- [ ] Watermarks / incremental loads
- [ ] Graceful shutdown and cancellation
- [ ] Concurrent-run protection
- [ ] Atomic staging/swap and merge
- [ ] Snowflake / SQL Server / Oracle connectors
- [ ] `IN` lists, string functions, conditional split
- [ ] TLS
- [ ] Python bindings
