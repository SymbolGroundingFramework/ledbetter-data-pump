# STATE.md — live status

**As of 2026-11-10 19:20 (America/Chicago).** Prune this aggressively; move
anything durable into `LESSONS.md` or `PLAN.md`.

## Works (verified today, with evidence)

* `ldp run` executes Postgres → Parquet with range partitioning:
  1,000,000 rows, 1,000,000 distinct ids, 8 chunks, ~5 s (debug build,
  4 threads) — `scripts/verify_dod.py` DoD 1.
* `filter` + `derive` (including `coalesce`, `now`, decimal arithmetic):
  900,000 rows, identical to the same predicate run as SQL — DoD 2.
* Restart: `LDP_FAIL_AFTER_CHUNKS=3` → exit 2 with 3 checkpointed chunks;
  resume skips 3 and still yields 1,000,000 distinct rows — DoD 3.
* Real kill: process killed at 2/8 chunks (left one `.tmp`); resume completed
  to 1,000,000 distinct rows with no temp debris — DoD 3 (kill variant).
* Empty source: 0 partitions → exit 0, one valid empty Parquet file — DoD 4.
* Batch limits: `20,000 rows / 1 MB` produced 96 files with zero row loss —
  DoD 5.
* Atomic checkpoint + atomic file publish; no `.tmp` sidecars left — DoD 6.
* Validation: 4 invalid pipelines → exit 1, all problems reported at once — DoD 7.
* Flat JSON logs conform to the specified schema (18/18 lines) — DoD 8.
* **Every run self-verifies its output**: the engine recounts rows from the
  published files (Parquet footers / CSV re-parse) and fails with exit 2 if the
  count disagrees with what it wrote. Proven by a mock sink that under-reports,
  and checked on every real run (`verified_rows` in `job_summary`).
* 3 integration tests against real Postgres, including resume — DoD 9.
* 65 unit tests + `clippy` clean — DoD 10.

Full gate: `.\scripts\test.ps1` → **39/39 DoD checks, ALL GATES PASSED**.

## Environment state

* Portable Postgres 17.2 running on `127.0.0.1:55432`, database `ldp`, seeded
  with 1,000,000 `customers` rows and an empty `empty_table`.
* `ldp\target\debug\ldp.exe` built. Release profile configured but not built.
* Rust 1.99.1 `x86_64-pc-windows-gnu`; MinGW linker from Strawberry Perl.

## Open oddities / small debts

* Writer task is the throughput bottleneck under zstd (see `PLAN.md` §6).
* `rows_read` on a failed run over-counts (in-flight batches are discarded).
* Output columns are all declared nullable.
* `scripts/pg.ps1 start` logs a harmless "another server might be running"
  message when `pg_ctl` finds a stale `postmaster.pid`.

## Publication status (2026-11-10)

Repo is **publishable as a v0.1.0 preview**: `LICENSE` (MIT), `CHANGELOG.md`,
`CONTRIBUTING.md`, `docs/README.md` index, `.github/workflows/ci.yml`
(fmt + clippy + unit tests + integration tests against a Postgres service), and
the tree is `cargo fmt` clean. `README.md` now states the limits plainly — in
particular that **TLS is missing**, so cloud endpoints are not reachable yet.

Not published: no git repository initialised, no release binaries, no crates.io
metadata (`repository`/`homepage` still unset — fill in the GitHub URL first).

## Next actions (ordered)

1. Publish v0.1.0: `git init`, first commit, push to GitHub (no remote/identity
   configured on this machine yet), then fill `repository`/`homepage` in
   `ldp/Cargo.toml` and the release link in `CHANGELOG.md`.
2. **TLS** — the single biggest blocker to real use: without it Azure,
   Supabase and most managed endpoints are unreachable.
3. **DLQ + error budgets** — bad rows are currently dropped and counted, which
   is silent data loss from an operator's point of view.
4. Release build + a throughput baseline on 10M rows, then decide whether the
   single-writer design needs a perf pass.
5. Concurrent-run lock file (cheap, prevents a real footgun).
