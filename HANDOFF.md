# HANDOFF.md — bring-up brief for the next agent or engineer

**Read this first, then `STATE.md` and `PLAN.md`.** Last updated 2026-11-10.

---

## 1. What this workspace is

A working **Phase 1 implementation of the Ledbetter Data Pump (LDP)**: a Rust,
YAML-driven data movement engine that replaces the SSIS data flow. It moves
data from Postgres into Parquet/CSV over partitioned parallel workers, carrying
Apache Arrow batches, with atomic checkpointing so a killed job resumes without
duplicating or losing rows.

The specification is in `docs/`. The product is in `ldp/`. Everything else is
project scaffolding.

```
docs/    the contract (read-only reference; never edit)
ldp/     the Rust workspace: 6 crates + 1 integration test
examples/ 6 runnable pipeline YAML files
scripts/ build, test, pg fixture, cargo wrapper, DoD harness, log checker
```

## 2. Current status — in one paragraph

**Phase 1 is complete and verified.** `.\scripts\test.ps1` passes: 65 unit
tests, 3 integration tests against a real Postgres, `clippy` clean, and
`scripts/verify_dod.py` reports **39/39 checks** covering every item of the
contract's Definition of Done. A real 1,000,000-row Postgres → Parquet run takes
~5 s on a debug build (~190k rows/s, 4 threads, 8 partitions), and a process
killed mid-run resumes to exactly 1,000,000 distinct rows. Every run also
recounts its own output and fails rather than reporting a short write.

## 3. How to run it (the only commands you need)

```powershell
.\scripts\pg.ps1 start          # portable Postgres on 127.0.0.1:55432 (test fixture)
.\scripts\pg.ps1 seed           # 1,000,000-row customers table + a tiny empty_table
.\scripts\build.ps1             # builds ldp\target\debug\ldp.exe

$env:LDP_PG_URL = "postgres://postgres@127.0.0.1:55432/ldp"
.\ldp\target\debug\ldp.exe validate examples\01_simple_copy.yaml
.\ldp\target\debug\ldp.exe run      examples\01_simple_copy.yaml

.\scripts\test.ps1              # unit + integration + clippy + DoD (full gate)
.\scripts\test.ps1 -Fast        # unit + clippy only (no database)
```

Output lands in `out\<pipeline>\` (Parquet, one file per batch) or at the CSV
path named in the YAML. State files land in `.ldp\<pipeline>.json`.

The three Postgres integration tests create and drop a shared table (`ldp_it`),
so they must run serialised. `scripts\test.ps1` sets `RUST_TEST_THREADS=1` for
you; if you run `cargo test` by hand with `LDP_TEST_PG_URL` exported, do the
same or the tests race each other.

## 4. What is verified, and how

| Contract DoD | Evidence |
|---|---|
| 1. Postgres → Parquet, range partitioned | `out/simple_copy/` = 1,000,000 rows, 1,000,000 distinct ids, 8 chunks, `verified_rows` = 1,000,000 |
| 2. filter + derive | `out/filter_and_derive/` = 900,000 rows, equal to the same predicate executed in SQL |
| 3. restart skips completed chunks | `LDP_FAIL_AFTER_CHUNKS=3` → exit 2 with 3 chunks; resume skips 3, total still 1,000,000 distinct |
| — real kill test | process killed at 2/8 chunks (one `.tmp` orphan); resume → 1,000,000 distinct, no temp debris |
| 4. empty result | `examples/05_empty_source.yaml` → exit 0, one readable empty Parquet file |
| 5. batch limits | `examples/06_small_batches.yaml` (20k rows / 1 MB) → 96 files, still 1,000,000 distinct |
| 6. atomic checkpoint | `state.json.tmp` + rename; no sidecar left behind (unit + DoD check) |
| 7. semantic validation | 4 invalid pipelines → exit 1, all problems reported together |
| 8. structured JSON logs | `scripts/check_logs.py` validates 18 lines against the exact schema |
| 9. integration test incl. restart | `ldp/ldp-cli/tests/integration_postgres.rs`, 3 tests, real server |
| 10. test + clippy clean | 65 unit + 3 integration pass; `clippy` clean |
| — output integrity | every run recounts its published rows (`verified_rows`); a mock sink that under-reports is a permanent failing test |

## 5. Decisions that will look like mistakes unless you read them

Each is argued in `PLAN.md` §5 ("deviations register"). The short version:

1. **`Source::read_partition` returns a stream, not one `DataChunk`.** The
   contract's trait cannot enforce `max_batch_rows`/`max_batch_bytes` (DoD #5)
   if a connector must materialise a whole partition first.
2. **`ExecutionPlan`/`Stage` live in `ldp-core`, not `ldp-config`.** Otherwise
   `ldp-core` would depend on `ldp-config`, inverting the contract's own
   dependency direction.
3. **Phase 1 is one linear transformation chain, and all targets share one
   input.** Fan-out and per-branch targets need Phase 2 conditional split;
   the validator says so explicitly instead of half-working.
4. **Parquet writes one file per batch**: `part-<chunk:05>-<batch:05>.parquet`.
   Naming by chunk alone silently lost 524,288 rows during bring-up because
   later batches overwrote earlier files.
5. **CSV declares `resumable() == false`.** A single output file cannot skip
   chunks without dropping their rows, so the runner replays everything and
   rewrites the file atomically. Parquet can resume because its filenames are
   deterministic per (chunk, batch).
6. **`numeric` is cast to text and rebuilt as `Decimal128(38, sampled_scale)`.**
   `tokio-postgres` has no numeric decoder without an extra feature; sampling
   keeps the contract's "no silent truncation" rule and fails loudly instead of
   rounding.
7. **No TLS in Phase 1.** Local/plaintext connections only; TLS is a Phase 2
   item because it is the one place where an extra native dependency
   (`ring`/`aws-lc`) would enter the build.
8. **`ldp validate` exists** in addition to the required `run`. It is what makes
   DoD #7 testable without a database.
9. **Every run self-verifies its output.** `Sink::verify_rows()` recounts rows
   from the published files (Parquet footers, CSV re-parse) and the runner fails
   with exit 2 when that disagrees with its own tally. This is not paranoia: the
   original filename bug reported `status: success` while writing 56% of the
   rows, and only an external reader noticed. The `job_summary` line now carries
   `verified_rows` per target.

## 6. Traps that cost time here

Read `LESSONS.md` before touching the toolchain; the five that bite hardest:

* **Bare `cargo` fails.** Rust is `x86_64-pc-windows-gnu`; its linker is MinGW
  from Strawberry Perl. Always use `scripts\cargo.ps1` (it fixes `PATH`).
* **`pg_ctl` must be started with `Start-Process` + file redirection.** It
  spawns a daemon that inherits the console handle, so capturing its output in
  PowerShell hangs forever. `scripts/pg.ps1` already does this.
* **`psql` NOTICEs go to stderr**; with `$ErrorActionPreference='Stop'` a piped
  NOTICE aborts the whole script mid-way. Run SQL from `-f file`.
* **PowerShell `>` writes UTF-16.** Redirect JSONL logs with
  `| Set-Content -Encoding utf8`.
* **Never round-trip source files through `Get-Content`/`Set-Content`** — it
  double-encodes UTF-8 and turns `→` into `â†’`. Use the edit tools.

## 7. What is *not* done (deliberately — Phase 2+)

Full DLQ + error budgets; schema policy (strict/lenient); multi-target partial
failure policy; watermarks/incremental loads; graceful shutdown/cancellation;
concurrent-run locking; atomic staging/swap and `merge`; Snowflake/SQL Server/
Oracle connectors; `IN` lists and string functions; conditional split; TLS;
Python bindings; spill-to-disk.

The contract is explicit that these must not be half-implemented in Phase 1.
`PLAN.md` §4 lists them with their acceptance criteria.

## 8. Where to start if you are asked to continue

1. `.\scripts\test.ps1` — confirm the baseline is green before changing anything.
2. Read `STATE.md`'s "next actions".
3. The highest-value next piece is **Phase 2 hardening**, and within it the
   **DLQ + error budgets**, because row isolation currently *drops* bad rows
   and only counts them — an operator cannot see *which* rows were discarded.
   That is the single biggest gap between "works" and "safe to leave running".
4. Keep the phase gate discipline: no Phase 2 feature lands without its own
   test in `verify_dod.py` or `integration_postgres.rs`.
