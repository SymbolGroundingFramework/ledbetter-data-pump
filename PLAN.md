# PLAN.md — Ledbetter Data Pump

Strategy and rationale. Status lives in `STATE.md`; permanent conclusions in
`LESSONS.md`; the authoritative task list is `studio.db` (`plan.py show`).

## 1. Objective

Replace the SSIS data flow ("the data pump") with a declarative, restartable,
high-throughput engine: YAML job files, partitioned shared-nothing workers,
Apache Arrow in flight, atomic checkpointing, structured logs.

Phase 1 is the only committed scope. It is **complete and verified** (see
`STATE.md`). Phases 2+ are planned here but not started.

## 2. Architecture as built

```
pipeline.yaml
   │  ldp-config: strict YAML → semantic checks → ExecutionPlan
   ▼
ldp-core::runner
   │  partitions ──► N workers (read → transform) ──► bounded channel
   │                                                   │
   │                                    single writer task owns the sinks
   │                                                   │
   └── checkpoint (atomic JSON) ◄── after every sink accepts the chunk
                                   ▼
                     Parquet (one file per batch) / CSV (one file)
```

| Crate | Owns | Depends on |
|---|---|---|
| `ldp-arrow` | all SQL↔Arrow type mapping, decimal helpers, batch sizing | arrow |
| `ldp-core` | DataChunk, Partition, ExecutionPlan, traits, checkpoint, expression engine, transforms, worker pool, logger | ldp-arrow |
| `ldp-config` | YAML schema, semantic validation, plan construction | ldp-core |
| `ldp-postgres` | Postgres source: schema discovery, MIN/MAX planning, streaming reads, row→Arrow | ldp-core, ldp-arrow |
| `ldp-parquet` | Parquet sink: one file per batch, atomic publish | ldp-core |
| `ldp-csv` | CSV sink: single file, RFC 4180, atomic publish, non-resumable | ldp-core |
| `ldp-cli` | `ldp run` / `ldp validate`, exit codes, wiring | all |

**Invariants that must not regress**

1. A chunk is checkpointed **only after every sink accepted every batch**.
2. Output files are either complete or absent: write `*.tmp`, then rename.
3. Checkpoint writes are atomic: write `*.json.tmp`, fsync, rename.
4. Partition planning is deterministic: same data ⇒ same partitions, so a
   restart can skip by id.
5. A corrupt or foreign state file is a loud error, never a silent reset.
6. **Every run recounts what it published** (`Sink::verify_rows`): Parquet sums
   file footers, CSV re-parses the file. A mismatch with the runner's own tally
   is exit 2, never a cheerful summary. This invariant exists because a filename
   bug once wrote 475,712 of 1,000,000 rows and still reported success.

## 3. Phase gates

A phase is done only when its acceptance criteria are executable, not narrated.
`scripts/verify_dod.py` is the executable form of the Phase 1 contract's
Definition of Done; Phase 2 must extend it the same way.

## 4. Phase 2 candidates (in value order, not yet started)

1. **DLQ + error budgets** — row isolation currently *drops* bad rows and only
   counts them (`dropped_rows`). Acceptance: `dlq_path` JSONL containing the
   original row + error; `max_bad_rows` / `max_error_percentage` abort cleanly;
   `fail_fast` mode; test proves a bad row is recoverable from the DLQ.
2. **Concurrent-run protection** — lock file with run_id/PID; second run
   refuses. Acceptance: two simultaneous runs, one exits non-zero with a clear
   message.
3. **Graceful shutdown** — SIGINT stops new partitions, finishes in-flight
   batches, checkpoints only completed chunks, exits 3.
4. **Schema policy** — strict/lenient, persisted observed schema in state.
5. **Watermarks** — MAX(non-null), optional lookback, previous+new in state.
6. **TLS** — the only place a native dependency (`ring`/`aws-lc`) enters the
   build; do it deliberately, verify on Windows-gnu before merging.
7. **Additional connectors** — Snowflake (ADBC or REST), SQL Server (TDS via
   `tiberius`). Neither needs a server install; both need their own type-map
   rows in `ldp-arrow` and their own dialect for the partition predicate.
8. **Atomic staging/swap + merge** for SQL targets.

## 5. Deviations register

Every deviation from `docs/LDP_Phase1_Implementation_Contract.md`, with the
reason and the trigger to revisit.

| # | Deviation | Why | Revisit when |
|---|---|---|---|
| 1 | `Source::read_partition` returns a `BatchStream` instead of one `DataChunk` | The literal signature cannot enforce `max_batch_rows`/`max_batch_bytes` (DoD #5): the connector would have to materialise a whole partition first | never — this is required for correctness |
| 2 | `ExecutionPlan`, `Stage`, `Expr` live in `ldp-core`; `ldp-config` depends on it | The contract's sketch has `ldp-core` needing types from `ldp-config`, which inverts its own "no cycles" rule | never |
| 3 | One linear transformation chain; all targets share one input | Fan-out needs conditional split (Phase 2). The validator rejects fan-out explicitly rather than silently using the wrong branch | Phase 2 |
| 4 | `Sink::abort()` added | Addendum §10 requires failed runs to publish nothing; `finalize()` cannot express that | never |
| 5 | Parquet file per **batch** (`part-<chunk>-<batch>.parquet`), not per chunk | Per-chunk naming made each batch overwrite the previous one — 524,288 rows lost silently | never |
| 6 | CSV sink reports `resumable() == false`; runner replays everything | A single output file cannot skip chunks without dropping their rows | when CSV writes per-chunk parts (Phase 3 option) |
| 7 | Downloads/installs a local Postgres for testing | No Docker, no WSL2, no admin on this machine; DoD #9 demands a real server | when a real server is available |
| 8 | `ldp validate` subcommand added | Makes DoD #7 testable without a database | never (useful permanently) |
| 9 | Custom flat-JSON logger instead of `tracing` | Operational Details §2 fixes exact flat field names; a subscriber fights that nesting | never |
| 10 | `numeric` → text cast + `Decimal128(38, sampled scale)` | `tokio-postgres` cannot decode numeric without an extra feature; sampling keeps "no silent truncation" honest | if a native numeric decoder is adopted |
| 11 | `Sink::verify_rows()` added; every run self-checks its output | The engine once reported `status: success` while writing 475,712 of 1,000,000 rows. Counting inside the engine is the only durable defence against a *silent* short write | never |

## 6. Known limitations (honest list)

* **The writer is the bottleneck.** One task writes all sinks; with zstd, chunk
  throughput degrades as workers finish ahead of it (visible in the logs as
  falling `rows_per_sec`). Correct, not yet fast.
* **`rows_read` counts rows read before the abort**, so a failed run can report
  more rows read than written. It is a progress counter, not an audit total.
* **Nullability is declared `true` for every output column.** Postgres NOT NULL
  constraints are not propagated; Phase 1 does not claim otherwise.
* **No TLS**, so remote/cloud targets need a private path or a Phase 2 addition.
* **No concurrent-run lock**: two runs against one state file will both write.
* **Row isolation drops bad rows** without preserving them (no DLQ yet).
* **`partitioning: range` requires an integer column** and the planner runs
  MIN/MAX on the whole wrapped query (a full scan on large tables).

## 7. Risks

| Risk | Mitigation |
|---|---|
| Toolchain is exotic (gnu + Strawberry MinGW) | `scripts\cargo.ps1` is the only supported entry point; recorded in `LESSONS.md` |
| Silent data loss (already happened once) | Acceptance harness counts rows against the SQL oracle every run; never trust "it compiled" |
| Spec drift between docs and code | Deviations table above; `HANDOFF.md` §5 repeats the reasoning for newcomers |
| Scope creep into Phase 2 | The validator and `PLAN.md` §4 state what is deliberately absent; Phase 2 has its own acceptance list |
