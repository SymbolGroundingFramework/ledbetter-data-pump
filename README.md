# Ledbetter Data Pump (LDP)

A YAML-driven data movement engine for Postgres → Parquet/CSV: partitioned
workers, Apache Arrow in flight, and checkpoints, so a killed job resumes
without duplicating or losing rows.

Think "the SSIS data flow, without the `.dtsx`" — one readable job file instead
of XML with designer metadata, and restartability designed in rather than bolted
on.

> **Status: v0.1.0 — Phase 1 preview.**
> It works, it is tested, and it is *not* ready to run unattended in production.
> Read [Is this ready?](#is-this-ready) before trusting it with data you care
> about. See [CHANGELOG.md](CHANGELOG.md) for exactly what ships.

```yaml
version: "1.0"
pipeline:
  name: "customer_export"
  resources:
    connections:
      - { name: "pg", type: "postgres", url: "env://PG_URL" }
  dataflow:
    sources:
      - id: "src"
        connection: "pg"
        query: "SELECT id, name, status, amount FROM customers"
        partitioning: { type: "range", column: "id", chunks: 8 }
    transformations:
      - id: "active"
        input: "src"
        type: "filter"
        condition: "status = 'ACTIVE' AND amount > 0"
      - id: "taxed"
        input: "active"
        type: "derive"
        fields:
          - { name: "taxed_amount", expression: "amount * 1.0825" }
    targets:
      - id: "out"
        input: "taxed"
        type: "parquet"
        path: "./out/customers/"
        compression: "zstd"
```

## What it does

- **Moves rows** from a Postgres query (on-prem, RDS, Aurora, Redshift,
  Cockroach, or anything speaking the v3 wire protocol) into Parquet or CSV.
- **Partitions the read** by integer range (`chunks: 8` → 8 independent
  workers), or reads in one partition.
- **Transforms lightly**: `filter` and `derive` with comparisons, boolean logic,
  arithmetic, `IS [NOT] NULL`, `coalesce()` and `now()`.
- **Restarts safely**: each finished partition is checkpointed; a rerun skips it.
- **Verifies its own output**: after writing, it recounts the rows from the
  files and fails rather than reporting a short write.
- **Logs JSON**, one flat object per line, with `chunk_started`,
  `chunk_completed`, `job_summary` and `output_verified` events.

## Quickstart

### Linux / macOS

```bash
cd ldp
cargo build --release
export PG_URL="postgres://user:password@localhost:5432/mydb"
./target/release/ldp validate ../examples/01_simple_copy.yaml
./target/release/ldp run      ../examples/01_simple_copy.yaml
```

Any Postgres works; there is no bundled server.

### Windows

This repository was developed on a machine with no MSVC toolchain, so it uses
Rust's `windows-gnu` target and the MinGW linker bundled with Strawberry Perl.
`scripts\cargo.ps1` sets that up for you:

```powershell
.\scripts\pg.ps1 start          # portable Postgres test fixture, port 55432
.\scripts\pg.ps1 seed           # 1,000,000 demo rows
.\scripts\build.ps1             # -> ldp\target\debug\ldp.exe
$env:LDP_PG_URL = "postgres://postgres@127.0.0.1:55432/ldp"
.\ldp\target\debug\ldp.exe run examples\01_simple_copy.yaml
```

On a normal Windows machine with Visual Studio Build Tools installed, plain
`cargo build` works and you can ignore the wrapper. Details:
[ENVIRONMENT.md](ENVIRONMENT.md).

## Guarantees the engine actually enforces

1. A partition is checkpointed **only after** every target accepted every batch.
2. Output files are **complete or absent** — written to `*.tmp`, then renamed.
3. Checkpoints are written atomically (`*.json.tmp` + fsync + rename), and a
   corrupt or foreign state file is a loud error, never a silent reset.
4. Partition planning is deterministic, so a restart can skip by partition id.
5. **Every run recounts its published rows** and exits 2 on a mismatch.

## Is this ready?

Honest answers, not marketing:

| Question | Answer |
|---|---|
| Can it move my Postgres table to Parquet reliably? | **Yes**, with restart safety and row verification. |
| Can I point it at Azure / Supabase / a TLS-only endpoint? | **No.** Phase 1 has no TLS (`NoTls` only). Local and non-TLS endpoints only. |
| If a row fails to evaluate, do I get it back? | **No.** Bad rows are dropped and counted; there is no dead-letter queue yet. |
| Can I write into another database? | **No.** File targets only (Parquet, CSV). |
| Is it safe if two runs overlap? | **No.** There is no run lock. |
| Is it fast? | It did 1M rows in ~5 s (debug build, 4 threads) on a laptop. The single writer becomes the bottleneck under compression. |
| Is it production-ready? | **Not yet.** Phase 2 (DLQ, TLS, locks, shutdown, schema policy, SQL targets) is the gap between "works" and "safe to leave running". |

Phase 2 is planned item-by-item in [PLAN.md](PLAN.md) §4.

## Verifying it yourself

Everything claimed above is executable:

```bash
cd ldp && cargo test --workspace        # 65 unit tests
# with a real Postgres:
export LDP_TEST_PG_URL=postgres://postgres@127.0.0.1:5432/postgres
export RUST_TEST_THREADS=1
cargo test -p ldp-cli --test integration_postgres
```

On Windows, `.\scripts\test.ps1` runs the whole gate: unit tests, clippy,
integration tests, and `scripts/verify_dod.py` — a 39-check acceptance harness
that drives the real binary against a real database and reads the output back
with `pyarrow`, asserting row counts against SQL oracles.

## How it is built

```
ldp-arrow        every SQL↔Arrow type mapping (the only place types are decided)
ldp-core         plan, traits, checkpoint, expression engine, transforms,
                 worker pool, JSON logger
ldp-config       strict YAML schema, semantic validation, plan construction
ldp-connectors   ldp-postgres (streaming reads), ldp-parquet, ldp-csv
ldp-cli          `ldp run` / `ldp validate`
```

Reads happen in parallel; a single writer task owns the sinks, so Parquet/CSV
writers never need locking. Backpressure comes from a bounded channel.

## Repository layout

```
docs/        the specification this implements (contract, whitepaper,
             failure modes, operational details, YAML guide, roadmap)
ldp/         the Rust workspace
examples/    six runnable pipelines
scripts/     build/test helpers, Postgres fixture, DoD harness, log checker
AGENTS.md    how an AI agent should work in this repo
PLAN.md      strategy, phase gates, deviations register
HANDOFF.md   bring-up brief
STATE.md     live status
LESSONS.md   hard-won toolchain/traps notes
```

The working-memory files (`AGENTS.md`, `PLAN.md`, `STATE.md`, `HANDOFF.md`,
`LESSONS.md`) are part of how this project is built rather than marketing. They
are kept in the open on purpose: the reasoning behind a decision should be
reviewable alongside the code, including the bug that once lost half a million
rows ([LESSONS.md](LESSONS.md) #8).

## License

MIT — see [LICENSE](LICENSE).
