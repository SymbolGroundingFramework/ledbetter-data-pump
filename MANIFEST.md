# MANIFEST.md — workspace file index

If disk and this file disagree, **this file is wrong** — fix it.
Last verified: 2026-11-10.

## Root files

| File | Purpose |
|---|---|
| `AGENTS.md` | session protocol, orientation ritual, hard rules (auto-injected) |
| `PLAN.md` | strategy: architecture, phase gates, deviations register, risks |
| `STATE.md` | live status: what is verified, what is odd, next actions |
| `HANDOFF.md` | bring-up brief: how to run, what is proven, where to start |
| `LESSONS.md` | permanent, dated, append-only toolchain/traps knowledge |
| `ENVIRONMENT.md` | toolchain inventory, paths, ports, install reproduction |
| `MANIFEST.md` | this file index |
| `README.md` | human quickstart + honest readiness table |
| `CHANGELOG.md` | release notes, Keep-a-Changelog format |
| `CONTRIBUTING.md` | ground rules, setup, the pre-PR gate |
| `LICENSE` | MIT |
| `studio.db` | plan tracking (`plan_items`/`plan_events`) — authoritative task status via `scripts\plan.py` |
| `.gitignore` | ignores `target/`, `out/`, `.ldp/`, `.toolchain/`, caches |
| `.gitattributes` | line-ending policy (LF in repo, CRLF for `.ps1`) and binary markers |

## Folders

| Folder | Holds |
|---|---|
| `.github/` | `workflows/ci.yml` — fmt, clippy, unit tests, and integration tests against a Postgres service |
| `docs/` | the delivered specification: 6 documents as `.md` + `.pdf`, plus `README.md` (index), `history/` (session transcript, Gemini chat export) and `phase1-package/` (the original download, unzipped) |
| `archive/` | `LDP_Phase1_Complete_Package.zip`, kept intact as delivered |
| `ldp/` | the Rust workspace — the product (see below) |
| `examples/` | 6 runnable pipeline YAML files (`01_simple_copy` … `06_small_batches`) |
| `scripts/` | `cargo.ps1`, `build.ps1`, `test.ps1`, `pg.ps1`, `verify_dod.py`, `check_logs.py`, `plan.py` (plan DB) |
| `tasks/` | fine-grained checklists (`phase1-checklist.md`) |
| `.toolchain/` | rustup installer, portable Postgres 17.2, scratch logs, generated test fixtures — **not part of the product** |
| `out/` | pipeline output from the last run (gitignored) |
| `.ldp/` | checkpoint state files (gitignored) |

## `ldp/` — the Rust workspace

| Path | Purpose |
|---|---|
| `ldp/Cargo.toml` | workspace manifest, pinned dependency versions, release profile |
| `ldp/ldp-arrow/` | every SQL→Arrow type mapping, decimal parse/format, batch sizing |
| `ldp/ldp-core/src/error.rs` | `LdpError`, exit-code mapping, `ValidationErrors` |
| `ldp/ldp-core/src/chunk.rs` | `DataChunk` |
| `ldp/ldp-core/src/partition.rs` | `Partition` + the specified range-planning algorithm |
| `ldp/ldp-core/src/plan.rs` | `ExecutionPlan`, settings, connection/source/sink specs, URL redaction |
| `ldp/ldp-core/src/traits.rs` | `Source`, `Sink`, `Transform` traits |
| `ldp/ldp-core/src/checkpoint.rs` | atomic JSON checkpoint, corruption/foreign-file guards |
| `ldp/ldp-core/src/expr.rs` | lexer, parser, static type check, columnar evaluator (SQL 3-valued logic) |
| `ldp/ldp-core/src/transform.rs` | filter + derive, row isolation on batch errors |
| `ldp/ldp-core/src/runner.rs` | worker pool, bounded channel, single writer, retries, summary |
| `ldp/ldp-core/src/log.rs` | flat JSON event logger matching the specified schema |
| `ldp/ldp-config/` | strict YAML schema, all-at-once semantic validation, plan construction |
| `ldp/ldp-connectors/ldp-postgres/` | schema discovery, MIN/MAX planning, streaming reads, row→Arrow |
| `ldp/ldp-connectors/ldp-parquet/` | one file per batch, atomic publish, empty-file path |
| `ldp/ldp-connectors/ldp-csv/` | single file, RFC 4180, atomic publish, non-resumable |
| `ldp/ldp-cli/src/main.rs` | `ldp run` / `ldp validate`, flag overrides, exit codes |
| `ldp/ldp-cli/tests/integration_postgres.rs` | 3 real-server tests: end-to-end, resume, empty, bad column |
