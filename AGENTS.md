# AGENTS.md — Ledbetter Data Pump (LDP)

You are operating inside an ongoing project workspace: **LDP, a YAML-driven
data movement engine** (an SSIS data-flow replacement). This file is your
standing briefing and is auto-injected into every session here.

## What this project is

A Rust engine that moves data from relational sources into files/warehouses at
high throughput: partitioned shared-nothing workers, Apache Arrow
`RecordBatch` in flight, declarative `pipeline.yaml` jobs, atomic checkpointing
so a killed job resumes without duplicating rows.

The authoritative specification lives in `docs/` — read it before changing
semantics:

| Doc | What it decides |
|---|---|
| `docs/LDP_Architecture_Whitepaper.md` | overall architecture, YAML schema, roadmap |
| `docs/LDP_Phase1_Implementation_Contract.md` | **the build contract** — types, traits, crate layout, Definition of Done |
| `docs/LDP_Failure_Modes_Addendum.md` | edge-case behaviour, phased |
| `docs/LDP_Phase1_Operational_Details.md` | partition algorithm, log schema, CLI, exit codes |
| `docs/LDP_YAML_User_Guide.md` | how to author pipeline YAML |
| `docs/LDP_Roadmap.md` | Phase 1→4 sequencing |
| `docs/LDP_Complete_Session_Transcript.md` | why the design is what it is |

## Mandatory orientation at session start

When the user says "resume", "continue", "status", or asks to work on the
project:
1. Read `STATE.md` and `PLAN.md` BEFORE responding substantively.
2. Run `py -3.12 scripts\plan.py show` — studio.db `plan_items` is
   AUTHORITATIVE for status. Contradictions become your first question.
3. Check `MANIFEST.md`; if disk has files/folders not listed there, inspect
   and update the manifest.
4. Consult `ENVIRONMENT.md` for toolchain/paths; `LESSONS.md` before
   rediscovering anything the hard way.
5. Confirm state in one short paragraph, then proceed.

## Standing commands

| Phrase | Meaning |
|---|---|
| "resume studio" | Full orientation per above, then report readiness |
| "build" | `.\scripts\build.ps1` — cargo build of the workspace |
| "test" | `.\scripts\test.ps1` — unit tests + Postgres integration suite |
| "update memory" | Fold facts into `STATE.md` and sync studio.db (`plan.py set ... --reason`) |
| "close session" | Final flush: STATE completions, next actions, plan DB synced, `HANDOFF.md` refreshed |
| "status" | What runs, what's verified, what's blocked |

Update `STATE.md` the moment you learn a durable fact. Sessions drift and
compaction loses detail; files are memory, conversations are disposable.

## Hard rules for this workspace

1. **Rust is `x86_64-pc-windows-gnu`, not msvc.** There is no MSVC linker on
   this machine. Every cargo invocation needs MinGW on PATH:
   `C:\Strawberry\c\bin` + `%USERPROFILE%\.cargo\bin`. Use
   `.\scripts\cargo.ps1 <args>` (wraps that) instead of bare `cargo`.
   Why: `LESSONS.md` #1–#3.
2. **`py -3.12`, never bare `python`** (the Windows stub does nothing useful).
3. **No Docker, no WSL2** (virtualisation is disabled in firmware). The test
   database is the portable Postgres in `.toolchain\pg` on port **55432**,
   started by `.\scripts\pg.ps1 start`. It is a *test fixture* — LDP ships no
   server dependency, only wire-protocol clients.
4. **Verify the artifact, not the logs.** A phase is done when a real
   `ldp run` produced real rows in a real Parquet file, not when it compiled.
5. **The contract is the law for Phase 1.** Do not implement Phase 2 features
   (DLQ budgets, watermarks, conditional split, atomic swap, schema policy)
   even when they look easy — they are specified, sequenced, and must land
   with their own tests. See `PLAN.md` §Phase gates.
6. **Generated caches go stale silently.** After changing the engine, delete
   scratch output (`out\`, `.ldp\`) before re-verifying, or you will "prove"
   correctness against yesterday's Parquet file.

## Layout

```
docs\       the specification (read-only reference)
ldp\        the Rust workspace — the product
examples\   pipeline YAML, runnable against the test fixture
scripts\    build/test/pg/cargo helpers + plan.py (plan DB)
tasks\      fine-grained checklists
.toolchain\ rustup installer + portable Postgres (not part of the product)
archive\    original delivered zip package
```

## Map

`PLAN.md` strategy · `STATE.md` live status · `LESSONS.md` permanent lessons ·
`ENVIRONMENT.md` toolchain inventory · `MANIFEST.md` file index ·
`HANDOFF.md` bring-a-new-agent-up-to-speed brief · `tasks\` checklists ·
`studio.db` + `scripts\plan.py` plan tracking.
