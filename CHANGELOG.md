# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-11-10

First working release: the Phase 1 scope of the implementation contract.
**Preview quality — not yet safe for unattended production use** (see
`README.md` §"Is this ready?").

### Added

- **Declarative pipelines**: strict `pipeline.yaml` (version, settings,
  connections, sources, transformations, targets) with semantic validation that
  reports every problem at once, before any query is issued.
- **Postgres source** as a pure wire-protocol client (no server software
  needed), with range partitioning planned from `MIN`/`MAX` and `none` for a
  single partition. Streams rows into Arrow batches.
- **Parquet sink** (zstd/snappy/none), one file per batch, atomic publish via
  `*.tmp` + rename, empty-result file on zero rows.
- **CSV sink**, RFC 4180 quoting, single file, atomic publish.
- **Transformations**: `filter` and `derive` over a deliberate subset —
  columns, literals, `= != > >= < <=`, `AND OR NOT`, `+ - * /`,
  `IS [NOT] NULL`, `coalesce()`, `now()` — with SQL three-valued logic,
  column-at-a-time evaluation, and row isolation on evaluation errors.
- **Restart safety**: atomic JSON checkpoints (`*.json.tmp` + fsync + rename),
  written only after every sink accepted every batch; a resumed run skips
  completed partitions.
- **Output integrity check**: every run recounts the rows it published
  (Parquet footers, CSV re-parse) and fails with exit code 2 on a mismatch.
- **Partition-level retries** with exponential backoff (3 attempts).
- **Flat JSON logging** for `chunk_started`, `chunk_completed`, `job_summary`,
  `output_verified` and `error`, matching the specified schema exactly.
- **CLI**: `ldp run` and `ldp validate`, with `--threads`, `--state-file`,
  `--log-level`; exit codes 0 (success), 1 (configuration), 2 (runtime).
- **Test suites**: 65 unit tests, 3 integration tests against a real Postgres
  (including resume), and `scripts/verify_dod.py` — a 39-check acceptance
  harness covering the contract's Definition of Done.

### Fixed during bring-up

- The Parquet sink named files by chunk only, so each batch overwrote the
  previous one and a run that wrote 475,712 of 1,000,000 rows still reported
  success. Filenames now include the batch sequence, and the engine verifies
  its own row counts so this class of silent short write fails the run.

### Known limitations

- No TLS: cloud/managed Postgres endpoints that require TLS are not reachable
  yet (`NoTls` only).
- Row isolation **drops** bad rows and counts them; no dead-letter queue yet.
- File targets only — no SQL/warehouse target.
- One source, one linear transformation chain; all targets read the same stage.
- No concurrent-run lock; no graceful shutdown; no schema policy; no watermarks.
