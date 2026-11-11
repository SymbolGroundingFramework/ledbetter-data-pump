# LESSONS.md — hard-won, append-only

Strike with explanation, never silently delete. Newest last.

## 1. `rustup-init` installs an msvc toolchain unless you set `--default-host`

**2026-11-10.** `--default-toolchain stable-x86_64-pc-windows-gnu` is *not*
enough: rustup warns "installing msvc toolchain without its prerequisites" and
then fails, because there is no MSVC linker here. The flag that decides is
`--default-host x86_64-pc-windows-gnu`. Verify with
`rustup toolchain list` (must not say `msvc`) and by building a hello-world.

## 2. The MinGW linker for Windows-gnu lives inside Strawberry Perl

**2026-11-10.** `C:\Strawberry\c\bin\gcc.exe` is MinGW-w64 13.1.0 and links
Rust's gnu target. It is not on `PATH`. Every cargo call needs
`C:\Strawberry\c\bin` and `%USERPROFILE%\.cargo\bin` prepended — that is what
`scripts\cargo.ps1` exists for. `parquet`'s `zstd-sys` also needs this gcc to
compile its C.

## 3. `cargo add --path` resolves relative to the *workspace root*, not the crate

**2026-11-10.** `cargo add -p ldp-parquet --path ../../ldp-core ldp-core`
looked for `C:\ldp-core`. Write path dependencies into the manifests directly;
it is less magic and survives restructuring.

## 4. `pg_ctl` hangs PowerShell because the daemon inherits the console handle

**2026-11-10.** Capturing `pg_ctl start` output (`*>` / pipe) blocks until the
server exits — the wrapper hung for five minutes. Use
`Start-Process -RedirectStandardOutput <file>`, then poll `pg_isready`.
`pg_isready` also needs `-U postgres -d postgres`, or it reports "not ready"
because it probes as the OS user (and logs `role "..." does not exist`).

## 5. A `psql` NOTICE on stderr can abort your whole script

**2026-11-10.** `DROP TABLE IF EXISTS` emits a NOTICE on stderr. With
`$ErrorActionPreference='Stop'`, piping SQL into `psql` turned that NOTICE into
a terminating error that killed the pipeline after the `DROP` — leaving a table
created but unseeded (`count(*) = 0`). Run SQL with `-f file` and lower
`ErrorActionPreference` around the call.

## 6. PowerShell `>` writes UTF-16; JSONL must be UTF-8

**2026-11-10.** Capturing engine logs with `>` produced a UTF-16LE file with a
BOM, which the log checker rejected with `UnicodeDecodeError: 0xff`. Use
`| Set-Content -Encoding utf8`, and have readers use `utf-8-sig` so a BOM is
tolerated rather than fatal.

## 7. Never round-trip source files through `Get-Content`/`Set-Content`

**2026-11-10.** A PowerShell rewrite of a Rust file double-encoded UTF-8: the
doc comment `Int32 → Int64` became `Int32 â†’ Int64`. Use the edit tools for
source; keep PowerShell for scripts and orchestration.

## 8. "One file per chunk" silently destroyed 524,288 rows

**2026-11-10.** A partition streams several batches (`max_batch_rows`), but the
Parquet sink named files by chunk id only — so each batch overwrote the previous
file, and the run still reported success. The independent row count caught it
(475,712 of 1,000,000). Filenames must include the batch sequence
(`part-<chunk>-<batch>.parquet`), and every acceptance run must count rows
against a SQL oracle.

**Permanent guard (added same day):** `Sink::verify_rows()` makes the engine
recount its own output after every run — Parquet sums the file footers, CSV
re-parses the file — and a mismatch is exit code 2, not a warning. A mock sink
that under-reports is a permanent test. The lesson is not "name files
carefully"; it is **a component that claims success must be able to prove it**.

## 9. `numeric` has no decoder in `tokio-postgres` by default

**2026-11-10.** Reading a `numeric` column with the default features fails at
runtime. Casting the column to text in the generated SELECT and rebuilding
`Decimal128(38, scale)` in Arrow keeps precision — but the scale must be
sampled from the data (bounded `LIMIT 500`) and an unrepresentable value must
be a loud error, never a truncation.

## 10. A `RowStream` is `!Unpin`

**2026-11-10.** `client.query_raw(..)` returns a `RowStream` that pins itself;
`stream.next()` needs `Box::pin(stream)` first. The compiler's `PhantomPinned`
error names the symptom but not the fix.

## 11. A single-file sink cannot resume from a chunk checkpoint

**2026-11-10.** CSV writes one file. If a restart skipped "completed" chunks,
their rows would be missing from the rebuilt file. `resumable() == false` makes
the runner replay everything and rewrite the file atomically — still
duplicate-free. Per-chunk-partitioned outputs (Parquet) can resume safely.
