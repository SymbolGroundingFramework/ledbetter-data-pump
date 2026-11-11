# ENVIRONMENT.md — machine inventory for this workspace

Verified 2026-11-10. Anything here can be re-checked with the commands shown.

## Toolchain (the unusual part)

| Thing | Value | Why it matters |
|---|---|---|
| Rust | `1.99.1`, host `x86_64-pc-windows-gnu` | installed per-user by `rustup-init.exe`; **no MSVC** on this machine |
| Cargo | `1.99.1` at `%USERPROFILE%\.cargo\bin\cargo.exe` | not on the default `PATH` |
| Linker | MinGW-w64 GCC 13.1.0, `C:\Strawberry\c\bin\gcc.exe` | comes with Strawberry Perl; **required** by the gnu toolchain |
| Python | `3.12.9` at `C:\Python312` (`py -3.12`) | `pyarrow 26`, `pandas 3`, `duckdb`, `pytest` available |
| PostgreSQL | **17.2 portable** at `.toolchain\pg\pgsql` | test fixture only; **not** a product dependency |
| Docker | absent | no container-based Postgres possible |
| WSL | distro registered but **virtualisation disabled** | WSL2 cannot start; WSL1 unsupported |
| Admin rights | none | nothing may require elevation |

**Why gnu and not msvc:** `rustup-init` defaults to `x86_64-pc-windows-msvc`,
which needs the MSVC linker plus the Windows SDK — neither is installed and
installing them needs admin. The gnu host links with the MinGW that Strawberry
Perl already ships.

Reproduce the install (no admin):
```powershell
Invoke-WebRequest https://static.rust-lang.org/rustup/dist/x86_64-pc-windows-msvc/rustup-init.exe -OutFile $env:TEMP\rustup-init.exe
& $env:TEMP\rustup-init.exe -y --no-modify-path --default-host x86_64-pc-windows-gnu --default-toolchain stable --profile minimal
& "$env:USERPROFILE\.cargo\bin\rustup.exe" component add clippy --toolchain stable-x86_64-pc-windows-gnu
```
(The `rustup-init.exe` binary is msvc-hosted by design; `--default-host` picks
the *toolchain*, which is what matters.)

## Paths and ports

| What | Where |
|---|---|
| Rust workspace | `ldp\` (6 crates + integration test) |
| Built binary | `ldp\target\debug\ldp.exe` (or `...\release\`) |
| Postgres binaries | `.toolchain\pg\pgsql\bin` |
| Postgres data dir | `.toolchain\pg\data` |
| Postgres port | **55432**, bound to `127.0.0.1`, trust auth, user `postgres` |
| Postgres log | `.toolchain\pg\server.log` |
| Test database / table | `ldp` / `customers` (1,000,000 rows), `empty_table` |
| Pipeline output | `out\<pipeline>\` |
| Checkpoint state | `.ldp\<pipeline>.json` |

## Dependency versions that matter

`arrow 60.1.0` · `parquet 60.1.0` · `tokio 1.54.1` · `tokio-postgres 0.7.18`
(with `with-chrono-0_4`) · `serde_yaml 0.9.34` (deprecated but sufficient) ·
`thiserror 2.0` · `chrono 0.4`.

`parquet` pulls `zstd-sys`, which compiles C via `cc` → needs the MinGW `gcc`
on `PATH`. That is one more reason every cargo invocation goes through
`scripts\cargo.ps1`.

## Things that are deliberately *not* installed

No Snowflake driver, no Oracle Instant Client, no SQL Server tools, no ODBC.
LDP is a wire-protocol client: for Phase 1 it needs nothing but TCP to
`5432`-style endpoints. Cloud targets (RDS/Aurora/Azure/Redshift/Supabase) work
with a URL change and no local software.
