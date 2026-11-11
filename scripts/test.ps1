# scripts/test.ps1 — the full gate: unit tests, Postgres integration tests,
# clippy, and the Definition of Done acceptance harness.
#
#   .\scripts\test.ps1              everything
#   .\scripts\test.ps1 -Fast       unit tests + clippy only (no database)
param(
    [switch]$Fast
)
# 'Continue': cargo writes progress to stderr, and under 'Stop' that aborts the
# script instead of the build. Every step below checks $LASTEXITCODE explicitly.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$fail = 0

# The integration tests create and drop a shared table, so they must not run
# concurrently with each other. RUST_TEST_THREADS avoids passing `--` through
# the cargo wrapper, which PowerShell keeps swallowing.
$env:RUST_TEST_THREADS = '1'

Write-Output "=== unit tests ==="
& (Join-Path $PSScriptRoot 'cargo.ps1') test --workspace --quiet
if ($LASTEXITCODE -ne 0) { $fail = 1; Write-Output "unit tests FAILED" }

Write-Output "=== clippy ==="
# Gate on clippy's own output rather than passing `-- -D warnings` through the
# wrapper: PowerShell keeps eating the `--` separator, and a silent mis-pass
# turns into a confusing cargo argument error.
$clippyOut = & (Join-Path $PSScriptRoot 'cargo.ps1') clippy --workspace --all-targets --quiet 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) { $fail = 1; Write-Output "clippy FAILED (non-zero exit)" }
$clippyWarnings = ($clippyOut -split "`n") | Where-Object { $_ -match '^warning:' -or $_ -match ': warning:' }
if ($clippyWarnings) {
    $fail = 1
    Write-Output "clippy produced warnings:"
    $clippyWarnings | Select-Object -First 20 | ForEach-Object { Write-Output "  $_" }
}
else {
    Write-Output "clippy clean"
}

if ($Fast) {
    if ($fail -eq 0) { Write-Output "FAST gate passed" }
    exit $fail
}

Write-Output "=== postgres fixture ==="
& (Join-Path $PSScriptRoot 'pg.ps1') start
if ($LASTEXITCODE -ne 0) { Write-Output "cannot start postgres"; exit 1 }

Write-Output "=== integration tests (real Postgres) ==="
$env:LDP_TEST_PG_URL = 'postgres://postgres@127.0.0.1:55432/ldp'
$env:LDP_PG_URL = $env:LDP_TEST_PG_URL
& (Join-Path $PSScriptRoot 'cargo.ps1') test -p ldp-cli --test integration_postgres --quiet
if ($LASTEXITCODE -ne 0) { $fail = 1; Write-Output "integration tests FAILED" }

Write-Output "=== Definition of Done ==="
# Build first, quietly: cargo's progress goes to stderr, which PowerShell renders
# as an error record and makes a passing gate look like a failure.
& (Join-Path $PSScriptRoot 'build.ps1') *> $null
if ($LASTEXITCODE -ne 0) { $fail = 1; Write-Output "build FAILED" }
$env:PYTHONIOENCODING = 'utf-8'
py -3.12 (Join-Path $PSScriptRoot 'verify_dod.py')
if ($LASTEXITCODE -ne 0) { $fail = 1 }

if ($fail -eq 0) { Write-Output "ALL GATES PASSED" } else { Write-Output "GATES FAILED" }
exit $fail
