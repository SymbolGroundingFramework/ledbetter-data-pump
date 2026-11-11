# scripts/build.ps1 — build the engine.
#
#   .\scripts\build.ps1            debug binary (fast iteration)
#   .\scripts\build.ps1 -Release   optimised binary (recommended for real loads)
#   .\scripts\build.ps1 -Workspace everything, including tests
param(
    [switch]$Release,
    [switch]$Workspace
)
# 'Continue': cargo's stderr progress must not become a terminating error.
$ErrorActionPreference = 'Continue'
$cargoArgs = @('build')
if ($Release) { $cargoArgs += '--release' }
if ($Workspace) { $cargoArgs += '--workspace' } else { $cargoArgs += @('--bin', 'ldp') }
& (Join-Path $PSScriptRoot 'cargo.ps1') @cargoArgs
$profile = if ($Release) { 'release' } else { 'debug' }
$exe = Join-Path (Split-Path -Parent $PSScriptRoot) "ldp\target\$profile\ldp.exe"
if (Test-Path $exe) { Write-Output "binary: $exe" }
