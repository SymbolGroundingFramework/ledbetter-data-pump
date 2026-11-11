# scripts/cargo.ps1 — every cargo invocation on this machine goes through here.
#
# Why: Rust is installed as x86_64-pc-windows-gnu because there is no MSVC
# linker here, and the MinGW toolchain that links it lives inside Strawberry
# Perl. Neither is on the default PATH, so a bare `cargo build` fails with
# "linker `x86_64-w64-mingw32-gcc` not found". This wrapper puts both on PATH.
#
#   .\scripts\cargo.ps1 build
#   .\scripts\cargo.ps1 test --workspace
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CargoArgs
)

# NOTE: 'Continue', not 'Stop'. Cargo writes progress ("Compiling ...") to
# stderr; under $ErrorActionPreference='Stop' every such line becomes a
# terminating NativeCommandError and the build dies on its own status output.
# Failures are detected via $LASTEXITCODE instead.
$ErrorActionPreference = 'Continue'
$mingw = 'C:\Strawberry\c\bin'
$cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'

if (-not (Test-Path (Join-Path $cargoBin 'cargo.exe'))) {
    throw "cargo not found at $cargoBin - install Rust first (see ENVIRONMENT.md)"
}

# On this machine the gnu toolchain links with the MinGW bundled in Strawberry
# Perl. Anywhere else that directory is absent and the installed toolchain
# (usually MSVC) works as-is — so a missing MinGW is not an error, only a
# reason not to prepend it.
if (Test-Path (Join-Path $mingw 'gcc.exe')) {
    $env:PATH = "$mingw;$cargoBin;$env:PATH"
}
else {
    $env:PATH = "$cargoBin;$env:PATH"
}
$ldp = Join-Path (Split-Path -Parent $PSScriptRoot) 'ldp'
Push-Location $ldp
try {
    & cargo @CargoArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
