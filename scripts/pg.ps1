# scripts/pg.ps1 — the local test fixture ONLY.
#
# LDP ships no server dependency: the engine is a wire-protocol client and
# reaches any Postgres anywhere (localhost, RDS, Aurora, Azure, Redshift) by
# URL. This portable server exists so the Phase 1 Definition of Done - including
# the kill-mid-run restart test - can be proven on this machine, which has no
# Docker and no WSL2.
#
#   .\scripts\pg.ps1 init     first-time initdb
#   .\scripts\pg.ps1 start    start on 127.0.0.1:55432
#   .\scripts\pg.ps1 stop
#   .\scripts\pg.ps1 status
#   .\scripts\pg.ps1 seed     (re)create the demo dataset, 1,000,000 rows
#   .\scripts\pg.ps1 psql     interactive shell
param(
    [Parameter(Position = 0)]
    [ValidateSet('init', 'start', 'stop', 'status', 'seed', 'psql', 'url')]
    [string]$Action = 'status'
)

$ErrorActionPreference = 'Stop'
$root   = Split-Path -Parent $PSScriptRoot
$pghome = Join-Path $root '.toolchain\pg\pgsql'
$bin    = Join-Path $pghome 'bin'
$data   = Join-Path $root '.toolchain\pg\data'
$log    = Join-Path $root '.toolchain\pg\server.log'
$port   = 55432
$db     = 'ldp'
$conn   = "postgres://postgres@127.0.0.1:$port/$db"

function Test-Ready {
    & (Join-Path $bin 'pg_isready.exe') -h 127.0.0.1 -p $port -U postgres -d postgres *> $null
    return ($LASTEXITCODE -eq 0)
}

# pg_ctl spawns the postgres daemon, which inherits the console handle. Capturing
# its stdout in PowerShell blocks until the server exits, so daemon-starting
# commands go through Start-Process with file redirection. All paths here are
# space-free, so no inner quoting is needed.
function Invoke-PgCtl {
    param([string[]]$PgArgs, [string]$Tag)
    $out = Join-Path $root ".toolchain\pg\pg_ctl.$Tag.out"
    $p = Start-Process -FilePath (Join-Path $bin 'pg_ctl.exe') -ArgumentList $PgArgs `
        -WindowStyle Hidden -RedirectStandardOutput $out -PassThru
    $p.WaitForExit(60000) | Out-Null
}

switch ($Action) {
    'init' {
        if (-not (Test-Path (Join-Path $bin 'initdb.exe'))) {
            throw "portable Postgres is missing at $pghome (see ENVIRONMENT.md)"
        }
        if (Test-Path (Join-Path $data 'PG_VERSION')) {
            Write-Output "cluster already initialised at $data"
        }
        else {
            Invoke-PgCtl -Tag 'initdb' -PgArgs @('-D', $data, '-U', 'postgres', '--auth=trust', '--encoding=UTF8', '--no-sync')
            if (-not (Test-Path (Join-Path $data 'PG_VERSION'))) { throw "initdb failed; see .toolchain\pg\pg_ctl.initdb.out" }
            Write-Output "initialised cluster at $data"
        }
    }
    'start' {
        if (-not (Test-Path (Join-Path $data 'PG_VERSION'))) { & $PSCommandPath init }
        if (Test-Ready) { Write-Output "already running on $port"; break }
        # The -o value contains spaces, so it is the one argument that must
        # carry its own quotes through Start-Process.
        Invoke-PgCtl -Tag 'start' -PgArgs @('-D', $data, '-o', '"-p 55432 -c listen_addresses=127.0.0.1"', '-l', $log, 'start')
        for ($i = 0; $i -lt 40; $i++) {
            if (Test-Ready) { break }
            Start-Sleep -Milliseconds 300
        }
        if (-not (Test-Ready)) {
            $detail = if (Test-Path $log) { Get-Content $log -Tail 15 | Out-String } else { '' }
            throw "server did not become ready; see $log`n$detail"
        }
        Write-Output "postgres ready on 127.0.0.1:$port"
        $exists = & (Join-Path $bin 'psql.exe') -h 127.0.0.1 -p $port -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'"
        if ("$exists".Trim() -ne '1') {
            & (Join-Path $bin 'createdb.exe') -h 127.0.0.1 -p $port -U postgres $db
            if ($LASTEXITCODE -ne 0) { throw "createdb failed" }
            Write-Output "created database '$db'"
        }
    }
    'stop' {
        if (Test-Path (Join-Path $data 'PG_VERSION')) {
            Invoke-PgCtl -Tag 'stop' -PgArgs @('-D', $data, 'stop', '-m', 'fast')
            Write-Output "stopped"
        }
    }
    'status' {
        if (Test-Ready) { Write-Output "running on 127.0.0.1:$port" }
        else { Write-Output "not running" }
    }
    'seed' {
        if (-not (Test-Ready)) { throw "start the server first: .\scripts\pg.ps1 start" }
        $sql = @'
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    status     TEXT,
    amount     DOUBLE PRECISION,
    created_at TIMESTAMP,
    region     TEXT,
    unit_price NUMERIC(12,4)
);
INSERT INTO customers (id, name, status, amount, created_at, region, unit_price)
SELECT
    g,
    'customer_' || g,
    CASE WHEN g % 10 = 0 THEN 'CANCELLED' ELSE 'ACTIVE' END,
    ((g % 1000)::double precision) / 10.0,
    TIMESTAMP '2026-01-01 00:00:00' + (g * INTERVAL '1 minute'),
    (ARRAY['US','CA','MX','GB','DE'])[1 + (g % 5)],
    ((g % 10000)::numeric) / 100
FROM generate_series(1, 1000000) AS g;
DROP TABLE IF EXISTS empty_table;
CREATE TABLE empty_table (id BIGINT PRIMARY KEY, note TEXT);
'@
        $sqlPath = Join-Path $root '.toolchain\pg\seed.sql'
        Set-Content -Path $sqlPath -Value $sql -Encoding utf8
        # Run from a file, not a pipeline: psql writes NOTICEs to stderr, and
        # with $ErrorActionPreference='Stop' a piped NOTICE aborts the pipeline
        # mid-script (which once left the table created but unseeded).
        $prev = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        & (Join-Path $bin 'psql.exe') -h 127.0.0.1 -p $port -U postgres -d $db -v ON_ERROR_STOP=1 -q -f $sqlPath 2>&1 | Out-Null
        $seedExit = $LASTEXITCODE
        $ErrorActionPreference = $prev
        if ($seedExit -ne 0) { throw "seed failed (psql exit $seedExit)" }
        $n = & (Join-Path $bin 'psql.exe') -h 127.0.0.1 -p $port -U postgres -d $db -tAc 'SELECT count(*) FROM customers'
        Write-Output "seeded customers: $("$n".Trim()) rows"
    }
    'psql' {
        & (Join-Path $bin 'psql.exe') -h 127.0.0.1 -p $port -U postgres -d $db
    }
    'url' {
        Write-Output $conn
    }
}
