#!/usr/bin/env python3
"""LDP Phase 1 — Definition of Done acceptance harness.

Runs the real binary against the real Postgres fixture and checks the output
files with pyarrow (an independent reader, not our own code).

    py -3.12 scripts/verify_dod.py [--keep]

Requirements:
  * portable Postgres running on 127.0.0.1:55432 with the `ldp` database seeded
    (`.\\scripts\\pg.ps1 start` then `.\\scripts\\pg.ps1 seed`)
  * `ldp/target/debug/ldp.exe` built (`.\\scripts\\build.ps1`)

Exit code 0 only if every check passes. Each check maps to an item in
docs/LDP_Phase1_Implementation_Contract.md §8.
"""
from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The Windows console here is cp1252 and cannot encode "→"/"—"; force UTF-8 so
# the report is readable instead of raising UnicodeEncodeError mid-run.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
LDP = os.path.join(ROOT, "ldp", "target", "debug", "ldp.exe")
PSQL = os.path.join(ROOT, ".toolchain", "pg", "pgsql", "bin", "psql.exe")
PG_URL = os.environ.get("LDP_PG_URL", "postgres://postgres@127.0.0.1:55432/ldp")
OUT = os.path.join(ROOT, "out")
STATE = os.path.join(ROOT, ".ldp")

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, bool(ok), detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def run(yaml: str, extra_env: dict | None = None) -> tuple[int, str]:
    env = dict(os.environ)
    env["LDP_PG_URL"] = PG_URL
    if extra_env:
        env.update(extra_env)
    p = subprocess.run(
        [LDP, "run", yaml],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def validate(yaml: str, extra_env: dict | None = None) -> tuple[int, str]:
    env = dict(os.environ)
    env["LDP_PG_URL"] = PG_URL
    if extra_env:
        env.update(extra_env)
    p = subprocess.run(
        [LDP, "validate", yaml],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def sql(query: str) -> str:
    p = subprocess.run(
        [PSQL, "-h", "127.0.0.1", "-p", "55432", "-U", "postgres", "-d", "ldp", "-tAc", query],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return (p.stdout or "").strip()


def parquet_rows(pattern: str) -> tuple[int, int]:
    """(rows, distinct ids) across matching Parquet files."""
    import pyarrow.dataset as ds
    import pyarrow.compute as pc

    files = sorted(glob.glob(pattern))
    if not files:
        return 0, 0
    t = ds.dataset(files, format="parquet").to_table()
    distinct = 0
    if "id" in t.column_names and t.num_rows:
        distinct = pc.count_distinct(t.column("id")).as_py()
    return t.num_rows, distinct


def writes_files(dir_path: str) -> int:
    return len([f for f in glob.glob(os.path.join(dir_path, "*")) if not f.endswith(".tmp")])


def temp_debris(dir_path: str) -> int:
    return len(glob.glob(os.path.join(dir_path, "*.tmp"))) + len(glob.glob(os.path.join(dir_path, ".*.tmp")))


def reset(*paths: str) -> None:
    for p in paths:
        if os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)
        elif os.path.exists(p):
            os.remove(p)


def make_fixtures() -> str:
    """Write the invalid pipelines used by the validation checks, so this script
    is reproducible from a clean checkout instead of relying on ad-hoc files."""
    fixtures = os.path.join(ROOT, ".toolchain", "fixtures")
    os.makedirs(fixtures, exist_ok=True)
    base = open(os.path.join(ROOT, "examples", "01_simple_copy.yaml"), encoding="utf-8").read()
    cases = {
        "bad_input.yaml": base.replace('input: "src"', 'input: "nope"'),
        "bad_env.yaml": base.replace("env://LDP_PG_URL", "env://NOT_SET_ANYWHERE"),
        "bad_setting.yaml": base.replace("max_threads: 4", "max_thread: 4"),
        "bad_expr.yaml": base.replace(
            "    targets:",
            '    transformations:\n      - id: "f"\n        input: "src"\n'
            '        type: "filter"\n        condition: "region IN (\'US\',\'CA\')"\n    targets:',
        ).replace(
            '        input: "src"\n        type: "parquet"', '        input: "f"\n        type: "parquet"'
        ),
    }
    for name, text in cases.items():
        with open(os.path.join(fixtures, name), "w", encoding="utf-8") as fh:
            fh.write(text)
    return fixtures


def main() -> int:
    if not os.path.exists(LDP):
        print(f"missing binary: {LDP}\nrun .\\scripts\\build.ps1 first")
        return 2
    if not os.path.exists(PSQL):
        print(f"missing psql fixture: {PSQL}")
        return 2
    fixtures = make_fixtures()

    print("LDP Phase 1 — Definition of Done\n")

    # --- DoD 1: Postgres → Parquet with range partitioning -----------------
    print("DoD 1 — Postgres → Parquet, range partitioned")
    reset(os.path.join(OUT, "simple_copy"), os.path.join(STATE, "simple_copy.json"))
    code, log = run(os.path.join("examples", "01_simple_copy.yaml"))
    rows, distinct = parquet_rows(os.path.join(OUT, "simple_copy", "*.parquet"))
    summary = last_summary(log)
    check("exit 0", code == 0, f"exit={code}")
    check("all 1,000,000 rows written", rows == 1_000_000, f"rows={rows}")
    check("no duplicates or loss", distinct == 1_000_000, f"distinct={distinct}")
    check("8 partitions completed", summary.get("chunks_completed") == 8, str(summary.get("chunks_completed")))
    check("no temp debris", temp_debris(os.path.join(OUT, "simple_copy")) == 0)
    check(
        "engine self-verified the rows on disk",
        target_field(summary, "verified_rows") == 1_000_000,
        f"verified={target_field(summary, 'verified_rows')}",
    )

    # --- DoD 2: filter + derive -------------------------------------------
    print("\nDoD 2 — filter and derive")
    reset(os.path.join(OUT, "filter_and_derive"), os.path.join(STATE, "filter_and_derive.json"))
    code, log = run(os.path.join("examples", "02_filter_and_derive.yaml"))
    rows, _ = parquet_rows(os.path.join(OUT, "filter_and_derive", "*.parquet"))
    oracle = int(
        sql(
            "SELECT count(*) FROM customers WHERE status='ACTIVE' AND amount>0 "
            "AND created_at IS NOT NULL"
        )
        or 0
    )
    check("exit 0", code == 0, f"exit={code}")
    check("row count matches the SQL oracle", rows == oracle, f"engine={rows} sql={oracle}")
    check(
        "derived columns present",
        derived_columns(os.path.join(OUT, "filter_and_derive", "*.parquet"))
        == ["taxed_amount", "processed_at", "region_label"],
    )

    # --- DoD 3: resume skips completed chunks ------------------------------
    print("\nDoD 3 — restart skips completed chunks")
    reset(os.path.join(OUT, "simple_copy"), os.path.join(STATE, "simple_copy.json"))
    code, log = run(
        os.path.join("examples", "01_simple_copy.yaml"),
        {"LDP_FAIL_AFTER_CHUNKS": "3"},
    )
    check("aborted run exits 2", code == 2, f"exit={code}")
    state = read_state(os.path.join(STATE, "simple_copy.json"))
    check("3 chunks checkpointed", len(state.get("completed_chunks", [])) == 3, str(state.get("completed_chunks")))
    code, log = run(os.path.join("examples", "01_simple_copy.yaml"))
    rows, distinct = parquet_rows(os.path.join(OUT, "simple_copy", "*.parquet"))
    summary = last_summary(log)
    check("resume exits 0", code == 0, f"exit={code}")
    check("3 chunks skipped", summary.get("chunks_skipped") == 3, str(summary.get("chunks_skipped")))
    check("still exactly 1,000,000 rows", rows == 1_000_000, f"rows={rows}")
    check("still no duplicates", distinct == 1_000_000, f"distinct={distinct}")
    check(
        "resumed run verified only what it wrote",
        target_field(summary, "verified_rows") == 625_000,
        f"verified={target_field(summary, 'verified_rows')}",
    )

    # --- DoD 4: empty result is a success ---------------------------------
    print("\nDoD 4 — empty source")
    reset(os.path.join(OUT, "empty_source"), os.path.join(STATE, "empty_source.json"))
    code, log = run(os.path.join("examples", "05_empty_source.yaml"))
    empty_files = glob.glob(os.path.join(OUT, "empty_source", "*.parquet"))
    rows, _ = parquet_rows(os.path.join(OUT, "empty_source", "*.parquet"))
    check("exit 0", code == 0, f"exit={code}")
    check("a Parquet file exists", len(empty_files) == 1, f"files={len(empty_files)}")
    check("it is readable and empty", rows == 0, f"rows={rows}")

    # --- DoD 5: memory limits enforced ------------------------------------
    print("\nDoD 5 — max_batch_rows / max_batch_bytes")
    reset(os.path.join(OUT, "small_batches"), os.path.join(STATE, "small_batches.json"))
    code, log = run(os.path.join("examples", "06_small_batches.yaml"))
    rows, distinct = parquet_rows(os.path.join(OUT, "small_batches", "*.parquet"))
    n_files = writes_files(os.path.join(OUT, "small_batches"))
    check("exit 0", code == 0, f"exit={code}")
    check("batches were split (more files than partitions)", n_files > 8, f"files={n_files}")
    check("no rows lost to batching", rows == 1_000_000 and distinct == 1_000_000, f"rows={rows} distinct={distinct}")

    # --- DoD 7: semantic validation before any query ----------------------
    print("\nDoD 7 — semantic validation")
    for name in ("bad_input.yaml", "bad_expr.yaml", "bad_env.yaml", "bad_setting.yaml"):
        path = os.path.join(fixtures, name)
        if not os.path.exists(path):
            check(f"{name} exit 1", False, "fixture missing")
            continue
        code, log = run(path)
        check(f"{name} rejected with exit 1", code == 1, f"exit={code}")
        check(f"{name} explains itself", "configuration error" in log, log.splitlines()[0][:80] if log else "")

    # --- DoD 8: structured logs -------------------------------------------
    print("\nDoD 8 — structured JSON logs")
    reset(os.path.join(OUT, "simple_copy"), os.path.join(STATE, "simple_copy.json"))
    code, log = run(os.path.join("examples", "01_simple_copy.yaml"))
    logfile = os.path.join(ROOT, ".toolchain", "run_logs.jsonl")
    with open(logfile, "w", encoding="utf-8") as fh:
        fh.write(log)
    checker = subprocess.run(
        [sys.executable, os.path.join(ROOT, "scripts", "check_logs.py"), logfile],
        capture_output=True,
        text=True,
    )
    check("log schema conforms", checker.returncode == 0, checker.stdout.strip().splitlines()[-1])
    check("job_summary emitted", "job_summary" in log)

    # --- DoD 6: atomic checkpoint (crash leaves previous state intact) -----
    print("\nDoD 6 — atomic checkpoint writes")
    csv_state = os.path.join(STATE, "single_partition_csv.json")
    reset(csv_state, csv_state + ".tmp")
    run(os.path.join("examples", "03_single_partition.yaml"))
    check("state file written", os.path.exists(csv_state), csv_state)
    check("no .tmp sidecar left", not os.path.exists(csv_state + ".tmp"))

    # --- CSV sink -----------------------------------------------------------
    print("\nCSV target")
    csv_path = os.path.join(OUT, "single_partition_customers.csv")
    reset(os.path.join(OUT, "single_partition_customers.csv"), os.path.join(STATE, "single_partition_csv.json"))
    code, log = run(os.path.join("examples", "03_single_partition.yaml"))
    n = count_lines(csv_path) - 1 if os.path.exists(csv_path) else -1
    check("exit 0", code == 0, f"exit={code}")
    check("5000 data rows + header", n == 5000, f"rows={n}")

    # --- dual target --------------------------------------------------------
    print("\nDual target (Parquet + CSV, one pass)")
    reset(os.path.join(OUT, "dual_target"), os.path.join(OUT, "dual_target.csv"), os.path.join(STATE, "dual_target.json"))
    code, log = run(os.path.join("examples", "04_csv_target.yaml"))
    rows, _ = parquet_rows(os.path.join(OUT, "dual_target", "*.parquet"))
    n_csv = count_lines(os.path.join(OUT, "dual_target.csv")) - 1
    check("exit 0", code == 0, f"exit={code}")
    check("parquet and csv agree", rows == n_csv == 900_000, f"parquet={rows} csv={n_csv}")

    # --- exit code 2 on runtime failure ------------------------------------
    print("\nExit codes")
    code, log = run(os.path.join("examples", "01_simple_copy.yaml"), {"LDP_PG_URL": "postgres://postgres@127.0.0.1:1/ldp"})
    check("unreachable server exits 2", code == 2, f"exit={code}")

    # --- summary -----------------------------------------------------------
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    print(f"\n{'=' * 60}\n{passed}/{total} checks passed")
    if passed != total:
        print("\nFailures:")
        for name, ok, detail in results:
            if not ok:
                print(f"  - {name} ({detail})")
        return 1
    print("Phase 1 Definition of Done: satisfied.")
    return 0


def last_summary(log: str) -> dict:
    best: dict = {}
    for line in log.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("event") == "job_summary":
            best = obj
    return best


def target_field(summary: dict, field: str):
    """Read a per-target field from the first target in a job_summary."""
    targets = summary.get("targets") or []
    if not targets:
        return None
    return targets[0].get(field)


def read_state(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}


def derived_columns(pattern: str) -> list[str]:
    import pyarrow.dataset as ds

    files = sorted(glob.glob(pattern))
    if not files:
        return []
    t = ds.dataset(files, format="parquet").to_table()
    base = {"id", "name", "status", "amount", "created_at", "region", "unit_price"}
    return [c for c in t.column_names if c not in base]


def count_lines(path: str) -> int:
    try:
        with open(path, encoding="utf-8") as fh:
            return sum(1 for _ in fh)
    except OSError:
        return 0


if __name__ == "__main__":
    sys.exit(main())
