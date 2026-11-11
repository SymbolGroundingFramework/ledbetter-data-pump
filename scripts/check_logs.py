#!/usr/bin/env python3
"""Validate LDP structured logs against the schema in
docs/LDP_Phase1_Operational_Details.md Â§2.

Usage:
    py -3.12 scripts/check_logs.py <logfile.jsonl>

Every line must be one JSON object with timestamp/level/event/pipeline, and
each event type must carry its required fields. Exit 1 on any violation.
"""
import json
import sys

REQUIRED_ALWAYS = {"timestamp", "level", "event", "pipeline"}

REQUIRED_BY_EVENT = {
    "chunk_started": {"chunk_id", "partition_start", "partition_end"},
    "chunk_completed": {"chunk_id", "rows", "duration_ms", "rows_per_sec"},
    "chunk_skipped": {"chunk_id"},
    "error": {"chunk_id", "message"},
    "job_summary": {
        "status",
        "chunks_total",
        "chunks_completed",
        "rows_read",
        "rows_written",
        "duration_ms",
        "error",
    },
}

LEVELS = {"ERROR", "WARN", "INFO", "DEBUG"}


def main(path: str) -> int:
    problems = []
    events = {}
    with open(path, "r", encoding="utf-8-sig") as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                problems.append(f"line {lineno}: not JSON ({exc})")
                continue
            if not isinstance(obj, dict):
                problems.append(f"line {lineno}: not a JSON object")
                continue
            missing = REQUIRED_ALWAYS - obj.keys()
            if missing:
                problems.append(f"line {lineno}: missing {sorted(missing)}")
                continue
            if obj["level"] not in LEVELS:
                problems.append(f"line {lineno}: bad level {obj['level']!r}")
            event = obj["event"]
            events[event] = events.get(event, 0) + 1
            need = REQUIRED_BY_EVENT.get(event, set())
            gap = need - obj.keys()
            if gap:
                problems.append(f"line {lineno} ({event}): missing {sorted(gap)}")

    if "job_summary" not in events:
        problems.append("no job_summary event was emitted (it is mandatory)")

    print(f"events: {dict(sorted(events.items()))}")
    if problems:
        print(f"FAIL: {len(problems)} problem(s)")
        for p in problems:
            print("  -", p)
        return 1
    print(f"OK: {sum(events.values())} log lines conform to the Phase 1 schema")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))

