"""plan.py v2 — versioned, prioritized, hierarchical plan tracker on studio.db

Every mutation is logged to plan_events (immutable audit trail), so any past
state is reconstructable. Export generates named whole-plan snapshots (v1, v2...).

Examples:
  py -3.12 scripts/plan.py show
  py -3.12 scripts/plan.py show --status blocked
  py -3.12 scripts/plan.py add task-foo --kind task --parent phase-3 ^
       --title "Do it" --priority critical --rationale "because X"
  py -3.12 scripts/plan.py set task-foo --status blocked --note "waiting" ^
       --reason "addon not installed yet"
  py -3.12 scripts/plan.py history task-foo
  py -3.12 scripts/plan.py export --label initial
"""
import argparse
import sqlite3
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent


def _resolve_db(argv):
    """--db PATH (anywhere on the line) lets FOREIGN workspaces reuse this CLI
    against their own studio.db; default = the sibling of this script."""
    if "--db" in argv:
        i = argv.index("--db")
        if i + 1 < len(argv):
            return Path(argv[i + 1])
    import os
    env = os.environ.get("STUDIO_DB")
    return Path(env) if env else (ROOT / "studio.db")


DB = _resolve_db(sys.argv)

# strip --db <path> from argv so argparse never sees it
if "--db" in sys.argv:
    _i = sys.argv.index("--db")
    del sys.argv[_i:_i + 2]
VERSIONS = ROOT / "plan_versions"

STATUSES = ["todo", "in_progress", "blocked", "done", "dropped"]
PRIORITIES = ["critical", "important", "normal", "optional", "minimal"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE, category TEXT NOT NULL,
    title TEXT NOT NULL, description TEXT DEFAULT '', version INTEGER NOT NULL DEFAULT 1,
    path_blend TEXT, collections TEXT DEFAULT '', polycount INTEGER,
    source TEXT DEFAULT '', status TEXT NOT NULL DEFAULT 'seed',
    notes TEXT DEFAULT '', created_at TEXT DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS shots (
    id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE, scene TEXT NOT NULL,
    script_text TEXT NOT NULL, spec_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft', seed INTEGER, output_dir TEXT,
    created_at TEXT DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS shot_assets (
    shot_id INTEGER REFERENCES shots(id), asset_id INTEGER REFERENCES assets(id),
    asset_version INTEGER NOT NULL, PRIMARY KEY (shot_id, asset_id));
CREATE TABLE IF NOT EXISTS recipes (
    id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE, path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'seed', validated_at TEXT,
    failure_notes TEXT DEFAULT '', notes TEXT DEFAULT '');
CREATE TABLE IF NOT EXISTS plan_items (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('phase','task','step')),   -- grain: high/mid/low
    parent_slug TEXT,                                             -- hierarchy
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'todo'
        CHECK (status IN ('todo','in_progress','blocked','done','dropped')),
    priority TEXT NOT NULL DEFAULT 'normal'
        CHECK (priority IN ('critical','important','normal','optional','minimal')),
    rationale TEXT DEFAULT '',                                    -- why this item exists
    status_note TEXT DEFAULT '',                                  -- live commentary / deviation log
    meta TEXT DEFAULT '{}',                                       -- free-form JSON metadata
    sort INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT);
CREATE TABLE IF NOT EXISTS plan_events (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL,
    ts TEXT DEFAULT (datetime('now')),
    field TEXT NOT NULL,          -- created | status | priority | rationale | status_note
    old_value TEXT, new_value TEXT,
    reason TEXT DEFAULT ''
);
"""


def connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    # migrate legacy plan_items rows that predate v2 columns
    cols = {r["name"] for r in con.execute("PRAGMA table_info(plan_items)")}
    for col, ddl in [
        ("priority", "ALTER TABLE plan_items ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal'"),
        ("rationale", "ALTER TABLE plan_items ADD COLUMN rationale TEXT DEFAULT ''"),
        ("meta", "ALTER TABLE plan_items ADD COLUMN meta TEXT DEFAULT '{}'"),
    ]:
        if col not in cols:
            con.execute(ddl)
    con.commit()
    return con


def log_event(con, slug, field, old, new, reason=""):
    con.execute(
        "INSERT INTO plan_events (slug, field, old_value, new_value, reason) "
        "VALUES (?, ?, ?, ?, ?)",
        (slug, field, str(old) if old is not None else None,
         str(new) if new is not None else None, reason),
    )


def cmd_show(a):
    con = connect()
    q, v = "SELECT * FROM plan_items", []
    clauses = []
    if not a.all:
        clauses.append("status != 'done'")
    if a.kind:
        clauses.append("kind=?"); v.append(a.kind)
    if a.status:
        clauses.append("status=?"); v.append(a.status)
    if a.priority:
        clauses.append("priority=?"); v.append(a.priority)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " ORDER BY sort, id"
    rows = con.execute(q, v).fetchall()
    pri_mark = {"critical": "!!!", "important": "!! ", "normal": "   ",
                "optional": ".  ", "minimal": "   "}
    for r in rows:
        print(f"{pri_mark.get(r['priority'],'   ')} [{r['status']:>11}] ({r['kind']:<5}) "
              f"{r['slug']}  {r['title']}")
        if r["rationale"]:
            print(f"               why: {r['rationale']}")
        if r["status_note"]:
            print(f"               \\_ {r['status_note']}")
    if not rows:
        print("(no rows)")
    con.close()


def cmd_add(a):
    con = connect()
    con.execute(
        """INSERT INTO plan_items
           (slug, kind, parent_slug, title, status, priority, rationale, status_note)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (a.slug, a.kind, a.parent, a.title, a.status, a.priority,
         a.rationale or "", a.note or ""),
    )
    log_event(con, a.slug, "created", None, a.status, a.reason or "")
    con.commit()
    print(f"[added] {a.slug} ({a.kind}, {a.status}, {a.priority})")


def _mutate(a):
    con = connect()
    row = con.execute("SELECT * FROM plan_items WHERE slug=?", (a.slug,)).fetchone()
    if not row:
        print(f"[NOT FOUND] {a.slug}"); con.close(); return
    changes = []
    for field, argval in (("status", a.status), ("priority", a.priority)):
        if argval and argval != row[field]:
            changes.append((field, row[field], argval))
    if a.note is not None and a.note != row["status_note"]:
        changes.append(("status_note", row["status_note"], a.note))
    if a.rationale is not None and a.rationale != row["rationale"]:
        changes.append(("rationale", row["rationale"], a.rationale))
    if not changes:
        print(f"[no changes] {a.slug}"); con.close(); return
    sets = ", ".join(f"{f}=?" for f, _, _ in changes) + ", updated_at=datetime('now')"
    vals = [new for _, _, new in changes] + [a.slug]
    con.execute(f"UPDATE plan_items SET {sets} WHERE slug=?", vals)
    for field, old, new in changes:
        log_event(con, a.slug, field, old, new, a.reason or "")
    con.commit()
    for field, old, new in changes:
        print(f"[{a.slug}] {field}: {old!r} -> {new!r}")
    con.close()


def cmd_set(a): _mutate(a)


def cmd_done(a):
    class A: pass
    ns = A(); ns.slug = a.slug; ns.status = "done"; ns.priority = None
    ns.note = None; ns.rationale = None; ns.reason = a.reason or ""
    _mutate(ns)


def cmd_history(a):
    con = connect()
    rows = con.execute(
        "SELECT ts, field, old_value, new_value, reason FROM plan_events "
        "WHERE slug=? ORDER BY ts, id", (a.slug,)
    ).fetchall()
    for r in rows:
        print(f"{r['ts']}  {r['field']:<12} {r['old_value'] or '-'} -> {r['new_value'] or '-'}"
              + (f"   ({r['reason']})" if r["reason"] else ""))
    if not rows:
        print("(no events)")
    con.close()


def cmd_export(a):
    VERSIONS.mkdir(exist_ok=True)
    n = len(list(VERSIONS.glob("plan_v*.md"))) + 1
    label = f"_{a.label}" if a.label else ""
    path = VERSIONS / f"plan_v{n:02d}{label}.md"
    con = connect()
    rows = con.execute("SELECT * FROM plan_items ORDER BY sort, id").fetchall()
    lines = [
        f"# Plan snapshot v{n:02d}{(' — ' + a.label) if a.label else ''}",
        f"_Generated {__import__('datetime').datetime.now().isoformat(timespec='seconds')}_",
        "",
    ]
    for r in rows:
        lines.append(f"- **[{r['status']}] {r['slug']}** ({r['kind']}, {r['priority']}) — {r['title']}")
        if r["rationale"]:
            lines.append(f"    - why: {r['rationale']}")
        if r["status_note"]:
            lines.append(f"    - note: {r['status_note']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    con.close()
    print(f"[exported] {path}")


def main():
    p = argparse.ArgumentParser(description="versioned plan tracker")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("show")
    s.add_argument("--all", action="store_true")
    s.add_argument("--kind", choices=["phase", "task", "step"])
    s.add_argument("--status", choices=STATUSES)
    s.add_argument("--priority", choices=PRIORITIES)
    s.set_defaults(fn=cmd_show)

    a = sub.add_parser("add")
    a.add_argument("slug")
    a.add_argument("--kind", default="task", choices=["phase", "task", "step"])
    a.add_argument("--parent")
    a.add_argument("--title", required=True)
    a.add_argument("--status", default="todo", choices=STATUSES)
    a.add_argument("--priority", default="normal", choices=PRIORITIES)
    a.add_argument("--rationale"); a.add_argument("--note"); a.add_argument("--reason")
    a.set_defaults(fn=cmd_add)

    t = sub.add_parser("set"); t.add_argument("slug")
    t.add_argument("--status", choices=STATUSES); t.add_argument("--priority", choices=PRIORITIES)
    t.add_argument("--note"); t.add_argument("--rationale"); t.add_argument("--reason")
    t.set_defaults(fn=cmd_set)

    d = sub.add_parser("done"); d.add_argument("slug"); d.add_argument("--reason")
    d.set_defaults(fn=cmd_done)

    h = sub.add_parser("history"); h.add_argument("slug"); h.set_defaults(fn=cmd_history)

    e = sub.add_parser("export"); e.add_argument("--label"); e.set_defaults(fn=cmd_export)

    args = p.parse_args()
    getattr(sys.modules[__name__], f"cmd_{args.cmd}")(args)


if __name__ == "__main__":
    main()
