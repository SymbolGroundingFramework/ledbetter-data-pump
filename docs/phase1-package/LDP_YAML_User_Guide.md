# Ledbetter Data Pump (LDP)
## User Guide: Writing Pipeline YAML (Phase 1)

**Version 1.0**  
**Date: November 2026**  
**Audience: Humans and coding agents**

This guide explains how to write a valid `pipeline.yaml` file for Phase 1 of the Ledbetter Data Pump.  
It is deliberately precise so both humans and LLM agents can produce correct files.

---

## 1. File Structure Overview

Every pipeline file has this shape:

```yaml
version: "1.0"
pipeline:
  name: string
  settings: { ... }          # optional
  resources:
    connections: [ ... ]
  dataflow:
    sources: [ ... ]
    transformations: [ ... ] # optional
    targets: [ ... ]
```

- `version` must be exactly `"1.0"`.
- Exactly one source is supported in Phase 1.
- All `id` values must be unique.
- Every `input` must reference an existing `id`.

---

## 2. Connections

```yaml
resources:
  connections:
    - name: "pg"
      type: "postgres"
      url: "env://PG_URL"          # preferred
      # or
      # url: "postgres://user:pass@host:5432/dbname"
```

**Rules**
- `type` must be `"postgres"` in Phase 1.
- Prefer `env://VARIABLE_NAME`. The variable must be set in the environment.
- Plain URLs are allowed but passwords will be redacted in logs.
- Connection names are referenced by sources.

---

## 3. Sources

```yaml
sources:
  - id: "src"
    connection: "pg"
    query: |
      SELECT id, name, status, amount, created_at
      FROM customers
      WHERE created_at >= '2026-01-01'
    partitioning:
      type: "range"          # or "none"
      column: "id"           # required for range
      chunks: 8              # required for range
```

**Partitioning**
- `type: range` — engine runs `MIN`/`MAX` on the column and splits into `chunks` ranges.
- `type: none` — single partition, entire query result.
- Partition column must be an integer type (BIGINT/INTEGER).
- The connector automatically adds `WHERE column >= $1 AND column < $2`.

---

## 4. Transformations (Optional)

Phase 1 supports only `filter` and `derive`.

### Filter
```yaml
- id: "active_only"
  input: "src"
  type: "filter"
  condition: "status = 'ACTIVE' AND amount > 0 AND created_at IS NOT NULL"
```

### Derive
```yaml
- id: "with_tax"
  input: "active_only"
  type: "derive"
  fields:
    - name: "taxed_amount"
      expression: "amount * 1.0825"
    - name: "processed_at"
      expression: "now()"
```

**Phase 1 Expression Rules**
- Columns, numeric/string/boolean/null literals
- Comparisons: `= != > >= < <=`
- Logical: `AND OR NOT` (use parentheses)
- Arithmetic: `+ - * /`
- `IS NULL` / `IS NOT NULL`
- Functions: `coalesce(a, b)`, `now()`
- Strings use single quotes: `'ACTIVE'`
- Nulls follow SQL three-valued logic

**Not supported yet**: `IN`, string functions, aggregates, subqueries.

---

## 5. Targets

### Parquet
```yaml
- id: "out"
  input: "with_tax"
  type: "parquet"
  path: "./output/customers/"
  compression: "zstd"       # zstd | snappy | none
```

### CSV
```yaml
- id: "out_csv"
  input: "with_tax"
  type: "csv"
  path: "./output/customers.csv"
```

- `path` for Parquet is a directory (one or more files will be written).
- `path` for CSV is a file.
- Files are written via temporary names + atomic rename.

---

## 6. Settings

```yaml
settings:
  max_threads: 8                    # default 8
  state_file: ".ldp/my_job.json"    # default ".ldp/<pipeline_name>.json"
  max_batch_rows: 65536             # default 65536
  max_batch_bytes: 134217728        # default ~128 MB
```

All settings are optional.

---

## 7. Complete Working Example

```yaml
version: "1.0"
pipeline:
  name: "customer_export"
  settings:
    max_threads: 4
    state_file: ".ldp/customer_export.json"
  resources:
    connections:
      - name: "pg"
        type: "postgres"
        url: "env://PG_URL"
  dataflow:
    sources:
      - id: "src"
        connection: "pg"
        query: "SELECT id, name, status, amount, created_at FROM customers"
        partitioning:
          type: "range"
          column: "id"
          chunks: 4
    transformations:
      - id: "active_only"
        input: "src"
        type: "filter"
        condition: "status = 'ACTIVE' AND amount > 0"
      - id: "with_tax"
        input: "active_only"
        type: "derive"
        fields:
          - name: "taxed_amount"
            expression: "amount * 1.0825"
    targets:
      - id: "parquet_out"
        input: "with_tax"
        type: "parquet"
        path: "./output/customers/"
        compression: "zstd"
```

**Expected Postgres table**
```sql
CREATE TABLE customers (
    id BIGINT PRIMARY KEY,
    name TEXT,
    status TEXT,
    amount DOUBLE PRECISION,
    created_at TIMESTAMP
);
```

---

## 8. Common Validation Errors

| Error | Cause | Fix |
|-------|-------|-----|
| Unknown input reference | `input` does not match any `id` | Check spelling and order |
| Cycle detected | Stage A inputs B and B inputs A | Remove the cycle |
| Partition column missing | `column` not plausible for query | Use a column that exists in the SELECT |
| Unsupported expression | Used `IN` or string function | Stay within Phase 1 subset |
| Missing env var | `env://PG_URL` not set | Export the variable before running |
| Version mismatch | `version` not `"1.0"` | Set exactly `"1.0"` |

---

## 9. Phase 1 Limitations (Do Not Use Yet)

- Multiple sources
- Conditional splits
- Snowflake, SQL Server, Oracle
- `IN` lists, string functions, complex expressions
- Incremental / watermark loads
- Atomic swap or merge modes
- Custom null handling or encoding options

---

## 10. Quick Checklist for Agents and Humans

- [ ] `version: "1.0"`
- [ ] Unique `id`s
- [ ] Every `input` resolves
- [ ] Postgres connection uses `env://` when possible
- [ ] Partition column is integer
- [ ] Expressions use only allowed operators and functions
- [ ] Parquet `path` is a directory
- [ ] State file path is writable

---

**End of User Guide**

A file that follows this guide will pass Phase 1 validation and execute correctly.
