# LDP Phase 1 Example Pipelines

These YAML files are valid Phase 1 pipelines.  
They all expect the same Postgres table and the environment variable `PG_URL`.

## Expected Table

```sql
CREATE TABLE customers (
    id          BIGINT PRIMARY KEY,
    name        TEXT,
    status      TEXT,
    amount      DOUBLE PRECISION,
    created_at  TIMESTAMP
);
```

## Environment

```bash
export PG_URL="postgres://user:password@localhost:5432/mydb"
```

## Examples

| File | Description |
|------|-------------|
| `01_simple_copy.yaml` | Straight Postgres → Parquet copy with range partitioning (4 chunks). No transforms. |
| `02_filter_and_derive.yaml` | Filter to active rows with positive amount, then derive `taxed_amount` and `processed_at`. |
| `03_single_partition.yaml` | Uses `partitioning.type: none` (single worker). Useful for small results or tables without a good integer key. |
| `04_csv_target.yaml` | Filtered export to a single CSV file instead of Parquet. |

## Running

```bash
ldp run examples/01_simple_copy.yaml
ldp run examples/02_filter_and_derive.yaml
# etc.
```

All examples write output under `./output/` and state under `.ldp/`.
