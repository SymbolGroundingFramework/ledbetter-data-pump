# Ledbetter Data Pump (LDP)
## Product Roadmap

**Version 1.0**  
**Date: November 2026**  
**Status: Planning document**

This roadmap shows the intended evolution of LDP from a working Phase 1 engine to a production-grade data pump.  
It is a planning artifact, not an implementation contract. Detailed specs are written only when a phase is about to start.

---

## Guiding Principles

- Ship a correct, restartable core before adding surface area.
- Prefer operational safety (idempotency, clear failure modes, observability) over feature count.
- Keep the YAML model and Arrow-based engine stable; extend rather than rewrite.
- Each phase should be independently useful.

---

## Phase 1 — Core Engine (Current)

**Goal:** A working, restartable Postgres → Parquet/CSV pump with basic transforms.

**Includes**
- YAML config + semantic validation
- Arrow RecordBatch data path
- Range and single partitioning
- Filter + derive (restricted expression subset)
- Atomic checkpointing
- Postgres source + Parquet/CSV sinks
- Structured logging
- CLI
- Core edge-case handling defined for Phase 1

**Definition of Done**  
See the Phase 1 Implementation Contract.

**Status:** Fully specified. Ready to build.

---

## Phase 2 — Production Hardening

**Goal:** Safe for unattended, repeated, and failure-prone production use.

**Includes**
- Full Dead Letter Queue + error budgets
- Schema policy (strict / lenient) + centralized type mapping improvements
- Multi-target consistency policy
- Incremental loads via watermarks (+ optional lookback)
- Graceful shutdown and cancellation
- Concurrent-run protection
- Additional connectors: Snowflake, SQL Server (and/or Oracle)
- Conditional split
- Improved constraint-violation handling
- Run summary and basic progress reporting

**Why this phase**  
Turns the engine from “works on the happy path” into something that can be left running and restarted with confidence.

**Depends on:** Phase 1 complete and validated with real jobs.

---

## Phase 3 — Richer Transforms & Operational Maturity

**Goal:** Handle more complex pipelines without leaving the LDP model, and improve operability.

**Includes**
- Richer expression language (via DataFusion or equivalent)
- Additional transform types (rename, drop, simple casts)
- Better skew detection and partition statistics
- Optional spill-to-disk for large batches
- Prometheus-style metrics endpoint
- More file-format options and tuning (row-group size, dictionary encoding, etc.)
- Improved CSV options (delimiter, header, encoding)
- Basic data-quality assertions (row counts, null thresholds)

**Why this phase**  
Removes the most common reasons users would otherwise drop to external tools for light transformation or observability.

**Depends on:** Phase 2 in production use.

---

## Phase 4 — Ecosystem & Scale (Optional / Future)

**Goal:** Broader adoption and larger-scale use cases.

**Possible items** (prioritized by demand)
- Python bindings (PyO3 + Arrow)
- Additional sources/targets (MySQL, BigQuery, S3/GCS direct, etc.)
- More sophisticated partitioning strategies
- Lightweight control-plane API or scheduler integration
- Distributed / multi-node execution (only if single-node limits are actually hit)
- UI or enhanced validation tooling for YAML

**Why this phase**  
Only pursued if Phase 1–3 prove the core model and real usage demands the extra surface area.

---

## Explicit Non-Goals (Across All Phases)

- Full SQL query engine or complex multi-table joins
- Real-time streaming / CDC
- Graphical pipeline designer
- Replacement for a data warehouse or transformation framework (dbt, Spark, etc.)
- Arbitrary user-defined code execution inside the engine

---

## Sequencing Summary

| Phase | Focus | When to start |
|-------|--------|---------------|
| 1 | Core engine + restartability | Now |
| 2 | Production safety & more connectors | After Phase 1 is proven |
| 3 | Richer transforms & observability | After Phase 2 is in use |
| 4+ | Ecosystem & scale | Demand-driven |

---

## How to Use This Roadmap

- Treat Phase 1 as the only fully committed scope.
- Revisit and re-prioritize Phase 2+ after Phase 1 has run real workloads.
- Write detailed implementation contracts only for the phase about to begin.
- Keep this document updated when major sequencing decisions change.

---

**End of Roadmap**
