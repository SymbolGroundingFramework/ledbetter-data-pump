# Contributing

Thanks for looking. This is a small, deliberately-scoped project; the fastest
way to be useful is to pick something from `PLAN.md` §4 (Phase 2) or fix a
limitation listed in `README.md`.

## Ground rules

1. **The specification is the contract.** `docs/LDP_Phase1_Implementation_Contract.md`
   defines what Phase 1 is. If a change contradicts it, change the doc first and
   say why — deviations are recorded in `PLAN.md` §5, not smuggled in.
2. **No Phase 2 features in Phase 1 PRs.** DLQ, watermarks, schema policy,
   TLS, SQL targets: each has its own acceptance criteria in `PLAN.md` §4 and
   should land with tests that prove them.
3. **A change is done when the artifact is verified, not when it compiles.**
   Add or update a check in `scripts/verify_dod.py` (or an integration test) for
   anything that touches data movement.

## Getting set up

Standard Rust works on Linux/macOS:

```bash
cd ldp
cargo test --workspace          # unit tests
```

On Windows this machine's specific toolchain quirks (no MSVC; MinGW from
Strawberry Perl) are documented in `ENVIRONMENT.md`; use `scripts\cargo.ps1`.

A real Postgres enables the integration tests:

```bash
export LDP_TEST_PG_URL=postgres://postgres@127.0.0.1:5432/postgres
export RUST_TEST_THREADS=1      # they share a table; do not parallelise
cargo test -p ldp-cli --test integration_postgres
```

## Before opening a PR

```bash
cd ldp
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI runs exactly these, plus the integration suite against a Postgres service.

## Style

- Comments explain **why**, not what. If a line exists because of a bug, say
  which bug (`LESSONS.md` has the numbers).
- Prefer an explicit error message that tells an operator what to do over a
  clever one that does not.
- Public items get doc comments; anything touching type mapping belongs in
  `ldp-arrow`, not in a connector.
