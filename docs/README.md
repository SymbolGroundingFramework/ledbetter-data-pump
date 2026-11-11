# Documentation

This folder is the **specification** that the engine implements, kept as
delivered so the code can always be checked against it.

| Document | What it settles |
|---|---|
| [LDP_Phase1_Implementation_Contract.md](LDP_Phase1_Implementation_Contract.md) | **the build contract**: types, traits, crate layout, expression subset, Definition of Done |
| [LDP_Architecture_Whitepaper.md](LDP_Architecture_Whitepaper.md) | overall architecture, YAML schema, goals and non-goals |
| [LDP_Failure_Modes_Addendum.md](LDP_Failure_Modes_Addendum.md) | edge cases and their required behaviour, phased |
| [LDP_Phase1_Operational_Details.md](LDP_Phase1_Operational_Details.md) | partition algorithm, log schema, CLI contract, exit codes, secret handling |
| [LDP_YAML_User_Guide.md](LDP_YAML_User_Guide.md) | how to author a `pipeline.yaml` |
| [LDP_Roadmap.md](LDP_Roadmap.md) | Phase 1 → 4 sequencing |

PDF copies of each document are included for offline reading.

## Not the spec

- `phase1-package/` — the original download the project started from (same
  documents, plus its own `examples/`). Kept for provenance.
- `history/` — the design-session transcript and the Gemini chat export. Useful
  for "why is it like this?", not authoritative.

Where the implementation knowingly differs from the contract, the reason is
recorded in [../PLAN.md](../PLAN.md) §5 ("deviations register") rather than
buried here.
