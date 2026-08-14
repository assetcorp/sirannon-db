# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is single-context: one `CONTEXT.md` and one `docs/adr/` at the root cover both packages.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`docs/adr/`** — read the decision records that touch the area you're about to work in. Four exist today, covering replication end-to-end testing, the engine test observability API, in-process chaos hooks, and Sirannon-owned automatic failover.

`CONTEXT.md` doesn't exist yet, so **proceed silently** when you can't find it. Don't flag its absence; don't suggest creating it upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates it lazily when terms actually get resolved. `/grill-with-docs` is already the route this repo uses for a new decision record, as `AGENTS.md` says.

## File structure

```
/
├── CONTEXT.md                         ← not written yet
├── docs/adr/
│   ├── 0001-replication-e2e-architecture.md
│   ├── 0002-engine-test-observability-api.md
│   ├── 0003-chaos-via-in-process-hooks.md
│   └── 0004-sirannon-owned-automatic-failover.md
├── docs/                              ← user-facing guides, not domain docs
└── packages/
    ├── spec/                          ← the contract every implementation follows
    └── ts/                            ← the reference implementation
```

`packages/spec` outranks both `CONTEXT.md` and any decision record. Where a domain doc and the spec disagree, the spec wins and the domain doc is the thing to fix.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal, and it means one of two things: either you're inventing language the project doesn't use, in which case reconsider, or there's a real gap, in which case note it for `/domain-modeling`.

## Flag ADR conflicts

If your output contradicts an existing decision record, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0004 (Sirannon-owned automatic failover), but worth reopening because…_
