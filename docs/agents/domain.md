# Domain docs

Before you explore this repository, read the domain documents in the list below, and follow the instructions in each later section on how to use them.

The repository has a single context, so one `CONTEXT.md` and one `docs/adr/` folder at the root hold the domain documents for both packages.

## Read these before you explore

- Read `CONTEXT.md` at the repository root, where each term that this repository uses with a fixed meaning has its definition.
- Read every decision record in `docs/adr/` about the area that you're about to work in. Each record's file name is a short summary of its decision, so list the folder to find the records that apply.

When `CONTEXT.md` lacks a term that you need, carry on with your work and note the gap for the `/domain-modeling` skill, which an agent uses to add a term once somebody settles its meaning. Write a new decision record through the `/grill-with-docs` skill, following the instruction in `AGENTS.md`.

## File structure

```text
/
├── CONTEXT.md        ← the glossary of domain terms
├── docs/adr/         ← one numbered file per decision record
├── docs/             ← user-facing guides, which are not domain docs
└── packages/
    ├── spec/         ← the contract that every implementation follows
    └── ts/           ← the reference implementation
```

Treat `packages/spec` as the authority over both `CONTEXT.md` and every decision record, so where the text of a domain document differs from the spec, fix the domain document.

## Use the glossary's vocabulary

When you name a domain concept in your output, whether in an issue title, a refactoring proposal, a hypothesis, or a test name, use the term from `CONTEXT.md` and keep to that one term throughout.

When the glossary lacks a concept that you need, one of two things is true. Either you're inventing a word that appears nowhere in this repository, in which case you should reconsider the word, or you've found a real gap, in which case you should note it for `/domain-modeling`.

## Flag a conflict with a decision record

When you propose a change against an existing decision record, say so explicitly, as in this example:

> _I'm proposing a change against ADR-0004, under which Sirannon's own controller performs automatic failover, and I'd reopen that decision because [your reason]._
