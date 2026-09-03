# Working in this repository

`packages/ts` is the reference implementation. `packages/spec` is the contract every implementation follows, and it outranks the TypeScript code.

Read the file you're editing and follow what it already does. The rules below cover what that reading leaves out.

## Rules

- Re-read and apply the user's writing guidelines fully, if available, whenever you are writing TS-Doc or any prose in the repo.
- Leave `packages/spec` alone. The developer owns every spec change and makes it directly. When your work needs a different wire format, value encoding, client-facing error code, or replication invariant, say so and wait for their decision. Write no code against a spec change they have not approved.
- Treat the spec test vectors as fixed. When your change breaks that test, revert the change or raise it with the developer.
- Keep every source file under 400 lines. Measure a file when you touch it, and split it before your change pushes it over. The limit covers source only, because a test file grows with the cases it covers, and splitting one breaks up a suite that reads best in one place.
- Keep everything tree-shakeable. A bundler must be able to drop any capability the caller never imports, so add no barrel that pulls siblings in, and register no optional capability at module load. Make each new capability reachable by direct import, and let the caller opt into it.
- Put every statement against a `_sirannon_*` table in `core/system-catalog/`, one module per table, exported as a function that owns both the SQL and the shape it returns. Copy `meta-table.ts`. Keep SQL out of the call site, whether it creates, reads, or writes.
- Name each of those functions after the verb of the statement it runs: `select`, `insert`, `update`, `delete`, or `upsert`, and `selectMax`, `selectMin`, or `selectCount` for an aggregate. Use `ensure` for the idempotent `CREATE`, `prepare<Verb>` when you return a prepared statement for a loop, and a `Sql` suffix when you return the statement text. A `PRAGMA` helper and a function wrapping several statements take a descriptive name instead.
- Take internal table names from the constants in `core/internal-tables.ts`. Add columns with `ensureColumn`. Keep `CREATE TRIGGER` in `core/cdc/trigger-sql.ts`.
- Never propose refusing, narrowing, or deferring a capability because implementing it properly is harder. Read how established databases solve it and what this repository already does, then propose that, and when ease of implementation is your real reason, say so plainly instead of presenting it as a recommendation.
- Let a `SirannonError` propagate unchanged, because its code is what the server maps to an HTTP status.
- Give every exported function and every member of `Sirannon` and `Database` a TSDoc comment, because `tsup` carries it into the published `.d.ts`, where it becomes what a caller reads on hover in their editor. Open with what the caller gets, keep the prose plain and easy to understand, and describe each parameter and the return value. Put `@public`, `@internal`, and every other tag below that description, because a block opening with a tag hovers blank. Reference another symbol with `{@link Database.query}`, which resolves inside this package. Paste no URL into a comment. That TSDoc is the only comment the repository carries: a non-exported function, a catch block, a test, a harness, and a fixture carry none, whatever the reason seems to be.
- Run `pnpm lint`, `pnpm typecheck`, `pnpm build`, `pnpm check:bundle`, and `pnpm test` before you report a change done.

## Gotchas

- **Node builtins break the browser entries.** Lint, typecheck, and the tests all pass when a file under `core/` or `client/` imports `node:crypto`; only `pnpm check:bundle` reports it, and it reads `dist`, so build first. Use the hand-written SHA-256 and random-hex helpers, and route Node-only capabilities through optional driver members.
- **`defineDriver` copies members by an explicit name list.** A new optional member of `SQLiteDriver` type-checks everywhere, and `defineDriver` omits it from every driver at runtime until you add its line there. Give each optional member a fallback in core or an explicit `SirannonError`.
- **Both tsconfigs exclude the bun, wa-sqlite, and expo drivers.** `pnpm typecheck` reports nothing about them, so read them yourself after you change a driver interface.
- **Drivers read every integer as `BigInt`** and the driver value layer narrows it back, keeping only what exceeds the safe range. Leave `allRaw` un-narrowed, because that is its purpose.
- **Every subscriber receives the same `ChangeEvent` object.** Use the non-mutating encoder on the CDC path, and keep the in-place encoder for rows freshly materialised for a single response. The gRPC replication transport carries native values with no envelope.
- **`assertSqlAllowed` refuses `_sirannon` identifiers from the public query API.** Take a raw writer connection from the pool, or mark your statement `trusted: true`, and leave the guard as it is.
- **A write runs inside the write gate, then the writer lock,** and takes its connection from the pool inside that scope. Give a new controller the `runExclusive` and `acquireWriter` closures every other controller receives. Schedule work from inside a held write through the writer lock's detached path, or it runs inside one caller's transaction.
- **The streamed backup route needs a compiled extension.** Its C source is in `native/vfs/`, `pnpm build:vfs` builds it for the host, and the streamed backup tests skip without it. A process loads that extension into one SQLite build, so give each driver's streaming test a file of its own.
- **Vitest collects `src/**/__tests__/**/*.test.ts` and nothing else.** Put every test file under a `__tests__` directory. A file under `src/__tests__/e2e/` runs under `pnpm test:e2e` on every pull request, one under `src/__tests__/large/` runs under `pnpm test:large` on a runner of its own, and adding the `.soak.test.ts` suffix moves it to `pnpm test:soak`, which only the scheduled `Replication Soak` workflow runs.
- **The end-to-end suite runs two nodes in one process over loopback gRPC and needs no Docker.** Import its harness rather than building certificates, nodes, or temp directories by hand, and assert convergence with `waitForReplica`. Only the failover suite needs Docker.
- **A generator writes `BENCHMARKS.md`, `nx release` writes `CHANGELOG.md`, and `pnpm proto:gen` writes the gRPC directory.** Edit the source each one reads. Continuous integration fails when `BENCHMARKS.md` differs from what the generator produces.
- **The supply-chain gate refuses a package published less than four days ago.** Install with `pnpm add -E` from inside the target workspace.

Read `CONTRIBUTING.md` for the house style. The `/grill-with-docs` skill writes the decision records in `docs/adr/`, so load that skill for an architectural decision whenever it's available. Preserve the automatic-failover invariants when you change replication code.

## Agent skills

### Issue tracker

Issues live as GitHub issues on `assetcorp/sirannon-db`, and every operation runs through the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, each label string equal to its name. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and the existing `docs/adr/` at the repo root cover both packages. See `docs/agents/domain.md`.
