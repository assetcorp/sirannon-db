# Contributing to Sirannon

Thanks for your interest in Sirannon. This guide covers how to set up the repository, run the checks, and propose a change.

## Prerequisites

- Node.js 22 or newer.
- pnpm 11.8.0, the version pinned in `package.json`. Run `corepack enable` and pnpm will match it.

## Set up the repository

```bash
git clone https://github.com/assetcorp/sirannon-db.git
cd sirannon-db
pnpm install
```

## Run the checks

The same checks run in continuous integration, so run them before you open a pull request.

```bash
pnpm build       # build every package
pnpm test        # run the unit test suites
pnpm lint        # run Biome
pnpm typecheck   # run the TypeScript compiler
pnpm format      # apply Biome formatting
```

The replication end-to-end, failover, and soak suites run from the TypeScript package, because they start Docker containers for etcd, gRPC, and fault injection.

```bash
cd packages/ts
pnpm test:e2e        # replication end-to-end scenarios
pnpm test:failover   # Docker-based failover conformance
pnpm test:soak       # long-running soak run
```

The benchmark suite runs from `benchmarks/server`. See [`BENCHMARKS.md`](BENCHMARKS.md) for the methodology and [`benchmarks/server/README.md`](benchmarks/server/README.md) for how to run it against Postgres.

## Repository layout

- `packages/ts` contains the TypeScript library: the core engine, the server and client subpaths, replication, the transports, and the SQLite drivers.
- `packages/spec` contains the language-agnostic specification that every implementation follows, along with its test vectors.
- `packages/ts/examples` contains runnable example projects for Node.js, the browser, the client-server path, and a distributed cluster.
- `benchmarks` contains the benchmark suite: the Python harness under `benchmarks/server`, the report generator under `benchmarks/writeup`, and the cloud provisioning under `benchmarks/cloud`.

## Conventions

- Let Biome format the code. Do not hand-format around it.
- Install dependencies at an exact version with `pnpm add -E`, so the lockfile stays deterministic.
- Write a comment only for a reason a reader cannot infer from the code.
- Anything that changes a wire format, a protocol, or a replication invariant starts from [`packages/spec`](packages/spec), because the specification is a cross-language contract. A change there affects every future implementation.

## Ways to help

Code is one way to help, and it is not the only one. You can improve the documentation, add or clarify an example, write a reproduction for a bug, or sharpen the benchmarks. Each of these is a valued contribution.

## Propose a change

- For a small first change, look for issues labelled `good first issue`.
- For a bug, open an issue with the bug template so the report includes the version, the runtime, and a reproduction.
- For a larger change, open an issue to discuss the design before you write the code.

## Licence

By contributing, you agree your contribution is licensed under the Apache-2.0 licence that covers the project.
