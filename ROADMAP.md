# Roadmap

Sirannon is defined by a language-agnostic specification in [`packages/spec`](packages/spec). It covers the driver contract, the core layer, the replication engine, the transports, the server, the client, and the error taxonomy. The TypeScript package is the reference implementation, and every other implementation is checked against the specification.

This roadmap sets out where the project is heading. It covers direction and intent, and it will change as the work proceeds. It does not commit to dates. To propose or discuss an item, open an issue.

## Available now

- **Core engine.** Queries, transactions, connection pooling, change data capture, migrations, backups, hooks, metrics, and multi-tenant lifecycle management run inside your process on any supported SQLite driver.
- **Server and client.** Any Sirannon instance goes over HTTP and WebSocket through the server subpath, and the client SDK mirrors the core API with reconnection and subscription restore.
- **Primary-replica replication.** A single primary stamps changes with a Hybrid Logical Clock, groups them into checksummed batches, and replicates them to read replicas over gRPC with mutual TLS. Conflict resolvers, initial sync, and write concerns are part of this path.

## In progress

- **Coordinator-backed failover toward stable.** The etcd coordinator and automatic failover are the newest parts of the project. A Docker conformance run exercises promotion and demotion under fault injection today. The work ahead widens the failure coverage, proves recovery under sustained load, and settles the operational guidance needed to mark this stable.
- **Type declarations for every driver.** The Bun and Expo drivers run today but have no TypeScript declarations yet. Adding them brings both to the same footing as the better-sqlite3, Node, and wa-sqlite drivers.

## Planned

- **A second-language implementation.** The specification names TypeScript, Go, Rust, and Python as targets, and a second implementation is the headline item, because it proves the specification is portable across languages. The choice of which language to build first is open, and the decision will weigh runtime footprint, the concurrency model, and the ecosystem each language reaches.
- **Scaling beyond a single node's disk.** Each Sirannon node uses one SQLite file on one machine, so a dataset larger than local storage is where Postgres wins today, as the [benchmarks](packages/ts/benchmarks/BENCHMARKS.md) set out plainly. How Sirannon handles data beyond a single machine, whether through sharding, tiered storage, or another route, is an open question. We have not settled on a design.

## How to get involved

Read [CONTRIBUTING.md](CONTRIBUTING.md) to set up the repository, and look for issues labelled `good first issue` to make a first change. For anything that touches a wire format, a protocol, or a replication invariant, start from the specification in [`packages/spec`](packages/spec).
