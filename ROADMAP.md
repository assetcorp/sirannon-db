# Roadmap

A language-agnostic specification in [`packages/spec`](packages/spec) defines Sirannon. It covers the driver contract, the core layer, the replication engine, the transports, the server, the client, device sync, and the error taxonomy. The TypeScript package is the reference implementation, but it has to follow the specification like every other implementation.

This roadmap sets out the features that Sirannon has today and the ones that we plan to build next. We update it as the work proceeds, which is why it has no dates. To propose or discuss an item, open an issue.

## Available now

- **Core engine.** Queries, transactions, connection pooling, change data capture, live queries, migrations, backups, hooks, metrics, and multi-tenant lifecycle management work inside your process on any supported SQLite driver.
- **Server and client.** The server subpath serves any Sirannon instance over HTTP and WebSocket. The client SDK has the same API as the core. When its connection drops, the SDK reconnects and restores each subscription.
- **Registered operations.** By default, a server executes only the reads and writes that you register under a name, so it accepts no SQL from the network. Code generation produces typed client references from those registered operations. A live query also works over a registered read.
- **Primary-replica replication.** A single primary stamps each change with a Hybrid Logical Clock and groups the changes into checksummed batches. It then replicates those batches to read replicas over gRPC with mutual TLS. This replication path also includes conflict resolvers, first sync, read concerns, and write concerns.

## In progress

- **Coordinator-backed failover toward stable.** The etcd coordinator and automatic failover are the newest parts of the project. Today, a conformance test in Docker checks promotion and demotion under injected faults. Before we mark failover as stable, we will test more kinds of failure, prove recovery under sustained load, and write the guidance for operating it.
- **Device sync toward stable.** With device sync, the app on an end user's device already works offline on a local Sirannon database. Sirannon keeps that database in step with one server database in both directions, through push, live pull, snapshot resync, and a migration handshake. We want to see device sync in production use on mobile devices and in browsers before we mark it as stable.
- **Type declarations for every driver.** The Bun and Expo drivers work today, but they have no TypeScript declarations yet. Once we add those declarations, both drivers will match the better-sqlite3, Node, and wa-sqlite drivers.

## Planned

- **A second-language implementation.** The specification lists TypeScript, Go, Rust, and Python as targets. A second implementation is our headline item, since building one is how we show that the specification is portable across languages. Which language comes first is open, so we will compare each candidate's runtime footprint, concurrency model, and ecosystem before we choose.
- **Scaling beyond a single node's disk.** Each Sirannon node stores its data in one SQLite file on one machine. For a dataset larger than that machine's disk, you will therefore need a database such as Postgres today. How Sirannon will store data beyond one machine is an open question, with sharding and tiered storage among the routes that we are considering.
- **Time-to-live (TTL) for rows.** You will be able to set a timestamp column and a period on a table so that Sirannon deletes each row once its timestamp is older than that period. Sirannon will keep each row whose timestamp is empty. In an AI agent's memory, for example, the code can write into that column the time at which a newer value replaces a fact. Sirannon will then delete each replaced value after the period that you set, while the current value stays. Sessions and one-time codes can expire in the same way. Before we build it, we need to settle how Sirannon will sweep a database that it closes after an idle period, and how the primary alone will delete expired rows and pass those deletes to each replica. The sweep will also have to clear a deleted row's text from the change log and from the file's free pages, because a deletion for privacy has to remove those copies too.

## How to get involved

Read [CONTRIBUTING.md](CONTRIBUTING.md) to set up the repository. Look for issues labelled `good first issue` when you want to make a first change. For a change to a wire format, a protocol, or a replication invariant, start from the specification in [`packages/spec`](packages/spec).
