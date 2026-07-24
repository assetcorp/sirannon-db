# Sirannon Specification

This document defines the language-agnostic specification for Sirannon. Sirannon
turns SQLite databases into a networked, distributed data layer with real-time
subscriptions, multi-node replication, offline-first device sync, and schema
management. Every implementation must follow the contracts defined here. Where a behaviour is left to the runtime, this specification says so. The TypeScript package is the reference implementation, but no contract here depends on it; the wire formats, value encodings, and behaviours are the source of truth.

---

## Architecture

Sirannon is organised into layers, each depending only on the layers below it.

```text
+-----------------------------------------------+
| Application layer: server, client, device sync |
+-----------------------------------------------+
| Replication and transport layer                |
| Engine, HLC, topology, coordinator failover    |
+-----------------------------------------------+
| Database orchestration layer                   |
| Registry, database, hooks, lifecycle, metrics  |
+-----------------------------------------------+
| Connection and execution layer                 |
| Pool, group commit, query, CDC, migrations     |
+-----------------------------------------------+
| Driver abstraction layer                       |
+-----------------------------------------------+
| Pluggable SQLite engine                        |
+-----------------------------------------------+
```

**Driver layer.** A pluggable interface wraps any SQLite engine (native binding,
WebAssembly, or runtime built-in) behind a uniform asynchronous API.

**Connection and execution layer.** A connection pool holds one writer and several
readers, group commit coalesces writes, trigger-based CDC records row changes, and
a migration runner versions the schema.

**Orchestration layer.** The registry manages many named databases with lifecycle
hooks, metrics, idle eviction, and auto-open.

**Replication and transport layer.** Changes distribute across server nodes using
Hybrid Logical Clocks for causal ordering, over a primary-replica topology where
one primary per group accepts writes. In coordinator mode, Sirannon runs automatic
primary failover through coordinator-backed leases, primary terms, in-sync
tracking, and fail-closed promotion. The production replication transport is gRPC.

**Application layer.** An HTTP and WebSocket server exposes databases over the
network, a client SDK provides a remote database proxy with subscriptions and
topology-aware read routing, and device sync keeps an end-user device's local
database in step with a server. The HTTP and WebSocket transports are not used for
node-to-node replication.

---

## Specification Documents

| Document | Contents |
|----------|----------|
| [01-driver.md](01-driver.md) | Driver contract, connection, statement, value mapping, capabilities |
| [02-core.md](02-core.md) | Registry, database, group commit, connection pool, writer worker, query execution, CDC, hooks, lifecycle, migrations, bulk load, backups, metrics |
| [03-replication.md](03-replication.md) | Replication engine, HLC, replication log, primary-replica topology, coordinator failover, conflict resolution, first sync, write forwarding |
| [04-transport.md](04-transport.md) | Transport interface, gRPC wire protocol, authentication |
| [05-server.md](05-server.md) | HTTP endpoints, value encoding, WebSocket protocol, health endpoints, CORS |
| [06-client.md](06-client.md) | Client SDK, remote database proxy, subscriptions, topology-aware routing |
| [07-errors.md](07-errors.md) | Error taxonomy and wire protocol error codes |
| [08-device-sync.md](08-device-sync.md) | Device identity, write stamping, change application, push, device cursors, live pull, snapshot download, migration handshake, capability negotiation, sync controller |

Test vector files are in the [test-vectors/](test-vectors/) directory.

---

## Requirement Tiers

- **Normative.** Every conforming implementation must follow the behaviour. A build
  that violates a normative requirement does not conform. Normative requirements
  use 'must'.
- **Implementation-defined.** The specification defines the contract (inputs,
  outputs, and invariants) and leaves the mechanism to the runtime.
- **Recommended.** Suggested defaults the reference uses, marked 'recommended' with
  a concrete value; other implementations may choose differently.

---

## Pseudocode Convention

Type definitions use a language-neutral notation; it is illustrative, not
prescriptive, and each implementation uses its own type system.

| Notation | Meaning |
|----------|---------|
| `Map<K, V>` | A mapping from keys of type K to values of type V |
| `List<T>` | An ordered collection of elements of type T |
| `any` | A value of any type |
| `T or null` | A value that is either T or absent (each language maps 'absent' to its own null, undefined, None, or Option) |
| `<T>` | A generic type parameter |
| `async -> T` | An asynchronous operation returning T |
| `number` | A numeric value; a 64-bit integer where the text says so |

---

## Glossary

| Term | Definition |
|------|------------|
| **Batch** | A group of changes sent as one unit with a checksum |
| **CDC** | Change Data Capture; trigger-based recording of row-level changes |
| **Change event** | A record of one row mutation (insert, update, or delete) |
| **Cluster coordinator** | A linearisable metadata service that stores cluster authority, leases, and watches |
| **Conflict resolver** | A function that selects or merges local and incoming row versions when an incoming change targets an existing row |
| **Controller** | A process role that makes failover decisions while it holds the controller lease |
| **Device** | An end-user process that syncs a whole local database with a server |
| **Device cursor** | The server's record of how far a device has acknowledged, used to bound retention |
| **Epoch** | An identifier for a database file's sequence space; a cursor from another epoch forces a resync |
| **Group commit** | Coalescing concurrent writes so one commit persists many |
| **HLC** | Hybrid Logical Clock; a timestamp combining wall-clock time with a logical counter for causal ordering |
| **In-sync set** | The replicas proven current enough for safe reads or promotion in a group |
| **Node** | A single server instance participating in replication |
| **Primary** | The node that accepts writes in a primary-replica topology |
| **Primary term** | A monotonically increasing fencing value for a group's current primary authority |
| **Read concern** | A read safety guarantee specifying which durability point a read may observe |
| **Registry** | The top-level object that manages many named databases |
| **Replica** | A read-only node that receives replicated changes from the primary |
| **Replication group** | The nodes that store copies of one database and share one primary authority |
| **Snapshot** | A full copy of a database served to a joining node or a resyncing device |
| **Subscription** | A callback registered to receive CDC events for a table |
| **Topology** | The pattern that determines which nodes can write and how changes flow |
| **Write concern** | A durability guarantee specifying how many nodes must acknowledge a write |
| **Write forwarding** | A replica forwarding a write to the primary for execution |
