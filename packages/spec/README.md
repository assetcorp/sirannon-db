# Sirannon Specification

This document defines the language-agnostic specification for
Sirannon. Sirannon turns SQLite databases into a networked,
distributed data layer with real-time subscriptions, multi-node
replication, and schema management. All implementations
(TypeScript, Go, Rust, and Python) must follow the contracts defined
here. Where a behaviour is left to the runtime, this specification
says so.

---

## Architecture

Sirannon is organised into six layers. Each layer depends only on
the layers below it.

```text
+-----------------------------------------+
| Application layer (client and server)   |
+-----------------------------------------+
| Replication and transport layer         |
| Engine, HLC, topology, and failover     |
+-----------------------------------------+
| Database orchestration layer            |
| Registry, database, hooks, and metrics  |
+-----------------------------------------+
| Connection and execution layer          |
| Pool, query executor, CDC, and backup   |
+-----------------------------------------+
| Driver abstraction layer                |
| SQLiteDriver, connection, and statement |
+-----------------------------------------+
| Pluggable SQLite engine                 |
+-----------------------------------------+
```

**Driver Abstraction Layer.** This layer provides a pluggable
interface that wraps any SQLite engine (native bindings, WebAssembly,
and built-in runtime modules) behind a uniform async API.

**Connection & Execution Layer.** This layer manages a connection
pool (one dedicated writer and N read connections), caches prepared
statements, tracks changes through trigger-based CDC, and creates
backups.

**Database Orchestration Layer.** The Sirannon registry manages
multiple named databases with lifecycle hooks, metrics collection,
idle eviction, and auto-open resolution.

**Replication and transport layer.** This layer distributes changes
across nodes using Hybrid Logical Clocks for causal ordering. It uses
a primary-replica topology where a single primary per replication
group accepts writes and replicates to read replicas. In coordinator
mode, Sirannon runs automatic primary failover through
coordinator-backed leases, primary terms, in-sync replica tracking,
and fail-closed promotion rules. The production network transport
for node-to-node replication is gRPC.

**Application layer.** An HTTP + WebSocket server exposes databases
over the network. A client SDK provides a remote database proxy
with subscriptions and topology-aware read routing. These HTTP and
WebSocket client transports are not used for node-to-node replication.

---

## Specification Documents

| Document | Contents |
|----------|----------|
| [01-driver.md](01-driver.md) | Driver contract, connection, statement, capabilities |
| [02-core.md](02-core.md) | Registry, database, connection pool, query execution, CDC, hooks, lifecycle, migrations, backups, metrics |
| [03-replication.md](03-replication.md) | Replication engine, HLC, replication log, primary-replica topology, coordinator-backed failover, conflict resolution, peer tracking, first sync, write forwarding |
| [04-transport.md](04-transport.md) | Transport interface, message types, gRPC wire protocol, term fencing, authentication |
| [05-server.md](05-server.md) | HTTP endpoints, WebSocket protocol, routing metadata, health endpoints |
| [06-client.md](06-client.md) | Client SDK, remote database proxy, subscriptions, topology-aware routing, coordinator-mode primary discovery |
| [07-errors.md](07-errors.md) | Error taxonomy, wire protocol error codes |

Test vector files are stored in the [test-vectors/](test-vectors/) directory.

---

## Requirement Tiers

This specification uses three tiers of requirements.

**Normative.** All conforming implementations must follow this
behaviour. Normative requirements use the word 'must' in lowercase.
A build that violates a normative requirement does not conform to
this specification.

**Implementation-defined.** This tier covers behaviour where the
specification defines the contract (inputs, outputs, and invariants)
but leaves the mechanism to the runtime. For example, the
specification requires that statement caching produces correct
results for repeated queries, but does not prescribe the eviction
strategy.

**Recommended.** This tier covers suggested defaults and approaches
that the TypeScript reference implementation uses. Other
implementations may choose different values. Recommended items are
marked with the word 'recommended' and include a concrete default
value.

---

## Pseudocode Convention

Type definitions in this specification use a language-neutral
pseudocode. The notation is illustrative, not prescriptive;
implementations use their language's native type system.

| Notation | Meaning |
|----------|---------|
| `Map<K, V>` | A mapping from keys of type K to values of type V |
| `List<T>` | An ordered collection of elements of type T |
| `any` | A value of any type |
| `T or null` | A value that is either T or absent |
| `<T>` | A generic type parameter |
| `async -> T` | An asynchronous operation returning T |
| `readonly` | A value that must not be mutated after construction |

---

## Glossary

| Term | Definition |
|------|------------|
| **Batch** | A group of replication changes sent as a single unit with a checksum |
| **CDC** | Change Data Capture; trigger-based mechanism that records row-level changes |
| **Change event** | A record of a single row mutation (insert, update, or delete) |
| **Cluster coordinator** | A linearisable metadata service used to store Sirannon cluster authority, leases, and watches |
| **Conflict resolver** | A function that selects or merges local and incoming row versions when batch application finds the target row already present |
| **Controller** | A Sirannon process role that makes failover decisions while it has the coordinator controller lease |
| **Driver** | A pluggable adapter that wraps a specific SQLite engine |
| **HLC** | Hybrid Logical Clock; a timestamp combining wall-clock time with a logical counter for causal ordering |
| **In-sync set** | The replicas proven current enough to receive safe reads or promotion for a replication group |
| **Node** | A single Sirannon instance participating in replication |
| **Primary** | The node that accepts write operations in a primary-replica topology |
| **Primary term** | A monotonically increasing fencing value for a replication group's current primary authority |
| **Read concern** | A read safety guarantee specifying which durability point a read may observe |
| **Registry** | The top-level Sirannon object that manages multiple named databases |
| **Replica** | A read-only node that receives replicated changes from the primary |
| **Replication group** | The Sirannon nodes that store copies of the same database and share one primary authority |
| **Subscription** | A callback registered to receive CDC events for a specific table |
| **Topology** | The replication pattern that determines which nodes can write and how changes flow |
| **Client transport** | HTTP or WebSocket communication between an application client and a Sirannon server |
| **Replication transport** | The channel that moves replication messages between Sirannon nodes; the normative network protocol is gRPC |
| **Write concern** | A durability guarantee specifying how many nodes must acknowledge a write |
| **Write forwarding** | A mechanism where replicas forward write operations to the primary for execution |
