# Sirannon Specification

This document defines the language-agnostic specification for
Sirannon. Sirannon turns SQLite databases into a networked,
distributed data layer with real-time subscriptions, multi-node
replication, and schema management. All implementations
(TypeScript, Go, Rust, Python) must follow the contracts defined
here. Where a behaviour is left to the runtime, this specification
says so.

---

## Architecture

Sirannon is organised into six layers. Each layer depends only on
the layers below it.

```text
┌───────────────────���──────────────────────┐
│   Application Layer (Client / Server)    │
├──────────────────────────────────────────┤
│   Replication & Transport Layer          │
│   (Engine, HLC, Conflict, Topology)      │
├──────────────────────────────────────────┤
│   Database Orchestration Layer           │
│   (Registry, Database, Hooks, Metrics)   │
├──────────────────────────────────────────┤
│   Connection & Execution Layer           │
│   (Pool, Query Executor, CDC, Backup)    │
├──────────────────────────────────────────┤
│   Driver Abstraction Layer               │
│   (SQLiteDriver, Connection, Statement)  │
├──────────────────────────────────────────┤
│   SQLite Engine (pluggable)              │
└──────────────────────────────────────────┘
```

**Driver Abstraction Layer.** Pluggable interface that wraps any
SQLite engine (native bindings, WebAssembly, built-in runtime
modules) behind a uniform async API.

**Connection & Execution Layer.** Manages a connection pool (one
dedicated writer, N read connections), caches prepared statements,
tracks changes via trigger-based CDC, and creates backups.

**Database Orchestration Layer.** The Sirannon registry manages
multiple named databases with lifecycle hooks, metrics collection,
idle eviction, and auto-open resolution.

**Replication & Transport Layer.** Distributes changes across nodes
using Hybrid Logical Clocks for causal ordering. Uses a
primary-replica topology where a single primary accepts writes and
replicates to read replicas. Conflict resolvers handle split-brain
recovery during failover.

**Application Layer.** An HTTP + WebSocket server exposes databases
over the network. A client SDK provides a remote database proxy
with subscriptions and topology-aware read routing.

---

## Specification Documents

| Document | Contents |
|----------|----------|
| [01-driver.md](01-driver.md) | Driver contract, connection, statement, capabilities |
| [02-core.md](02-core.md) | Registry, database, connection pool, query execution, CDC, hooks, lifecycle, migrations, backups, metrics |
| [03-replication.md](03-replication.md) | Replication engine, HLC, replication log, primary-replica topology, conflict resolution, peer tracking, first sync, write forwarding |
| [04-transport.md](04-transport.md) | Transport interface, message types, gRPC wire protocol, authentication |
| [05-server.md](05-server.md) | HTTP endpoints, WebSocket protocol, health endpoints |
| [06-client.md](06-client.md) | Client SDK, remote database proxy, subscriptions, topology-aware routing |
| [07-errors.md](07-errors.md) | Error taxonomy, wire protocol error codes |

Test vector files live in the [test-vectors/](test-vectors/) directory.

---

## Requirement Tiers

This specification uses three tiers of requirements.

**Normative.** Behaviour that all conforming implementations must
follow. Normative requirements use the word "must" in lowercase.
Violating a normative requirement means the build does
not conform to this specification.

**Implementation-defined.** Behaviour where the specification defines
the contract (inputs, outputs, invariants) but leaves the
mechanism to the runtime. For example, the specification requires
that statement caching produces correct results for repeated
queries, but does not prescribe the eviction strategy.

**Recommended.** Suggested defaults and approaches that the
TypeScript reference implementation uses. Other implementations
may choose different values. Recommended items are marked with
the word "recommended" and include a concrete default value.

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
| **Conflict resolver** | A function that determines the outcome when two nodes write to the same row |
| **Driver** | A pluggable adapter that wraps a specific SQLite engine |
| **HLC** | Hybrid Logical Clock; a timestamp combining wall-clock time with a logical counter for causal ordering |
| **Node** | A single Sirannon instance participating in replication |
| **Primary** | The node that accepts write operations in a primary-replica topology |
| **Registry** | The top-level Sirannon object that manages multiple named databases |
| **Replica** | A read-only node that receives replicated changes from the primary |
| **Subscription** | A callback registered to receive CDC events for a specific table |
| **Topology** | The replication pattern that determines which nodes can write and how changes flow |
| **Transport** | The network layer that carries replication messages between nodes |
| **Write concern** | A durability guarantee specifying how many nodes must acknowledge a write |
| **Write forwarding** | A mechanism where replicas forward write operations to the primary for execution |
