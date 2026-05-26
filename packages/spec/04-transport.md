# Sirannon Transport Specification

This document defines the transport layer that carries replication
messages between nodes. It covers the transport interface, message
types, the gRPC wire protocol, and authentication. All Sirannon
implementations must support the transport interface. The gRPC wire
protocol is the normative network format for cross-implementation
interoperability.

---

## Transport Interface

The transport interface abstracts the network layer for
replication. Implementations may provide multiple transports (in-
memory for testing, gRPC for production), but all must conform to
this interface.

```text
ReplicationTransport {
  connect(localNodeId: string, config: TransportConfig): async -> void
  disconnect(): async -> void

  send(peerId: string, batch: ReplicationBatch): async -> void
  broadcast(batch: ReplicationBatch): async -> void
  sendAck(peerId: string, ack: ReplicationAck): async -> void

  forward(peerId: string, request: ForwardedTransaction):
    async -> ForwardedTransactionResult

  requestSync(peerId: string, request: SyncRequest): async -> void
  sendSyncBatch(peerId: string, batch: SyncBatch): async -> void
  sendSyncComplete(peerId: string, complete: SyncComplete): async -> void
  sendSyncAck(peerId: string, ack: SyncAck): async -> void

  onBatchReceived(handler: (batch, fromPeerId) -> async void): void
  onAckReceived(handler: (ack, fromPeerId) -> void): void
  onForwardReceived(
    handler: (request, fromPeerId) -> async ForwardedTransactionResult
  ): void

  onSyncRequested(handler: (request, fromPeerId) -> async void): void
  onSyncBatchReceived(handler: (batch, fromPeerId) -> async void): void
  onSyncCompleteReceived(handler: (complete, fromPeerId) -> async void): void
  onSyncAckReceived(handler: (ack, fromPeerId) -> void): void

  onPeerConnected(handler: (peer: NodeInfo) -> void): void
  onPeerDisconnected(handler: (peerId: string) -> void): void

  peers(): ReadonlyMap<string, NodeInfo>
}
```

### TransportConfig

```text
TransportConfig {
  endpoints?: List<string>
  localRole?:  'primary' | 'replica'
  metadata?:   Map<string, any>
}
```

### NodeInfo

```text
NodeInfo {
  id:            string
  role:          'primary' | 'replica'
  joinedAt:      number
  lastSeenAt:    number
  lastAckedSeq:  bigint
  metadata?:     Map<string, any>
}
```

### connect(localNodeId, config)

Starts the transport. For server-based transports (gRPC), this
binds to a port and begins accepting connections. For client-based
transports, this connects to the configured endpoints.

### disconnect()

Stops the transport. Closes all connections and stops accepting
new ones. After disconnect, all send methods must throw with error
code `TRANSPORT_ERROR`.

### send(peerId, batch)

Sends a replication batch to a specific peer. Throws with error
code `TRANSPORT_ERROR` if the peer is not connected.

### broadcast(batch)

Sends a replication batch to all connected peers. The engine uses
this when the topology directs replication to all peers.

### forward(peerId, request)

Sends a write-forwarding request to the primary and waits for the
response. This is a request-response operation with a timeout.
The recommended timeout is 30,000 milliseconds.

### peers()

Returns a read-only map of currently connected peers, keyed by
node ID. The engine uses this to determine replication targets
and to find the primary for write forwarding.

---

## gRPC Wire Protocol (Normative)

The gRPC transport is the normative network protocol for Sirannon
replication. Two Sirannon nodes in different languages must
interoperate over this protocol.

### Service Definition

```protobuf
syntax = "proto3";
package sirannon.replication.v1;

service Replication {
  rpc Replicate(stream ReplicationMessage) returns (stream ReplicationMessage);
  rpc Sync(stream SyncMessage) returns (stream SyncMessage);
  rpc Forward(ForwardRequest) returns (ForwardResponse);
}
```

### Message Types

```protobuf
message ReplicationMessage {
  oneof payload {
    Hello hello = 1;
    BatchPayload batch = 2;
    AckPayload ack = 3;
  }
}

message SyncMessage {
  oneof payload {
    Hello hello = 1;
    SyncRequestPayload sync_request = 2;
    SyncBatchPayload sync_batch = 3;
    SyncCompletePayload sync_complete = 4;
    SyncAckPayload sync_ack = 5;
  }
}

message Hello {
  string node_id = 1;
  string role = 2;
}

message ColumnValue {
  oneof kind {
    bool null_value = 1;
    string string_value = 2;
    int64 int_value = 3;
    double float_value = 4;
    bytes blob_value = 5;
    bool bool_value = 6;
  }
}

message RowData {
  map<string, ColumnValue> fields = 1;
}

message HlcRange {
  string min = 1;
  string max = 2;
}

message ReplicationChange {
  string table = 1;
  string operation = 2;
  string row_id = 3;
  RowData primary_key = 4;
  string hlc = 5;
  string tx_id = 6;
  string node_id = 7;
  RowData new_data = 8;
  RowData old_data = 9;
  string ddl_statement = 10;
}

message BatchPayload {
  string source_node_id = 1;
  string batch_id = 2;
  int64 from_seq = 3;
  int64 to_seq = 4;
  HlcRange hlc_range = 5;
  repeated ReplicationChange changes = 6;
  string checksum = 7;
}

message AckPayload {
  string batch_id = 1;
  int64 acked_seq = 2;
  string node_id = 3;
}

message Statement {
  string sql = 1;
  map<string, ColumnValue> named_params = 2;
  repeated ColumnValue positional_params = 3;
}

message ForwardRequest {
  repeated Statement statements = 1;
  string request_id = 2;
}

message ForwardResult {
  int64 changes = 1;
  int64 last_insert_row_id = 2;
}

message ForwardResponse {
  string request_id = 1;
  repeated ForwardResult results = 2;
  string error = 3;
}

message SyncRequestPayload {
  string request_id = 1;
  string joiner_node_id = 2;
  repeated string completed_tables = 3;
}

message SyncBatchPayload {
  string request_id = 1;
  string table = 2;
  int32 batch_index = 3;
  repeated RowData rows = 4;
  repeated string schema = 5;
  string checksum = 6;
  bool is_last_batch_for_table = 7;
}

message SyncTableManifest {
  string table = 1;
  int64 row_count = 2;
  string pk_hash = 3;
}

message SyncCompletePayload {
  string request_id = 1;
  int64 snapshot_seq = 2;
  repeated SyncTableManifest manifests = 3;
}

message SyncAckPayload {
  string request_id = 1;
  string joiner_node_id = 2;
  string table = 3;
  int32 batch_index = 4;
  bool success = 5;
  string error = 6;
}
```

### Statement Parameters

Each `Statement` carries either named or positional parameters, not
both. If both `named_params` and `positional_params` are populated,
the receiver must reject the statement with error code
`BATCH_VALIDATION_ERROR`.

---

## Connection Model

The primary runs the gRPC server. Replicas connect as gRPC clients.

- Replicas initiate the `Replicate` bidirectional stream. The
  primary writes batches; the replica writes acks.
- Replicas call `Forward` as a unary RPC for write forwarding.
- Replicas initiate the `Sync` bidirectional stream for first
  snapshot transfer.

---

## Peer Identity

The first message on every `Replicate` or `Sync` stream must be a
`Hello` message carrying the sender's `node_id` and `role`. A node
must not send or accept any other message type before the `Hello`
exchange completes. If the first message is not a `Hello`, the
receiving side must terminate the stream.

---

## Stream Invariants

At most one active `Replicate` stream per peer at any time. If a
new stream opens for a peer that already has an active stream, the
server must terminate the old stream before accepting messages on
the new one. This is normative.

---

## Authentication

Implementations must support mutual TLS (mTLS) for replication
channel authentication. Both the server and connecting clients
present certificates signed by a trusted certificate authority.

An insecure mode with no authentication may be provided for local
development. Insecure mode is non-normative and must require
explicit opt-in (for example, a configuration flag).

TLS is enabled by default. Disabling TLS requires an explicit
insecure flag.

---

## Health Checking

The gRPC server must serve the `grpc.health.v1.Health` service.

---

## Forward Deadline

The recommended default deadline for `Forward` RPCs is 30,000
milliseconds. Implementations may allow configuration.

---

## Graceful Shutdown

On shutdown, the server must stop accepting new streams and allow
in-flight `Forward` RPCs to complete, up to a bounded timeout.
`Replicate` and `Sync` streams may be terminated immediately. This
is normative.

---

## Reconnection

Implementations should support automatic reconnection with
exponential backoff. Recommended defaults: initial delay 100 ms,
maximum delay 30,000 ms, multiplier 2.

```text
delay = min(initialDelay * multiplier ^ attempts, maxDelay)
```

On successful reconnection, reset the attempt counter to zero.

---

## In-Memory Transport

For testing and single-process multi-node scenarios,
implementations should provide an in-memory transport. The in-
memory transport uses direct function calls instead of network I/O
and delivers messages asynchronously (via microtask scheduling or
equivalent) to simulate realistic delivery semantics.

The in-memory transport is not part of the normative wire protocol.
Its behaviour is implementation-defined.

---

## Simulated Transport

For fault injection testing, implementations may provide a
simulated transport that supports:

- Configurable message delay.
- Message reordering.
- Message dropping.
- Seeded random number generation for reproducible tests.

The simulated transport is not part of the normative wire protocol.
Its behaviour is implementation-defined.

---

## Transport Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `127.0.0.1` | Listening address for the gRPC server |
| `port` | 0 (OS-assigned) | Listening port for the gRPC server |
| `tlsCert` | required | Path to the TLS certificate file |
| `tlsKey` | required | Path to the TLS private key file |
| `tlsCaCert` | required | Path to the CA certificate for verifying peer certificates |
| `insecure` | `false` | Disable TLS and authentication (non-normative, development only) |
| `forwardDeadlineMs` | 30,000 (recommended) | Deadline for Forward RPCs |
| `reconnectInitialDelay` | 100 ms (recommended) | First reconnect delay |
| `reconnectMaxDelay` | 30,000 ms (recommended) | Maximum reconnect delay |
| `reconnectMultiplier` | 2 (recommended) | Exponential backoff multiplier |
