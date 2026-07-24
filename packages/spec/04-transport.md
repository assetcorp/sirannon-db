# Sirannon Replication Transport Specification

The transport layer carries replication messages between Sirannon nodes. Every
implementation must support the transport interface; the gRPC wire protocol is
the normative network format for cross-implementation interoperability. This
document covers node-to-node replication transport only. The HTTP and WebSocket
transports in the client SDK connect applications to a server and are separate
(see [06-client.md](06-client.md)); WebSocket is not a replication transport.

---

## Replication Transport Interface

```text
ReplicationTransport {
  connect(localNodeId: string, config: TransportConfig): async -> void
  disconnect(): async -> void

  send(peerId, batch): async -> void
  broadcast(batch): async -> void
  sendAck(peerId, ack): async -> void
  forward(peerId, request): async -> ForwardedTransactionResult

  requestSync(peerId, request): async -> void
  sendSyncBatch(peerId, batch): async -> void
  sendSyncComplete(peerId, complete): async -> void
  sendSyncAck(peerId, ack): async -> void

  onBatchReceived(handler): void
  onAckReceived(handler): void
  onForwardReceived(handler): void
  onSyncRequested(handler): void
  onSyncBatchReceived(handler): void
  onSyncCompleteReceived(handler): void
  onSyncAckReceived(handler): void
  onPeerConnected(handler): void
  onPeerDisconnected(handler): void

  peers(): ReadonlyMap<string, NodeInfo>
}

TransportConfig { endpoints?: List<string>, localRole?: 'primary' | 'replica',
                  groupId?, primaryTerm?: number, protocolVersion?, metadata? }

NodeInfo { id, groupId?, role: 'primary' | 'replica', primaryTerm?: number,
           protocolVersion?, joinedAt: number, lastSeenAt: number,
           lastAckedSeq: number, metadata? }
```

The engine supplies `localRole` from the configured topology, and in coordinator
mode also `groupId`, `primaryTerm`, and `protocolVersion`. Peers are addressed by
node ID. After `disconnect`, every send method fails with `TRANSPORT_ERROR`, as
does a send to a peer that is not connected. `forward` is a request-response call
bounded by a deadline (default 30,000 ms).

---

## gRPC Wire Protocol (Normative)

Two nodes in different languages must interoperate over this protocol.

```protobuf
syntax = "proto3";
package sirannon.replication.v1;

service Replication {
  rpc Replicate(stream ReplicationMessage) returns (stream ReplicationMessage);
  rpc Sync(stream SyncMessage) returns (stream SyncMessage);
  rpc Forward(ForwardRequest) returns (ForwardResponse);
}

message ColumnValue {
  oneof kind {
    bool   null_value   = 1;
    string string_value = 2;
    int64  int_value    = 3;
    double float_value  = 4;
    bytes  blob_value   = 5;
    bool   bool_value   = 6;
  }
}

message RowData { map<string, ColumnValue> fields = 1; }
message HlcRange { string min = 1; string max = 2; }

message ReplicationChange {
  string  table         = 1;
  string  operation     = 2;
  string  row_id        = 3;
  RowData primary_key   = 4;
  string  hlc           = 5;
  string  tx_id         = 6;
  string  node_id       = 7;
  RowData new_data      = 8;
  RowData old_data      = 9;
  string  ddl_statement = 10;
}

message BatchPayload {
  string source_node_id = 1;
  string batch_id       = 2;
  int64  from_seq       = 3;
  int64  to_seq         = 4;
  HlcRange hlc_range    = 5;
  repeated ReplicationChange changes = 6;
  string checksum       = 7;
  string group_id       = 8;
  int64  primary_term   = 9;
}

message AckPayload {
  string batch_id     = 1;
  int64  acked_seq    = 2;
  string node_id      = 3;
  string group_id     = 4;
  int64  primary_term = 5;
}

message Hello {
  string node_id          = 1;
  string role             = 2;
  string group_id         = 3;
  int64  primary_term     = 4;
  string protocol_version = 5;
}

message ReplicationMessage {
  oneof payload { Hello hello = 1; BatchPayload batch = 2; AckPayload ack = 3; }
}

message SyncRequestPayload {
  string request_id                 = 1;
  string joiner_node_id             = 2;
  repeated string completed_tables  = 3;
  string group_id                   = 4;
  int64  primary_term               = 5;
  bool   supports_stream_verification = 6;
}

message SyncBatchPayload {
  string request_id             = 1;
  string table                  = 2;
  int32  batch_index            = 3;
  repeated RowData rows         = 4;
  repeated string schema        = 5;
  string checksum               = 6;
  bool   is_last_batch_for_table = 7;
  string group_id               = 8;
  int64  primary_term           = 9;
  int32  total_tables           = 10;
}

message SyncCompletePayload {
  string request_id  = 1;
  int64  snapshot_seq = 2;
  repeated SyncTableManifest manifests = 3;
  string group_id    = 4;
  int64  primary_term = 5;
}

message SyncTableManifest {
  string table       = 1;
  int32  row_count   = 2;
  string pk_hash     = 3;
  string batch_digest = 4;
}

message SyncAckPayload {
  string request_id    = 1;
  string joiner_node_id = 2;
  string table         = 3;
  int32  batch_index   = 4;
  bool   success       = 5;
  string error         = 6;
  string group_id      = 7;
  int64  primary_term  = 8;
}

message SyncMessage {
  oneof payload {
    Hello hello = 1;
    SyncRequestPayload  sync_request  = 2;
    SyncBatchPayload    sync_batch    = 3;
    SyncCompletePayload sync_complete = 4;
    SyncAckPayload      sync_ack      = 5;
  }
}

message Statement {
  string sql = 1;
  map<string, ColumnValue> named_params = 2;
  repeated ColumnValue positional_params = 3;
}

message ForwardRequest {
  string request_id  = 1;
  repeated Statement statements = 2;
  string group_id    = 3;
  int64  primary_term = 4;
}

message StatementResult { int32 changes = 1; int64 last_insert_row_id = 2; }

message ForwardResponse {
  string request_id  = 1;
  repeated StatementResult results = 2;
  string error       = 3;
  string group_id    = 4;
  int64  primary_term = 5;
}
```

### Value Encoding on the Wire

A `ColumnValue` carries the native SQLite value with no tagged envelope: null,
string, 64-bit integer, double, byte array, or boolean. Any integer decodes back
as a 64-bit integer. An absent `group_id` or `primary_term` encodes as the empty
string or 0 and decodes back to absent. A `Statement` carries either
`named_params` or `positional_params`; when both are populated, the receiver uses
`named_params`.

### Term Fields

`group_id` and `primary_term` are required when coordinator mode is enabled and
may be empty in static mode. The transport carries these fields; the replication
engine enforces term fencing and rejects a stale term with `STALE_PRIMARY` (see
[03-replication.md](03-replication.md#term-fencing)).

---

## Connection Model

In static mode the primary runs the gRPC server and replicas connect as clients:
a replica opens the `Replicate` bidirectional stream (the primary writes batches,
the replica writes acks), calls `Forward` as a unary RPC, and opens the `Sync`
bidirectional stream for first sync. A node starts its gRPC server when its local
role is `primary` or a `groupId` is configured. In coordinator mode every
data-bearing node may expose the server, and replicas replicate from the current
recorded primary; when the primary changes, replicas close stale streams and
connect to the new primary for the new term. The transport itself does not
reconnect on stream loss.

### Peer Identity

The first message on every `Replicate` or `Sync` stream must be a `Hello` carrying
the sender's `node_id` and `role`, and in coordinator mode also `group_id`,
`primary_term`, and `protocol_version`. A node must not send or accept any other
message before the `Hello`. The server replies with its own `Hello` on the first
client `Hello`. If the first message is not a `Hello`, the receiver terminates the
stream. When mutual TLS is enabled, the authenticated peer identity must match the
node identity through the certificate common name.

### Stream Invariant

At most one active `Replicate` stream per peer. When a new stream opens for a peer
that already has one, the server ends the old stream before accepting messages on
the new one.

---

## Authentication

Implementations must support mutual TLS. The gRPC server presents a certificate
and, when a CA certificate is configured, verifies client certificates; a client
presents its own certificate. An insecure mode with no TLS may be provided for
local development; it is non-normative and requires explicit opt-in. TLS is on by
default.

Production coordinator access must use TLS, authenticated Sirannon identities, a
dedicated coordinator key prefix per cluster, and credentials limited to that
prefix. Insecure coordinator access is allowed only for development and test.

---

## Health, Shutdown, and Local Transports

The gRPC server must serve the `grpc.health.v1.Health` service, reporting the
`sirannon.replication.v1.Replication` service, and set it to not-serving on
disconnect. On shutdown the server stops accepting new streams and lets in-flight
`Forward` RPCs finish before closing; `Replicate` and `Sync` streams may be
terminated immediately.

For tests and single-process multi-node runs, an implementation may provide an
in-memory transport (direct calls, asynchronous delivery) and a simulated
transport (configurable delay, reordering, dropping, and seeded randomness for
reproducibility). Both are non-normative and their behaviour is
implementation-defined.

---

## Transport Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `0.0.0.0` | Listening address for the gRPC server |
| `port` | 0 (OS-assigned) | Listening port |
| `tlsCert` | required unless insecure | TLS certificate file |
| `tlsKey` | required unless insecure | TLS private key file |
| `tlsCaCert` | optional | CA certificate; when set, client certificates are verified |
| `insecure` | `false` | Disable TLS and authentication (development only) |
| `forwardDeadlineMs` | 30,000 | Deadline for `Forward` RPCs |
