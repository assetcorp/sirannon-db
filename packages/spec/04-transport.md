# Sirannon Transport Specification

This document defines the transport layer that carries replication
messages between nodes. It covers the transport interface, message
types, the WebSocket wire protocol, and the hello handshake. All
Sirannon implementations must support the transport interface. The
WebSocket wire protocol is the normative network format for
cross-implementation interoperability.

---

## Transport Interface

The transport interface abstracts the network layer for
replication. Implementations may provide multiple transports (in-
memory for testing, WebSocket for production), but all must
conform to this interface.

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
  endpoints?: Array<string>
  localRole?:  'primary' | 'replica'
  metadata?:   Record<string, unknown>
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
  metadata?:     Record<string, unknown>
}
```

### connect(localNodeId, config)

Starts the transport. For server-based transports (WebSocket),
this binds to a port and begins accepting connections. For client-
based transports, this connects to the configured endpoints.

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

## WebSocket Wire Protocol (Normative)

The WebSocket transport is the normative network protocol for
Sirannon replication. Two Sirannon nodes in different languages
must interoperate over this protocol.

### Endpoint

```text
WS /replication
```

### Message Envelope

All messages are JSON objects with a `type` field and a `payload`
field:

```text
ReplicationMessage {
  type:    string
  payload: object
}
```

### Message Types

| Type | Payload | Direction | Description |
|------|---------|-----------|-------------|
| `hello` | HelloPayload | Both | First message after connection |
| `batch` | ReplicationBatch | Primary -> Replica | Change batch |
| `ack` | ReplicationAck | Replica -> Primary | Batch acknowledgment |
| `forward_request` | ForwardedTransaction | Replica -> Primary | Write forwarding |
| `forward_response` | ForwardResponsePayload | Primary -> Replica | Forwarding result |
| `sync_request` | SyncRequest | Replica -> Primary | First sync request |
| `sync_batch` | SyncBatch | Primary -> Replica | Sync data batch |
| `sync_complete` | SyncComplete | Primary -> Replica | Sync finalization |
| `sync_ack` | SyncAck | Replica -> Primary | Sync batch acknowledgment |

### Hello Handshake

The first message sent after a WebSocket connection is established
must be a `hello` message. Both sides send their identity.

```text
HelloPayload {
  nodeId:     string
  role:       'primary' | 'replica'
  authToken?: string
}
```

A node must not send or accept any other message type before the
hello handshake completes. If the hello is missing or malformed,
close the connection.

### Recommended Handshake Extension

Implementations should extend the hello payload to include
conflict resolver configuration and topology type. This allows
nodes to detect configuration mismatches at connection time rather
than discovering them through silent data divergence.

```text
HelloPayload (extended, recommended) {
  nodeId:       string
  role:         'primary' | 'replica'
  authToken?:   string
  resolverType?: string
  topologyType?: string
}
```

### Authentication

When `authToken` is configured, the receiving node must validate
the token using a timing-safe comparison. If the token does not
match, close the connection with WebSocket close code `4001`.

### Forward Response

```text
ForwardResponsePayload {
  requestId: string
  result?:   ForwardedTransactionResult
  error?:    string
}
```

If `error` is present, the forwarded write failed on the primary.

### Bigint Encoding

All messages use the bigint serialisation convention defined in
[03-replication.md](03-replication.md#bigint-serialisation-normative).
JSON values prefixed with `\x00sirannon:bigint:` are decoded as
bigint on the receiving side.

### Transport Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `port` | 0 (random) | Listening port for the WebSocket server |
| `host` | `127.0.0.1` | Listening address |
| `maxPayloadLength` | 16,777,216 (16 MB) | Maximum message size in bytes |
| `reconnectInitialDelay` | 100 ms (recommended) | First reconnect delay |
| `reconnectMaxDelay` | 30,000 ms (recommended) | Maximum reconnect delay |
| `reconnectMultiplier` | 2 (recommended) | Exponential backoff multiplier |
| `forwardTimeout` | 30,000 ms (recommended) | Write forwarding request timeout |

### Reconnection

Client-initiated connections should support automatic reconnection
with exponential backoff:

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
