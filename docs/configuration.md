# Configuration reference

Every option accepted by the registry, the databases it opens, the server, the client, the device sync controller, and the replication engine.

## `SirannonOptions`

| Option | Type | Required | Description |
| --- | --- | --- | --- |
| `driver` | `SQLiteDriver` | Yes | The SQLite driver adapter to use |
| `hooks` | `HookConfig` | No | Before/after hooks for queries, connections, subscriptions |
| `metrics` | `MetricsConfig` | No | Callbacks for query timing, connection events, CDC activity |
| `lifecycle` | `LifecycleConfig` | No | Auto-open resolver, idle timeout, max open databases |
| `migrations` | `MigrationSource` | No | Migration set, or a function returning it, applied to every writable database before it registers |
| `writerWorker` | `boolean \| WriterWorkerOptions` | No | Default writer-worker setting for every database this registry opens |

## `LifecycleConfig`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `autoOpen.resolver` | `(id: string) => { path, options? } \| undefined` | - | Resolves an unknown database ID to a file path, which is how a tenant opens on first access |
| `idleTimeout` | `number` | `0` | Milliseconds before an idle database closes; `0` disables the timer |
| `maxOpen` | `number` | `0` | Maximum databases open at once, evicting least-recently-used; `0` means unlimited |

`createTenantResolver` builds a resolver from a `basePath`, an optional file `extension`, and `defaultOptions` applied to every tenant it opens.

## `DatabaseOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `readOnly` | `boolean` | `false` | Open in read-only mode |
| `readPoolSize` | `number` | `4` | Number of read connections |
| `walMode` | `boolean` | `true` | Enable WAL mode |
| `synchronous` | `'off' \| 'normal' \| 'full' \| 'extra'` | `'normal'` | Writer durability (`PRAGMA synchronous`); a bulk load restores this level when it finishes |
| `cdcPollInterval` | `number` | `50` | CDC polling interval in ms |
| `cdcRetention` | `number` | `3_600_000` | CDC retention period in ms |
| `writerWorker` | `boolean \| WriterWorkerOptions` | `false` | Run writes on a dedicated worker thread so disk flushes never block the serving thread |

`WriterWorkerOptions` accepts `maxPendingWrites` (in-flight writes before the server sheds load), `writeTimeoutMs` (per-operation deadline), and `maxRestarts` (respawns allowed after the worker crashes).

## `QueryOptions`

Passed per call to `query`, `execute`, `executeBatch`, and a registered operation.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `readConcern` | `{ level: 'local' \| 'majority' \| 'linearizable' }` | - | How current the read must be; coordinator mode enforces it and static mode ignores it |
| `writeConcern` | `{ level, timeoutMs? }` | local commit in static mode, `'majority'` in coordinator mode | How many nodes must acknowledge the write; `timeoutMs` defaults to `5_000` |

## `BulkLoadOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `durability` | `'off' \| 'normal'` | `'off'` | Durability in force while the load runs; `'off'` suits a load starting from nothing, `'normal'` keeps WAL corruption safety |
| `checkpoint` | `boolean` | `true` | Whether the load ends with a WAL checkpoint; set it false on every batch but the last of a multi-batch import |

## `LiveQueryOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `rereadJitterMs` | `number` | `25` | Upper bound on the random delay before a second read starts |
| `maxTransactionChanges` | `number` | `10_000` | Buffered changes in one transaction before the query reads a second time instead of applying them |

Both options reach a local `db.live` only. A remote subscription carries no options, so the server opens the query with these defaults.

`UseLiveQueryOptions` in the React entry adds `enabled`, which holds a query closed while it is `false`.

## `ServerOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `string` | `'127.0.0.1'` | Bind address |
| `port` | `number` | `9876` | Listen port |
| `cors` | `boolean \| CorsOptions` | `false` | CORS configuration |
| `maxBodyBytes` | `number` | `1_048_576` | Maximum HTTP body and WebSocket message size; a positive integer no larger than `4_294_967_295` |
| `maxWebSocketBackpressureBytes` | `number` | larger of `16_777_216` and `maxBodyBytes` | Bytes buffered per connection before the server closes it so the client reconnects instead of losing a frame |
| `cdcRetentionMs` | `number` | `3_600_000` | How long change events are retained, bounding change-log growth and how far back `sinceSeq` can resume |
| `deviceCursorRetentionMs` | `number` | `2_592_000_000` | How long a device cursor is retained before eviction, 30 days by default; an evicted device resyncs from a snapshot |
| `maxUnacknowledgedChanges` | `number` | `1_000` | How far a device may run past its acknowledged sequence before delivery pauses; a larger transaction still arrives whole |
| `authenticate` | `AuthenticateHook<Identity>` | - | Runs before every database route and WebSocket upgrade; returns the caller identity, throws `RequestDeniedError` to refuse |
| `operations` | `OperationRegistry<Identity>` | - | Reads and writes this server serves by name, keyed by database ID |
| `acceptSql` | `boolean` | `false` | Whether the server accepts SQL statements over the network |
| `resolveExecutionTarget` | `ServerExecutionTargetResolver` | - | Resolves the target each database runs against, which is how replication enforces authority |
| `getReplicationStatus` | `() => ReplicationStatusInfo \| null` | - | Feeds `GET /health/ready` with replication state |
| `getClusterStatus` | `(databaseId: string) => ClusterStatusInfo \| null` | - | Feeds `GET /db/{id}/cluster` with routing metadata |
| `authorizeClusterStatus` | `ClusterStatusAuthorizer` | - | Your check for whether a request may read cluster status, which names every node address |

## `ClientOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `transport` | `'websocket' \| 'http'` | `'websocket'` | Transport protocol |
| `headers` | `Record<string, string>` | - | Custom headers for HTTP requests, and for the WebSocket upgrade under Node and Bun; a browser client that sets it with the WebSocket transport fails with `INVALID_ARGUMENT` |
| `webSocketProtocols` | `string \| string[]` | - | Subprotocols offered during the upgrade, which is how a browser carries a credential; the client offers `sirannon.v1` ahead of them and the server selects that identifier |
| `autoReconnect` | `boolean` | `true` | Reconnect on WebSocket disconnect |
| `reconnectInterval` | `number` | `1000` | Reconnect delay in ms |
| `requestTimeout` | `number` | `30_000` | Per-request timeout in ms on the WebSocket transport; raise it for very large writes, or set `0` to wait indefinitely |

## `TopologyAwareClientOptions`

Accepted by `TopologyAwareClient` from `@delali/sirannon-db/client/topology`, alongside every `ClientOptions` field. `SirannonClient` refuses each of these with `INVALID_ARGUMENT`.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `endpoints` | `string[]` | - | Starter list that coordinator mode queries for routing metadata |
| `primary` | `string` | - | Primary endpoint used directly in static mode |
| `replicas` | `string[]` | - | Replica endpoints used directly in static mode |
| `readPreference` | `'primary' \| 'replica' \| 'nearest'` | `'primary'` | Which node serves a read |
| `discovery` | `'static' \| 'coordinator'` | `'static'` | Whether routing comes from your configuration or from `GET /db/{id}/cluster` |
| `readConcern` | `'local' \| 'majority' \| 'linearizable'` | `'majority'` in coordinator mode | Client-wide read concern applied to node selection |

## `LoadAllOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `batchSize` | `number` | `1_000` | Rows per request; each batch must fit under the server's `maxBodyBytes` |
| `durability` | `'off' \| 'normal'` | `'off'` | Durability in force on the server while each batch loads |

## `SyncControllerOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `url` | `string` | required | Server base URL |
| `databaseId` | `string` | required | Database to sync against |
| `tables` | `readonly string[]` | required | Tables the device syncs |
| `headers` | `Record<string, string>` | - | Headers sent on push, snapshot, and migration requests |
| `batchSize` | `number` | `100` | Changes per push request |
| `pushIntervalMs` | `number` | `1_000` | Push loop interval, also the base for retry backoff |
| `ackIntervalMs` | `number` | `2_000` | How often the device acknowledges applied changes |
| `maxPushRetryDelayMs` | `number` | `30_000` | Ceiling for push and pull retry backoff |
| `requestTimeout` | `number` | `30_000` | HTTP request timeout in ms |
| `autoResync` | `boolean` | `true` | Download a snapshot on start, on a server resync signal, and after a failed download |
| `snapshotRetryDelayMs` | `number` | `5_000` | First delay before retrying a failed snapshot |
| `maxSnapshotRetryDelayMs` | `number` | `300_000` | Ceiling for snapshot retry backoff |
| `snapshotPageSize` | `number` | `500` | Rows per snapshot page |
| `immediateAckAfterChanges` | `number` | half the server's window | Outstanding changes that trigger an immediate acknowledgement |
| `resolver` | `ConflictResolver \| ((table: string) => ConflictResolver)` | `LWWResolver` | Conflict resolution for pulled changes |
| `onChange` | `(event: ChangeEvent) => void` | - | Called for each pulled change after it commits locally |
| `onResyncRequired` | `() => void` | - | Called before a snapshot replaces local data |
| `onSnapshotProgress` | `(progress: SnapshotProgress) => void` | - | Table and row progress during a snapshot |
| `onSnapshotComplete` | `(outcome: SnapshotOutcome) => void` | - | Called once a snapshot load ends, carrying whether the local database is usable again |

## `SnapshotDownloadOptions`

Accepted by `downloadDatabaseSnapshot(db.deviceSync(), options)`, which copies a server database into a local one outside a `SyncController`.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `url` | `string` | required | Server base URL |
| `databaseId` | `string` | required | Database to copy |
| `headers` | `Record<string, string>` | - | Headers sent on the manifest and page requests |
| `pageSize` | `number` | `500` | Rows per snapshot page |
| `requestTimeoutMs` | `number` | `30_000` | Per-request timeout in ms |
| `onProgress` | `(progress: SnapshotProgress) => void` | - | Table and row progress during the copy |

## `ReplicationOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `nodeId` | `string` | auto-generated in static mode | Unique node identifier; coordinator mode requires a stable, persisted value |
| `topology` | `Topology` | required | `PrimaryReplicaTopology` |
| `transport` | `ReplicationTransport` | required | Transport for inter-node communication |
| `transportConfig` | `TransportConfig` | `{}` | Peer endpoints and transport metadata |
| `writeForwarding` | `boolean` | `false` | Forward writes from replicas to the primary |
| `defaultConflictResolver` | `ConflictResolver` | `LWWResolver` | Default conflict resolution strategy |
| `conflictResolvers` | `Record<string, ConflictResolver>` | - | Per-table conflict resolution overrides |
| `batchSize` | `number` | `100` | Changes per replication batch |
| `batchIntervalMs` | `number` | `100` | Sender loop interval in ms |
| `maxClockDriftMs` | `number` | `60000` | Largest HLC gap between two nodes this node accepts before it rejects a batch |
| `maxPendingBatches` | `number` | `10` | In-flight batches per peer before backpressure |
| `maxBatchChanges` | `number` | `1000` | Maximum accepted changes in one inbound batch |
| `ackTimeoutMs` | `number` | `5000` | Replication batch ack timeout |
| `initialSync` | `boolean` | `true` | Pull a full snapshot when joining a cluster |
| `syncBatchSize` | `number` | `10000` | Rows per sync batch during first sync |
| `maxConcurrentSyncs` | `number` | `2` | Maximum simultaneous sync sessions on the source |
| `maxSyncDurationMs` | `number` | `1800000` | Source aborts sync after this duration |
| `maxSyncLagBeforeReady` | `number` | `100` | Catch-up lag threshold, in sequences, to reach ready |
| `syncAckTimeoutMs` | `number` | `30000` | Per-batch ack timeout during sync |
| `catchUpDeadlineMs` | `number` | `600000` | Maximum time in catch-up before transitioning to ready |
| `resumeFromSeq` | `bigint` | - | Start replication from a specific sequence (out-of-band sync) |
| `snapshotConnectionFactory` | `() => Promise<SQLiteConnection>` | - | Factory for read-only connections used during sync serving |
| `changeTracker` | `ChangeTracker` | - | CDC trigger manager, required for first sync |
| `flowControl` | `{ maxLagSeconds?, onLagExceeded? }` | - | Replication lag monitoring callbacks |
| `onBeforeForwardedQuery` | `(sql, params?) => void` | - | Validation hook called before the primary runs each forwarded statement |
| `coordinator` | `CoordinatorModeConfig` | - | Enables coordinator-backed authority and failover |

## `CoordinatorModeConfig`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `clusterId` | `string` | required | Coordinator namespace for the cluster |
| `groupId` | `string` | required | Replication group containing copies of one database |
| `endpoint` | `string` | - | Application endpoint advertised for client discovery |
| `votingDataBearingNodeIds` | `string[]` | - | Voter set used to create an unregistered group and calculate write concerns |
| `coordinator` | `ClusterCoordinator` | required | Coordinator adapter, such as the etcd adapter |
| `sessionTtlMs` | `number` | `10000` | Node-session lease lifetime |
| `controller` | `boolean \| CoordinatorControllerConfig` | enabled | Enables the controller loop or configures its lease holder, TTL, and tick interval |
| `compatibility` | `CoordinatorCompatibilityMetadata` | - | Package, specification, and protocol versions checked before promotion |

`CoordinatorControllerConfig` accepts `enabled`, `holderId`, `leaseTtlMs` (default 10,000 ms), and `tickIntervalMs` (default 1,000 ms).

## `EtcdClusterCoordinatorOptions`

Accepted by `createEtcdCoordinator` from `@delali/sirannon-db/replication/coordinator/etcd`.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `hosts` | `string \| string[]` | required | etcd endpoints; each must use `https` unless you set `allowInsecure` |
| `keyPrefix` | `string` | required | Key namespace this cluster writes under |
| `credentials` | etcd credentials | - | Root certificate, private key, and certificate chain for mutual TLS |
| `auth` | etcd auth | - | Username and password authentication |
| `grpcOptions` | `Record<string, unknown>` | - | Options passed through to the etcd gRPC channel |
| `dialTimeoutMs` | `number` | the etcd client's own default | Connection timeout in ms |
| `defaultCallTimeoutMs` | `number` | - | Deadline applied to each coordinator call in ms |
| `allowInsecure` | `boolean` | `false` | Allows plain-`http` endpoints, which belongs in tests |
| `onWatcherError` | `(error: Error) => void` | - | Called when a coordinator watcher fails |

## `TransportConfig`

| Option | Type | Description |
| --- | --- | --- |
| `endpoints` | `string[]` | Peer addresses used to establish replication connections |
| `localRole` | `'primary' \| 'replica'` | Local topology role; `ReplicationEngine` supplies this value |
| `groupId` | `string` | Replication group carried in coordinator-mode handshakes |
| `primaryTerm` | `bigint` | Current fencing term, supplied from coordinator state |
| `protocolVersion` | `string` | Replication protocol version advertised to peers |
| `metadata` | `Record<string, unknown>` | Optional custom transport metadata |

`ReplicationEngine.start()` fills in role, group, term, and protocol version. Set them yourself only when you use a `ReplicationTransport` without the engine.

## `GrpcReplicationOptions`

Accepted by `GrpcReplicationTransport` from `@delali/sirannon-db/transport/grpc`.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `string` | `'0.0.0.0'` | Address this node listens on for replication traffic |
| `port` | `number` | `0` | Listen port; `0` takes an ephemeral one |
| `tlsCert` | `string` | - | Path to this node's certificate |
| `tlsKey` | `string` | - | Path to this node's private key |
| `tlsCaCert` | `string` | - | Path to the certificate authority that signs every peer |
| `insecure` | `boolean` | `false` | Runs without TLS, which belongs in local development |
| `forwardDeadlineMs` | `number` | `30_000` | Deadline in ms for a write forwarded to the primary |
