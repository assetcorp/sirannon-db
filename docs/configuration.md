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

## `ServerOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `string` | `'127.0.0.1'` | Bind address |
| `port` | `number` | `9876` | Listen port |
| `cors` | `boolean \| CorsOptions` | `false` | CORS configuration |
| `maxBodyBytes` | `number` | `1_048_576` | Maximum HTTP body and WebSocket message size; a positive integer no larger than `4_294_967_295` |
| `maxWebSocketBackpressureBytes` | `number` | larger of `16_777_216` and `maxBodyBytes` | Bytes buffered per connection before the server closes it so the client reconnects instead of losing a frame |
| `cdcRetentionMs` | `number` | `3_600_000` | How long change events are retained, bounding change-log growth and how far back `sinceSeq` can resume |
| `maxUnacknowledgedChanges` | `number` | `1_000` | How far a device may run past its acknowledged sequence before delivery pauses; a larger transaction still arrives whole |
| `onRequest` | `OnRequestHook` | - | Middleware hook for auth, rate limiting, and request validation |

## `ClientOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `transport` | `'websocket' \| 'http'` | `'websocket'` | Transport protocol |
| `headers` | `Record<string, string>` | - | Custom HTTP headers; browser WebSocket handshakes do not use this option |
| `webSocketProtocols` | `string \| string[]` | - | WebSocket subprotocols sent during the upgrade handshake |
| `autoReconnect` | `boolean` | `true` | Reconnect on WebSocket disconnect |
| `reconnectInterval` | `number` | `1000` | Reconnect delay in ms |

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
| `maxClockDriftMs` | `number` | `60000` | Maximum tolerated HLC drift before rejecting a batch |
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
