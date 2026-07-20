## 0.1.8 (2026-07-20)

### 🚀 Features

- **benchmarks:** add engine cgroup support and improve CPU usage tracking in benchmarks ([d6bbcee](https://github.com/assetcorp/sirannon-db/commit/d6bbcee))
- **benchmarks:** add prepare retry and request timeout configurations ([fc03813](https://github.com/assetcorp/sirannon-db/commit/fc03813))
- **benchmarks:** introduce WorkloadStallError for improved deadline handling in workload execution ([d7db185](https://github.com/assetcorp/sirannon-db/commit/d7db185))

### 🩹 Fixes

- **ts:** ensure proper cancellation handling for worker requests and improve timeout management ([4c383a6](https://github.com/assetcorp/sirannon-db/commit/4c383a6))

### ❤️ Thank You

- assetcorp

## 0.1.7 (2026-07-20)

### 🚀 Features

- introduce cloud benchmarking toolkit with support for multiple providers and automated result handling ([a98b3fc](https://github.com/assetcorp/sirannon-db/commit/a98b3fc))
- add benchmark harness for Sirannon vs PostgreSQL comparison with Docker support and detailed reporting ([cf2cd86](https://github.com/assetcorp/sirannon-db/commit/cf2cd86))
- add smoke preset for quick benchmarking and update README with usage instructions ([5f0e865](https://github.com/assetcorp/sirannon-db/commit/5f0e865))
- enhance cloud benchmarking with new profile options and update documentation ([87495aa](https://github.com/assetcorp/sirannon-db/commit/87495aa))
- add timezone support for backup scheduling, including daylight saving time handling and validation for unknown timezones ([8eb7677](https://github.com/assetcorp/sirannon-db/commit/8eb7677))
- implement bulk load feature with configurable durability and synchronous levels, improving performance for batch operations ([3d8e494](https://github.com/assetcorp/sirannon-db/commit/3d8e494))
- improve bulk load functionality with checkpointing ([fa8f4ba](https://github.com/assetcorp/sirannon-db/commit/fa8f4ba))
- add batch and load methods to transport layers, enhancing bulk operation capabilities with configurable durability and request timeout ([c1faf3a](https://github.com/assetcorp/sirannon-db/commit/c1faf3a))
- introduce timeout configurations and deadline handling in benchmark scripts and drivers to enhance stability and performance ([f2dffc9](https://github.com/assetcorp/sirannon-db/commit/f2dffc9))
- improvr subscription with optional reset handling and backpressure management ([200976f](https://github.com/assetcorp/sirannon-db/commit/200976f))
- enhance WebSocket transport with epoch handling and introduce ForbiddenSqlError for reserved table access ([87a1a62](https://github.com/assetcorp/sirannon-db/commit/87a1a62))
- refine internal table access control by distinguishing between read and write operations for reserved identifiers ([8522ffc](https://github.com/assetcorp/sirannon-db/commit/8522ffc))
- implement encoding and decoding for tagged values in WebSocket transport, enhancing support for BigInt and binary data in change events ([e916cfb](https://github.com/assetcorp/sirannon-db/commit/e916cfb))
- implement value encoding for BigInt and BLOB types across transports ([765517b](https://github.com/assetcorp/sirannon-db/commit/765517b))
- improve FieldMergeResolver and ConvergenceOracle to handle BigInt and BLOB types without throwing errors, and improve data comparison using canonicalisation for checksum ([2d669dc](https://github.com/assetcorp/sirannon-db/commit/2d669dc))
- add queryForWire method for efficient single-pass reading of encoded rows, enhancing performance for BigInt and BLOB types across database operations ([827dab9](https://github.com/assetcorp/sirannon-db/commit/827dab9))
- improve bulk load functionality with optional checkpoint control ([115cdba](https://github.com/assetcorp/sirannon-db/commit/115cdba))
- implement loadAll method for efficient bulk loading with batching and durability control ([38fd3df](https://github.com/assetcorp/sirannon-db/commit/38fd3df))
- reimplement the benchmarks agianst running in docker ([824d1d2](https://github.com/assetcorp/sirannon-db/commit/824d1d2))
- enhance error handling and connection retries in measureCdcLatency function ([a3350b1](https://github.com/assetcorp/sirannon-db/commit/a3350b1))
- implement soak phase in benchmarks to evaluate sustained performance over extended periods ([4791de7](https://github.com/assetcorp/sirannon-db/commit/4791de7))
- add VM_MAX_HOURS variable for self-delete backstop in cloud benchmarks ([7872fe7](https://github.com/assetcorp/sirannon-db/commit/7872fe7))
- **benchmark:** rewrite benchmark harness in node since sirannon runs note and node-pg is also available for postgress ([97f488c](https://github.com/assetcorp/sirannon-db/commit/97f488c))
- **ts:** introduce writer worker for offloading disk writes ([56944a7](https://github.com/assetcorp/sirannon-db/commit/56944a7))
- **ts:** add writerWorker option to Sirannon for configurable write handling ([4b54609](https://github.com/assetcorp/sirannon-db/commit/4b54609))
- **ts:** implement GroupCommitter for batching concurrent writes in a single transaction ([e7d5a54](https://github.com/assetcorp/sirannon-db/commit/e7d5a54))
- **ts:** add DatabaseCdcController for change data capture and enhance transaction handling ([717ac6c](https://github.com/assetcorp/sirannon-db/commit/717ac6c))
- **ts:** introduce DatabaseObserver for improved query observability and metrics tracking ([2b2fe3b](https://github.com/assetcorp/sirannon-db/commit/2b2fe3b))
- **ts:** implement pass classification and verdict determination for load testing ([4a20c89](https://github.com/assetcorp/sirannon-db/commit/4a20c89))
- **ts:** enhance WebSocketTransport to support transactions and improve transaction handling in tests ([dd9804e](https://github.com/assetcorp/sirannon-db/commit/dd9804e))
- **ts:** add support for writer worker offloading in SQLite drivers and enhance CI with browser bundle validation ([6445470](https://github.com/assetcorp/sirannon-db/commit/6445470))
- **ts:** introduce bundle checking and backup engine support in SQLite drivers ([3b9f722](https://github.com/assetcorp/sirannon-db/commit/3b9f722))

### 🩹 Fixes

- properly parse env vars ([8891bcf](https://github.com/assetcorp/sirannon-db/commit/8891bcf))
- update README and common script to clarify VM teardown behavior on exit ([bc908f1](https://github.com/assetcorp/sirannon-db/commit/bc908f1))
- replace Math.max with custom maxOf function to handle large sample arrays without stack overflow in latency measurements ([96f0695](https://github.com/assetcorp/sirannon-db/commit/96f0695))
- address issues with batch import durability and clean up code ([6a40c6e](https://github.com/assetcorp/sirannon-db/commit/6a40c6e))
- address issues with memory leaks ([1c86df9](https://github.com/assetcorp/sirannon-db/commit/1c86df9))
- **ts:** reject maxBodyBytes/backpressure above uWS 32-bit limit ([66e2413](https://github.com/assetcorp/sirannon-db/commit/66e2413))
- **ts:** address worker crash issue ([d89eb4e](https://github.com/assetcorp/sirannon-db/commit/d89eb4e))

### ❤️ Thank You

- assetcorp

## 0.1.6 (2026-07-05)

### 🚀 Features

- **ts:** add totalTables field to SyncBatch and related structures for improved sync progress tracking ([4be33e4](https://github.com/assetcorp/sirannon-db/commit/4be33e4))

### ❤️ Thank You

- assetcorp

## 0.1.5 (2026-06-20)

### 🚀 Features

- implement coordinator-backed automatic failover and enhance replication specifications ([3c24e28](https://github.com/assetcorp/sirannon-db/commit/3c24e28))
- **docs:** update README and TypeScript documentation to reflect new features in distributed replication, including conflict resolution, coordinator-backed failover, and enhanced security measures ([e5f523c](https://github.com/assetcorp/sirannon-db/commit/e5f523c))
- **spec:** introduce specifications for Sirannon's driver, core, replication, transport, server, client, and error handling modules ([69bef40](https://github.com/assetcorp/sirannon-db/commit/69bef40))
- **tests:** add end-to-end chaos replication tests with mTLS and improve node storage handling ([63e1b9e](https://github.com/assetcorp/sirannon-db/commit/63e1b9e))
- **ts:** add pool size sweep benchmark and improve existing benchmarks ([8ba32b1](https://github.com/assetcorp/sirannon-db/commit/8ba32b1))
- **ts:** improve benchmarks documentation with new chart types and reference results; remove outdated CSV files ([5f016a0](https://github.com/assetcorp/sirannon-db/commit/5f016a0))
- **ts:** propagate read and write concerns through query hooks ([3c0176b](https://github.com/assetcorp/sirannon-db/commit/3c0176b))
- **ts:** extend change tracker for replication workloads ([04960f4](https://github.com/assetcorp/sirannon-db/commit/04960f4))
- **ts:** add replication engine with Raft, log, and conflict resolution ([80efba5](https://github.com/assetcorp/sirannon-db/commit/80efba5))
- **ts:** add memory and websocket replication transports ([dd19768](https://github.com/assetcorp/sirannon-db/commit/dd19768))
- **ts:** add topology-aware Sirannon client ([2856a01](https://github.com/assetcorp/sirannon-db/commit/2856a01))
- **ts:** expose replication status on readiness endpoint ([38fded6](https://github.com/assetcorp/sirannon-db/commit/38fded6))
- **ts:** implement forward authorization in replication engine and enhance WebSocket transport with authentication ([9984661](https://github.com/assetcorp/sirannon-db/commit/9984661))
- **ts:** add recordColumnVersions method to track column changes in replication log ([c73d50a](https://github.com/assetcorp/sirannon-db/commit/c73d50a))
- **ts:** improve replication and transport layers with new features and error handling improvements ([dad6693](https://github.com/assetcorp/sirannon-db/commit/dad6693))
- **ts:** add safety checks for SQL statements in ReplicationEngine and enhance DDL validation in log module ([3a66bfa](https://github.com/assetcorp/sirannon-db/commit/3a66bfa))
- **ts:** implement convergence and fault policy mechanisms in simulated transport layer ([58f56c2](https://github.com/assetcorp/sirannon-db/commit/58f56c2))
- **ts:** enhance replication engine with acknowledgment timeout and in-flight batch tracking ([465a44e](https://github.com/assetcorp/sirannon-db/commit/465a44e))
- **ts:** implement initial sync functionality in ReplicationEngine with enhanced sync state management and error handling ([e45265e](https://github.com/assetcorp/sirannon-db/commit/e45265e))
- **ts:** enhance replication and transport layers with improved error handling, transport configuration, and type safety ([1585b5c](https://github.com/assetcorp/sirannon-db/commit/1585b5c))
- **ts:** improve ChangeTracker and ReplicationEngine ([7c82874](https://github.com/assetcorp/sirannon-db/commit/7c82874))
- **ts:** use gRPC transport  in place of websockets ([701fbf4](https://github.com/assetcorp/sirannon-db/commit/701fbf4))
- **ts:** add TLS identity validation and peer resolution in gRPC transport ([a17a1fa](https://github.com/assetcorp/sirannon-db/commit/a17a1fa))
- **ts:** enhance ChangeTracker and ReplicationEngine with DDL handling and trigger management ([2ab0236](https://github.com/assetcorp/sirannon-db/commit/2ab0236))
- **ts:** add soak testing configuration and related tests ([1ea4240](https://github.com/assetcorp/sirannon-db/commit/1ea4240))
- **ts:** add etcd3 coordinator support and enhance replication client with read concern handling ([965270c](https://github.com/assetcorp/sirannon-db/commit/965270c))
- **ts:** enhance replication coordinator with compatibility checks and automatic failover requirements ([6c42888](https://github.com/assetcorp/sirannon-db/commit/6c42888))
- **ts:** add failover testing framework and configurations for enhanced replication resilience ([a96cd27](https://github.com/assetcorp/sirannon-db/commit/a96cd27))
- **ts:** enhance failover node process with durability point checks and admission logic for in-sync set ([fa00e93](https://github.com/assetcorp/sirannon-db/commit/fa00e93))
- **ts:** implement execution target resolution for server and enhance HTTP/WS handlers for query execution ([eb62ce9](https://github.com/assetcorp/sirannon-db/commit/eb62ce9))
- **ts:** add advanceToLatest method to ChangeTracker and update WSHandler to utilize it for improved change tracking ([3d874d5](https://github.com/assetcorp/sirannon-db/commit/3d874d5))
- **ts:** enhance activity and product management with normalization functions and improved event handling ([e96d314](https://github.com/assetcorp/sirannon-db/commit/e96d314))
- **ts:** update web-client example with new fulfillment operations, improved server setup, and enhanced package dependencies ([f7a858a](https://github.com/assetcorp/sirannon-db/commit/f7a858a))
- **ts:** enhance security model in web-client example with WebSocket authentication and protocol validation ([9d7cfdd](https://github.com/assetcorp/sirannon-db/commit/9d7cfdd))
- **ts:** implement CDC event handling improvements and enhance BLOB decoding in change tracker ([7622f04](https://github.com/assetcorp/sirannon-db/commit/7622f04))
- **ts:** add distributed entitlements example with Docker setup, gRPC replication, and enhanced UI components ([0af9981](https://github.com/assetcorp/sirannon-db/commit/0af9981))
- **ts:** implement cluster routing fingerprinting and subscription migration for topology-aware transport ([843a606](https://github.com/assetcorp/sirannon-db/commit/843a606))
- **ts:** enhance distributed entitlements UI with new components and improved styling ([327cd38](https://github.com/assetcorp/sirannon-db/commit/327cd38))
- **ts:** improve ui ([fa8660b](https://github.com/assetcorp/sirannon-db/commit/fa8660b))
- **ts:** implement majority write availability checks and enhance UI components for distributed entitlements ([7229cc8](https://github.com/assetcorp/sirannon-db/commit/7229cc8))

### 🩹 Fixes

- address all codrabbit reported issues ([960e7f5](https://github.com/assetcorp/sirannon-db/commit/960e7f5))
- **ts:** update bulk-insert benchmark to cap at 10K rows and improve documentation on performance metrics ([d33d5a9](https://github.com/assetcorp/sirannon-db/commit/d33d5a9))
- **ts:** address security and reliability issues ([6a25549](https://github.com/assetcorp/sirannon-db/commit/6a25549))
- **ts:** include options parameter in query method for improved database querying ([22328f8](https://github.com/assetcorp/sirannon-db/commit/22328f8))
- **ts:** update lastSentSeq handling in ReplicationEngine to ensure accurate batch processing ([e30e808](https://github.com/assetcorp/sirannon-db/commit/e30e808))
- **ts:** update connected peer check to use isConnected property for accurate transport status ([e07029f](https://github.com/assetcorp/sirannon-db/commit/e07029f))
- **ts:** address issues with old raft replication ([1edf150](https://github.com/assetcorp/sirannon-db/commit/1edf150))
- **ts:** address all security and reliability issues reported by coderabbitai ([99e131f](https://github.com/assetcorp/sirannon-db/commit/99e131f))
- **ts:** address issues ([74a8350](https://github.com/assetcorp/sirannon-db/commit/74a8350))
- **ts:** update SQL logic for duplicate outcomes ([b94522b](https://github.com/assetcorp/sirannon-db/commit/b94522b))
- **ts:** address selection retry after coordinator primary change ([bb6ba6d](https://github.com/assetcorp/sirannon-db/commit/bb6ba6d))

### ❤️ Thank You

- assetcorp

## 0.1.4 (2026-03-20)

### 🚀 Features

- **ts:** add pluggable SQLite driver adapter system ([58dc122](https://github.com/assetcorp/sirannon-db/commit/58dc122))
- **ts:** improve wa-sqlite driver and tsup build config ([8577ace](https://github.com/assetcorp/sirannon-db/commit/8577ace))
- **ts:** update benchmarks for pluggable driver and async API ([4322c04](https://github.com/assetcorp/sirannon-db/commit/4322c04))
- **ts:** add example projects for each runtime ([e10109a](https://github.com/assetcorp/sirannon-db/commit/e10109a))
- **ts:** overhaul web client for product inventory management with real-time updates ([534ed8f](https://github.com/assetcorp/sirannon-db/commit/534ed8f))

### 🩹 Fixes

- **ts:** idle check improvement for LifecycleManager ([7a58903](https://github.com/assetcorp/sirannon-db/commit/7a58903))
- **ts:** validate extension path for control characters and improve path resolution in Database class ([832abeb](https://github.com/assetcorp/sirannon-db/commit/832abeb))
- **ts:** address SQLite transaction hanlding in expo ([adcf792](https://github.com/assetcorp/sirannon-db/commit/adcf792))
- **ts:** address flushAsync function in WSHandler tests for better message handling ([f08cb87](https://github.com/assetcorp/sirannon-db/commit/f08cb87))
- **ts:** address isseus with error handling in SirannonServer to manage unexpected errors during request processing ([5c86b3e](https://github.com/assetcorp/sirannon-db/commit/5c86b3e))
- **ts:** move beforeConnect hook invocation to ensure it executes before database creation in Sirannon ([b05cc60](https://github.com/assetcorp/sirannon-db/commit/b05cc60))
- **ts:** fix backup path validation to prevent directory traversal and control characters in migration paths ([94d4429](https://github.com/assetcorp/sirannon-db/commit/94d4429))
- **ts:** enforce read-only checks in Database methods and handle shutdown errors in Sirannon ([dc58216](https://github.com/assetcorp/sirannon-db/commit/dc58216))

### ❤️ Thank You

- assetcorp

## 0.1.3 (2026-03-16)

### 🚀 Features

- **ts:** implement core database modules with CDC, hooks, metrics, and connection pooling ([f36acb1](https://github.com/assetcorp/sirannon-db/commit/f36acb1))
- **ts:** improve connection pool and database error handling ([03f2d0a](https://github.com/assetcorp/sirannon-db/commit/03f2d0a))
- **ts:** implement health and HTTP server endpoints for sirannon-db ([136087a](https://github.com/assetcorp/sirannon-db/commit/136087a))
- **ts:** implement Sirannon client and database proxy with HTTP and WebSocket transport support ([2e93539](https://github.com/assetcorp/sirannon-db/commit/2e93539))
- **ts:** add backup and migration management features with corresponding tests ([8b05527](https://github.com/assetcorp/sirannon-db/commit/8b05527))
- **ts:** enhance Database and ConnectionPool with extension loading, CDC support, and integration tests ([be21f3f](https://github.com/assetcorp/sirannon-db/commit/be21f3f))
- **ts:** add benchmarking suite for Sirannon with Postgres integration, including various benchmark scenarios and results reporting ([8bf6d06](https://github.com/assetcorp/sirannon-db/commit/8bf6d06))
- **ts:** introduce Docker-based benchmarking suite for Sirannon, including end-to-end and engine-level benchmarks with detailed results reporting ([e09d60a](https://github.com/assetcorp/sirannon-db/commit/e09d60a))
- **ts:** expand benchmarking suite with concurrent read and mixed benchmarks, enhance configuration options, and improve statistical analysis features ([a9bf2e3](https://github.com/assetcorp/sirannon-db/commit/a9bf2e3))
- **ts:** update benchmarks to use batch processing, also improve statistical calculations, and Postgres connection handling ([999e540](https://github.com/assetcorp/sirannon-db/commit/999e540))
- **ts:** enhance benchmark engine with improved POST request handling and dynamic timeout estimation ([b5e0270](https://github.com/assetcorp/sirannon-db/commit/b5e0270))
- **ts:** update benchmarking suite to include concurrency scaling tests, replace outdated benchmarks, and enhance configuration for improved performance ([aa683ca](https://github.com/assetcorp/sirannon-db/commit/aa683ca))
- **ts:** improve migration system with file-based migrations and rollbacks ([f100fe9](https://github.com/assetcorp/sirannon-db/commit/f100fe9))
- **ts:** add statistical analysis features and CSV output for results ([943d89a](https://github.com/assetcorp/sirannon-db/commit/943d89a))
- **ts:** improve benchmarking configuration and add CSV escaping utility with comprehensive tests ([507c1ff](https://github.com/assetcorp/sirannon-db/commit/507c1ff))
- **ts:** improve test coverage for backup and subscription management ([1febd79](https://github.com/assetcorp/sirannon-db/commit/1febd79))
- **ts:** add new benchmark results and visualizations for micro-batch update, point select, and TPC-C lite workloads ([7a34fa3](https://github.com/assetcorp/sirannon-db/commit/7a34fa3))

### 🩹 Fixes

- **ts:** improve RNG output precision ([c8d5f2a](https://github.com/assetcorp/sirannon-db/commit/c8d5f2a))
- **ts:** refine CORS handling in server and improve test coverage for origin matching ([ec9b2b9](https://github.com/assetcorp/sirannon-db/commit/ec9b2b9))

### ❤️ Thank You

- assetcorp