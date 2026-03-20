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