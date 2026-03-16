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