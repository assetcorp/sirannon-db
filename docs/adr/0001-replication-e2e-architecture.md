# Replication E2E uses same-process gRPC on loopback with mTLS everywhere

The replication system needs validation under real wire conditions, which the in-memory transport used by integration tests cannot provide. We stand up two `Sirannon` instances in the same Node process, connect them over real gRPC on loopback, and drive every scenario through the public driver API. Same-process keeps the harness in one debuggable stack with no IPC overhead while still exercising real gRPC, real protobuf encoding, and the full mTLS handshake. Every scenario runs with mTLS, so a regression in the CN-matches-nodeId validation surfaces on every run. Two-process and Docker-based topologies were considered and rejected for v1; we can add OS-level failure simulation once a scenario needs a failure the in-process harness cannot produce.

## Conventions

- Tests live at `packages/ts/src/__tests__/e2e/`.
- Vitest with `vitest.e2e.config.ts` (PR-gating) and `vitest.soak.config.ts` (nightly). pnpm scripts: `test:e2e`, `test:soak`.
- `better-sqlite3` driver on both sides via the existing `testDriver` helper.
- Filesystem temp dirs via `mkdtempSync`. Required for the later restart scenarios and matches existing replication test precedent.
- Fresh primary+replica pair in `beforeEach`. One CA plus per-node certs generated once per file in `beforeAll`; cert generation helpers reused from `grpc.test.ts`.
- gRPC port `0` (kernel-assigned); read back via the existing `transport.getPort()`.
- Strict serial delivery, one branch per layer: happy-path E2E → CDC prune chaos → fault injection → soak.
- PR-gating layers run on every PR. Soak runs nightly only.
