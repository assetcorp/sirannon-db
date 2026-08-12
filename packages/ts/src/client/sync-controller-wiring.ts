import type { DeviceSyncPort } from '../core/database-sync.js'
import { toWsUrl } from './endpoint-urls.js'
import type { MigrationSyncStatus } from './migration-sync.js'
import type { SyncControllerOptions, SyncState } from './sync-controller-types.js'
import { PullStream } from './sync-pull-stream.js'
import { PushLoop } from './sync-push-loop.js'
import { ResyncScheduler } from './sync-resync-scheduler.js'

export function describeError(err: unknown): { code: string; message: string } {
  const code = err instanceof Error && 'code' in err ? String((err as { code: unknown }).code) : 'UNKNOWN_ERROR'
  return { code, message: err instanceof Error ? err.message : String(err) }
}

export const DEFAULT_BATCH_SIZE = 100
export const DEFAULT_PUSH_INTERVAL_MS = 1_000
export const DEFAULT_ACK_INTERVAL_MS = 2_000
export const DEFAULT_MAX_PUSH_RETRY_DELAY_MS = 30_000
export const DEFAULT_SNAPSHOT_RETRY_DELAY_MS = 5_000
export const DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS = 300_000

export interface SyncControllerHost {
  state(): SyncState
  port(): DeviceSyncPort | null
  schemaVersion(): number
  reconcileSchema(): Promise<MigrationSyncStatus>
  recordError(err: unknown): void
  clearError(): void
  markResyncRequired(): void
  onApplyFailure(err: unknown): void
  onApplySuccess(): void
  download(): Promise<void>
}

export interface SyncCollaborators {
  push: PushLoop
  pull: PullStream
  resync: ResyncScheduler
}

export function createSyncCollaborators(
  baseUrl: string,
  options: SyncControllerOptions,
  host: SyncControllerHost,
): SyncCollaborators {
  const isRunning = (): boolean => host.state() === 'running'

  const push = new PushLoop(
    {
      baseUrl,
      databaseId: options.databaseId,
      headers: options.headers,
      requestTimeout: options.requestTimeout,
      batchSize: options.batchSize ?? DEFAULT_BATCH_SIZE,
      intervalMs: options.pushIntervalMs ?? DEFAULT_PUSH_INTERVAL_MS,
      maxRetryDelayMs: options.maxPushRetryDelayMs ?? DEFAULT_MAX_PUSH_RETRY_DELAY_MS,
    },
    {
      isRunning,
      port: () => host.port(),
      schemaVersion: () => host.schemaVersion(),
      reconcileSchema: () => host.reconcileSchema(),
      recordError: err => host.recordError(err),
      onDrained: () => host.clearError(),
    },
  )

  const pull = new PullStream(
    {
      wsBaseUrl: toWsUrl(baseUrl),
      databaseId: options.databaseId,
      tables: options.tables,
      headers: options.headers,
      webSocketProtocols: options.webSocketProtocols,
      ackIntervalMs: options.ackIntervalMs ?? DEFAULT_ACK_INTERVAL_MS,
      requestTimeout: options.requestTimeout,
      immediateAckAfterChanges: options.immediateAckAfterChanges,
      resolver: options.resolver,
    },
    {
      isRunning,
      port: () => host.port(),
      onChange: options.onChange,
      onResyncRequired: () => host.markResyncRequired(),
      onApplyFailure: err => host.onApplyFailure(err),
      onApplySuccess: () => host.onApplySuccess(),
      recordError: err => host.recordError(err),
    },
  )

  const resync = new ResyncScheduler(
    {
      autoResync: options.autoResync,
      retryDelayMs: options.snapshotRetryDelayMs ?? DEFAULT_SNAPSHOT_RETRY_DELAY_MS,
      maxRetryDelayMs: options.maxSnapshotRetryDelayMs ?? DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS,
      onResyncRequired: options.onResyncRequired,
      onSnapshotComplete: options.onSnapshotComplete,
    },
    {
      isRunning,
      isSnapshotting: () => host.state() === 'snapshotting',
      port: () => host.port(),
      recordError: err => host.recordError(err),
      download: () => host.download(),
    },
  )

  return { push, pull, resync }
}
