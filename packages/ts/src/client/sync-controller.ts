import type { Database } from '../core/database.js'
import type { DeviceSyncPort } from '../core/database-sync.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import { STAGED_STREAM_CAPABILITY } from '../server/capabilities.js'
import { toBaseUrl, toWsUrl } from './endpoint-urls.js'
import { unrefTimer } from './http-json.js'
import type { MigrationSyncStatus } from './migration-sync.js'
import { syncDeviceMigrations } from './migration-sync.js'
import { downloadDatabaseSnapshot } from './snapshot-loader.js'
import { verifyDeviceSyncCapabilities } from './sync-capabilities.js'
import type { SnapshotOptions, SyncControllerOptions, SyncState, SyncStatus } from './sync-controller-types.js'
import { PullStream } from './sync-pull-stream.js'
import { PushLoop } from './sync-push-loop.js'
import { ResyncScheduler } from './sync-resync-scheduler.js'
import { RemoteError } from './types.js'

export type {
  SnapshotOptions,
  SnapshotOutcome,
  SyncControllerOptions,
  SyncState,
  SyncStatus,
} from './sync-controller-types.js'

const DEFAULT_BATCH_SIZE = 100
const DEFAULT_PUSH_INTERVAL_MS = 1_000
const DEFAULT_ACK_INTERVAL_MS = 2_000
const DEFAULT_MAX_PUSH_RETRY_DELAY_MS = 30_000
const DEFAULT_SNAPSHOT_RETRY_DELAY_MS = 5_000
const DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS = 300_000

export class SyncController {
  private readonly baseUrl: string
  private readonly pushIntervalMs: number
  private readonly maxPushRetryDelayMs: number
  private readonly pull: PullStream
  private readonly push: PushLoop
  private readonly resync: ResyncScheduler

  private port: DeviceSyncPort | null = null
  private deviceId: string | null = null
  private capabilities: string[] | null = null
  private schemaVersion: number | null = null
  private state: SyncState = 'stopped'
  private pullRetryTimer: ReturnType<typeof setTimeout> | null = null
  private consecutivePullFailures = 0
  private lastError: { code: string; message: string } | null = null

  constructor(
    private readonly db: Database,
    private readonly options: SyncControllerOptions,
  ) {
    this.baseUrl = toBaseUrl(options.url)
    this.pushIntervalMs = options.pushIntervalMs ?? DEFAULT_PUSH_INTERVAL_MS
    this.maxPushRetryDelayMs = options.maxPushRetryDelayMs ?? DEFAULT_MAX_PUSH_RETRY_DELAY_MS
    this.push = new PushLoop(
      {
        baseUrl: this.baseUrl,
        databaseId: options.databaseId,
        headers: options.headers,
        requestTimeout: options.requestTimeout,
        batchSize: options.batchSize ?? DEFAULT_BATCH_SIZE,
        intervalMs: this.pushIntervalMs,
        maxRetryDelayMs: this.maxPushRetryDelayMs,
      },
      {
        isRunning: () => this.state === 'running',
        port: () => this.port,
        schemaVersion: () => this.schemaVersion ?? 0,
        reconcileSchema: () => this.reconcileSchema(),
        recordError: err => this.recordError(err),
        onDrained: () => {
          this.lastError = null
        },
      },
    )
    this.pull = new PullStream(
      {
        wsBaseUrl: toWsUrl(this.baseUrl),
        databaseId: options.databaseId,
        tables: options.tables,
        ackIntervalMs: options.ackIntervalMs ?? DEFAULT_ACK_INTERVAL_MS,
        requestTimeout: options.requestTimeout,
        immediateAckAfterChanges: options.immediateAckAfterChanges,
        resolver: options.resolver,
      },
      {
        isRunning: () => this.state === 'running',
        port: () => this.port,
        onChange: options.onChange,
        onResyncRequired: () => this.markResyncRequired(),
        onApplyFailure: err => this.handleApplyFailure(err),
        onApplySuccess: () => {
          this.consecutivePullFailures = 0
        },
        recordError: err => this.recordError(err),
      },
    )
    this.resync = new ResyncScheduler(
      {
        autoResync: options.autoResync,
        retryDelayMs: options.snapshotRetryDelayMs ?? DEFAULT_SNAPSHOT_RETRY_DELAY_MS,
        maxRetryDelayMs: options.maxSnapshotRetryDelayMs ?? DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS,
        onResyncRequired: options.onResyncRequired,
        onSnapshotComplete: options.onSnapshotComplete,
      },
      {
        isRunning: () => this.state === 'running',
        isSnapshotting: () => this.state === 'snapshotting',
        port: () => this.port,
        recordError: err => this.recordError(err),
        download: () =>
          this.downloadSnapshot({
            pageSize: options.snapshotPageSize,
            onProgress: options.onSnapshotProgress,
          }),
      },
    )
  }

  async start(): Promise<void> {
    if (this.state === 'running' || this.state === 'starting') return
    this.state = 'starting'
    try {
      await this.verifyCapabilities()
      this.pull.stagedStream = this.capabilities?.includes(STAGED_STREAM_CAPABILITY) ?? false
      this.port ??= this.db.deviceSync()
      this.deviceId = (await this.port.identity()).nodeId
      this.push.cursor = await this.port.getPushCursor()
      this.port.protectUnpushedChanges(this.push.cursor)
      const pullState = await this.port.getPullState()
      this.pull.pullSeq = pullState?.seq ?? null
      this.pull.pullEpoch = pullState?.epoch
      if ((await this.port.snapshotLoadPending()) || (await this.port.getResyncRequired())) {
        this.resync.markRequired()
      }
      if (!this.resync.required) {
        try {
          await this.reconcileSchema()
        } catch (err) {
          this.recordError(err)
          this.schemaVersion = await this.localSchemaVersion()
        }
      }
      if (!this.resync.required) {
        await this.openPull()
      }
      this.state = 'running'
    } catch (err) {
      this.teardownStream()
      this.state = 'stopped'
      throw err
    }
    this.push.start()
    void this.push.drain()
    if (this.resync.required) {
      this.resync.schedule()
    }
  }

  pause(): void {
    if (this.state !== 'running') return
    this.teardownStream()
    this.state = 'paused'
    void this.pull.persist()
  }

  async resume(): Promise<void> {
    if (this.state !== 'paused') return
    this.state = 'stopped'
    await this.start()
  }

  async stop(): Promise<void> {
    if (this.state === 'stopped') return
    this.teardownStream()
    this.state = 'stopped'
    await this.pull.persist()
  }

  async status(): Promise<SyncStatus> {
    const pendingPushCount = this.port ? await this.port.countOutboxPending(this.push.cursor) : 0
    return {
      state: this.state,
      deviceId: this.deviceId,
      serverCapabilities: this.capabilities,
      schemaVersion: this.schemaVersion,
      pendingPushCount,
      lastPushedSeq: this.push.cursor,
      lastPulledSeq: this.pull.pullSeq,
      pushCaughtUp: pendingPushCount === 0,
      resyncRequired: this.resync.required,
      lastError: this.lastError,
    }
  }

  triggerPush(): void {
    void this.push.drain()
  }

  private async verifyCapabilities(): Promise<void> {
    if (this.capabilities !== null) return
    try {
      this.capabilities = await verifyDeviceSyncCapabilities({
        url: this.baseUrl,
        headers: this.options.headers,
        requestTimeoutMs: this.options.requestTimeout,
      })
    } catch (err) {
      if (err instanceof RemoteError && err.code === 'SYNC_UNSUPPORTED') throw err
      this.recordError(err)
    }
  }

  private async localSchemaVersion(): Promise<number> {
    return highestMigrationVersion(await this.db.appliedMigrations())
  }

  private async reconcileSchema(): Promise<MigrationSyncStatus> {
    const result = await syncDeviceMigrations(this.db, {
      url: this.baseUrl,
      databaseId: this.options.databaseId,
      headers: this.options.headers,
      requestTimeoutMs: this.options.requestTimeout,
    })
    this.schemaVersion = result.schemaVersion
    if (result.status === 'resync-required') {
      this.markResyncRequired()
    } else if (result.status === 'ahead') {
      this.lastError = {
        code: 'SCHEMA_AHEAD',
        message: `Device schema version ${result.schemaVersion} is ahead of server version ${result.serverVersion}`,
      }
    }
    return result.status
  }

  private markResyncRequired(): void {
    this.resync.markRequired()
    this.resync.schedule()
  }

  async downloadSnapshot(options?: SnapshotOptions): Promise<void> {
    if (this.state === 'snapshotting') {
      throw new Error('A snapshot download is already in progress')
    }
    if (this.state !== 'running' && this.state !== 'paused') {
      throw new Error('Snapshot download requires a started sync controller')
    }
    const port = this.port
    if (port === null) {
      throw new Error('Snapshot download requires a started sync controller')
    }

    this.teardownStream()
    this.state = 'snapshotting'
    try {
      await this.push.drainFully(port)
      await downloadDatabaseSnapshot(port, {
        url: this.baseUrl,
        databaseId: this.options.databaseId,
        headers: this.options.headers,
        pageSize: options?.pageSize,
        requestTimeoutMs: this.options.requestTimeout,
        onProgress: options?.onProgress ?? this.options.onSnapshotProgress,
      })
      this.schemaVersion = await this.localSchemaVersion()
      await port.setResyncRequired(false)
      this.resync.recordSuccess()
      this.lastError = null
    } catch (err) {
      const failure = describeError(err)
      this.lastError = failure
      this.resync.recordFailure()
      const databaseUsable = await this.snapshotGateOpen(port)
      this.state = 'stopped'
      try {
        await this.start()
      } catch {
        this.state = 'paused'
      }
      this.resync.complete({ ok: false, error: failure, databaseUsable, retrying: this.resync.retryScheduled })
      throw err
    }
    this.state = 'stopped'
    try {
      await this.start()
    } finally {
      this.resync.complete({ ok: true, error: null, databaseUsable: true, retrying: false })
    }
  }

  /**
   * Answers whether the local database serves reads and writes again. A failure
   * before the wipe begins leaves it intact, while one after it leaves every
   * statement refused with `SNAPSHOT_IN_PROGRESS` until a later copy succeeds,
   * so the application learns which of the two it is rather than assuming.
   */
  private async snapshotGateOpen(port: DeviceSyncPort): Promise<boolean> {
    try {
      return !(await port.snapshotLoadPending())
    } catch {
      return false
    }
  }

  private recordError(err: unknown): void {
    this.lastError = describeError(err)
  }

  private handleApplyFailure(err: unknown): void {
    this.recordError(err)
    this.pull.teardown()
    const live = this.state === 'running' || this.state === 'starting'
    if (!live || this.pullRetryTimer !== null) return

    const delay = Math.min(this.pushIntervalMs * 2 ** this.consecutivePullFailures, this.maxPushRetryDelayMs)
    this.consecutivePullFailures += 1
    this.pullRetryTimer = setTimeout(() => {
      this.pullRetryTimer = null
      void this.reopenPull()
    }, delay)
    unrefTimer(this.pullRetryTimer)
  }

  private async reopenPull(): Promise<void> {
    if (this.state !== 'running' || this.deviceId === null || this.resync.required) return
    try {
      await this.openPull()
    } catch (err) {
      this.handleApplyFailure(err)
    }
  }

  /**
   * Opens the pull subscription, reconciling migrations when the server refuses
   * it because this device is behind. A device that only reads never pushes, so
   * the subscribe refusal is the sole point at which it can learn that the
   * server has migrated; without this it would retry the same refused
   * subscription forever and silently receive nothing.
   */
  private async openPull(): Promise<void> {
    const deviceId = this.deviceId
    if (deviceId === null) return

    let refusal: RemoteError
    try {
      await this.pull.open(deviceId, this.schemaVersion ?? 0)
      return
    } catch (err) {
      if (!(err instanceof RemoteError) || err.code !== 'MIGRATION_REQUIRED') throw err
      this.recordError(err)
      refusal = err
    }

    const status = await this.reconcileSchema()
    if (this.resync.required) return
    if (status === 'ahead') throw refusal

    this.pull.teardown()
    this.lastError = null
    await this.pull.open(deviceId, this.schemaVersion ?? 0)
  }

  private teardownStream(): void {
    this.push.stop()
    this.resync.cancel()
    if (this.pullRetryTimer !== null) {
      clearTimeout(this.pullRetryTimer)
      this.pullRetryTimer = null
    }
    this.pull.teardown()
  }
}

function describeError(err: unknown): { code: string; message: string } {
  const code = err instanceof Error && 'code' in err ? String((err as { code: unknown }).code) : 'UNKNOWN_ERROR'
  return { code, message: err instanceof Error ? err.message : String(err) }
}
