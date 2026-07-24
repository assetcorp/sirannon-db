import type { Database } from '../core/database.js'
import type { DeviceSyncPort } from '../core/database-sync.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import { toBaseUrl, toWsUrl } from './endpoint-urls.js'
import { unrefTimer } from './http-json.js'
import type { MigrationSyncStatus } from './migration-sync.js'
import { syncDeviceMigrations } from './migration-sync.js'
import { downloadDatabaseSnapshot } from './snapshot-loader.js'
import { verifyDeviceSyncCapabilities } from './sync-capabilities.js'
import type { SnapshotOptions, SyncControllerOptions, SyncState, SyncStatus } from './sync-controller-types.js'
import { PullStream } from './sync-pull-stream.js'
import { pushSyncBatch } from './sync-push.js'
import { RemoteError } from './types.js'

export type { SnapshotOptions, SyncControllerOptions, SyncState, SyncStatus } from './sync-controller-types.js'

const DEFAULT_BATCH_SIZE = 100
const DEFAULT_PUSH_INTERVAL_MS = 1_000
const DEFAULT_ACK_INTERVAL_MS = 2_000
const DEFAULT_MAX_PUSH_RETRY_DELAY_MS = 30_000
const DEFAULT_SNAPSHOT_RETRY_DELAY_MS = 5_000
const DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS = 300_000

export class SyncController {
  private readonly baseUrl: string
  private readonly batchSize: number
  private readonly pushIntervalMs: number
  private readonly maxPushRetryDelayMs: number
  private readonly pull: PullStream

  private port: DeviceSyncPort | null = null
  private deviceId: string | null = null
  private capabilities: string[] | null = null
  private schemaVersion: number | null = null
  private state: SyncState = 'stopped'
  private pushCursor = 0n
  private pushTimer: ReturnType<typeof setInterval> | null = null
  private pushing = false
  private consecutivePushFailures = 0
  private nextPushAttemptAt = 0
  private pushCaughtUp = false
  private resyncRequired = false
  private resyncTimer: ReturnType<typeof setTimeout> | null = null
  private consecutiveResyncFailures = 0
  private lastError: { code: string; message: string } | null = null

  constructor(
    private readonly db: Database,
    private readonly options: SyncControllerOptions,
  ) {
    this.baseUrl = toBaseUrl(options.url)
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE
    this.pushIntervalMs = options.pushIntervalMs ?? DEFAULT_PUSH_INTERVAL_MS
    this.maxPushRetryDelayMs = options.maxPushRetryDelayMs ?? DEFAULT_MAX_PUSH_RETRY_DELAY_MS
    this.pull = new PullStream(
      {
        wsBaseUrl: toWsUrl(this.baseUrl),
        databaseId: options.databaseId,
        tables: options.tables,
        ackIntervalMs: options.ackIntervalMs ?? DEFAULT_ACK_INTERVAL_MS,
        requestTimeout: options.requestTimeout,
      },
      {
        isRunning: () => this.state === 'running',
        port: () => this.port,
        onChange: options.onChange,
        onResyncRequired: () => this.markResyncRequired(),
        recordError: err => this.recordError(err),
      },
    )
  }

  async start(): Promise<void> {
    if (this.state === 'running' || this.state === 'starting') return
    this.state = 'starting'
    try {
      await this.verifyCapabilities()
      this.port ??= this.db.deviceSync()
      this.deviceId = (await this.port.identity()).nodeId
      this.pushCursor = await this.port.getPushCursor()
      this.port.protectUnpushedChanges(this.pushCursor)
      const pullState = await this.port.getPullState()
      this.pull.pullSeq = pullState?.seq ?? null
      this.pull.pullEpoch = pullState?.epoch
      if (await this.port.snapshotLoadPending()) {
        this.resyncRequired = true
      }
      if (!this.resyncRequired) {
        try {
          await this.reconcileSchema()
        } catch (err) {
          this.recordError(err)
          this.schemaVersion = await this.localSchemaVersion()
        }
      }
      if (!this.resyncRequired) {
        await this.pull.open(this.deviceId, this.schemaVersion ?? 0)
      }
      this.state = 'running'
    } catch (err) {
      this.teardownStream()
      this.state = 'stopped'
      throw err
    }
    this.pushTimer = setInterval(() => {
      void this.drainOutbox()
    }, this.pushIntervalMs)
    unrefTimer(this.pushTimer)
    void this.drainOutbox()
    if (this.resyncRequired) {
      this.scheduleAutoResync()
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
    const pendingPushCount = this.port ? await this.port.countOutboxPending(this.pushCursor) : 0
    return {
      state: this.state,
      deviceId: this.deviceId,
      serverCapabilities: this.capabilities,
      schemaVersion: this.schemaVersion,
      pendingPushCount,
      lastPushedSeq: this.pushCursor,
      lastPulledSeq: this.pull.pullSeq,
      pushCaughtUp: this.pushCaughtUp,
      resyncRequired: this.resyncRequired,
      lastError: this.lastError,
    }
  }

  triggerPush(): void {
    void this.drainOutbox()
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
    this.resyncRequired = true
    try {
      this.options.onResyncRequired?.()
    } catch {}
    this.scheduleAutoResync()
  }

  private scheduleAutoResync(): void {
    if (this.options.autoResync === false) return
    if (this.resyncTimer !== null || this.state === 'snapshotting') return
    const baseDelay = this.options.snapshotRetryDelayMs ?? DEFAULT_SNAPSHOT_RETRY_DELAY_MS
    const maxDelay = this.options.maxSnapshotRetryDelayMs ?? DEFAULT_MAX_SNAPSHOT_RETRY_DELAY_MS
    const delay =
      this.consecutiveResyncFailures === 0
        ? 0
        : Math.min(baseDelay * 2 ** (this.consecutiveResyncFailures - 1), maxDelay)
    this.resyncTimer = setTimeout(() => {
      this.resyncTimer = null
      void this.attemptAutoResync()
    }, delay)
    unrefTimer(this.resyncTimer)
  }

  private async attemptAutoResync(): Promise<void> {
    if (this.state !== 'running') return
    if (!this.resyncRequired) return
    try {
      await this.downloadSnapshot({
        pageSize: this.options.snapshotPageSize,
        onProgress: this.options.onSnapshotProgress,
      })
    } catch {
      this.scheduleAutoResync()
    }
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
      await this.drainBeforeSnapshot(port)
      await downloadDatabaseSnapshot(port, {
        url: this.baseUrl,
        databaseId: this.options.databaseId,
        headers: this.options.headers,
        pageSize: options?.pageSize,
        requestTimeoutMs: this.options.requestTimeout,
        onProgress: options?.onProgress,
      })
      this.schemaVersion = await this.localSchemaVersion()
      this.resyncRequired = false
      this.consecutiveResyncFailures = 0
      this.lastError = null
    } catch (err) {
      this.recordError(err)
      this.resyncRequired = true
      this.consecutiveResyncFailures += 1
      this.state = 'stopped'
      try {
        await this.start()
      } catch {
        this.state = 'paused'
      }
      throw err
    }
    this.state = 'stopped'
    await this.start()
  }

  private async drainBeforeSnapshot(port: DeviceSyncPort): Promise<void> {
    let retriedAfterMigration = false
    for (;;) {
      try {
        if (!(await this.pushNextBatch(port))) return
      } catch (err) {
        if (!(err instanceof RemoteError) || err.code !== 'MIGRATION_REQUIRED') throw err
        if (retriedAfterMigration) return
        retriedAfterMigration = true
        const status = await this.reconcileSchema().catch(() => 'resync-required' as const)
        if (status === 'ahead') throw err
        if (status === 'resync-required') return
      }
    }
  }

  private async pushNextBatch(port: DeviceSyncPort): Promise<boolean> {
    const batch = await port.readOutboxBatch(this.pushCursor, this.batchSize)
    if (batch === null) {
      this.pushCaughtUp = true
      return false
    }
    this.pushCaughtUp = false
    await pushSyncBatch(
      this.baseUrl,
      this.options.databaseId,
      batch,
      this.options.headers,
      this.options.requestTimeout,
      this.schemaVersion ?? 0,
    )
    this.pushCursor = batch.toSeq
    await port.setPushCursor(batch.toSeq)
    port.protectUnpushedChanges(batch.toSeq)
    return true
  }

  private async drainOutbox(): Promise<void> {
    if (this.pushing || this.state !== 'running' || this.port === null) return
    if (Date.now() < this.nextPushAttemptAt) return
    this.pushing = true
    try {
      while (this.state === 'running') {
        const pushed = await this.pushNextBatch(this.port)
        if (!pushed) break
        this.consecutivePushFailures = 0
        this.nextPushAttemptAt = 0
        this.lastError = null
      }
    } catch (err) {
      this.recordError(err)
      this.consecutivePushFailures += 1
      const delay = Math.min(this.pushIntervalMs * 2 ** this.consecutivePushFailures, this.maxPushRetryDelayMs)
      this.nextPushAttemptAt = Date.now() + delay
      if (err instanceof RemoteError && err.code === 'MIGRATION_REQUIRED' && this.state === 'running') {
        try {
          const status = await this.reconcileSchema()
          if (status === 'migrated' || status === 'in-sync') {
            this.consecutivePushFailures = 0
            this.nextPushAttemptAt = 0
          }
        } catch (reconcileErr) {
          this.recordError(reconcileErr)
        }
      }
    } finally {
      this.pushing = false
    }
  }

  private recordError(err: unknown): void {
    const code = err instanceof Error && 'code' in err ? String((err as { code: unknown }).code) : 'UNKNOWN_ERROR'
    this.lastError = { code, message: err instanceof Error ? err.message : String(err) }
  }

  private teardownStream(): void {
    if (this.pushTimer !== null) {
      clearInterval(this.pushTimer)
      this.pushTimer = null
    }
    if (this.resyncTimer !== null) {
      clearTimeout(this.resyncTimer)
      this.resyncTimer = null
    }
    this.pull.teardown()
  }
}
