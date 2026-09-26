import type { Database } from '../core/database.js'
import type { DeviceSyncPort } from '../core/database-sync.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import { STAGED_STREAM_CAPABILITY } from '../server/capabilities.js'
import { toBaseUrl } from './endpoint-urls.js'
import { unrefTimer } from './http-json.js'
import type { MigrationSyncStatus } from './migration-sync.js'
import { syncDeviceMigrations } from './migration-sync.js'
import { downloadDatabaseSnapshot } from './snapshot-loader.js'
import { verifyDeviceSyncCapabilities } from './sync-capabilities.js'
import type { SnapshotOptions, SyncControllerOptions, SyncState, SyncStatus } from './sync-controller-types.js'
import {
  createSyncCollaborators,
  DEFAULT_MAX_PUSH_RETRY_DELAY_MS,
  DEFAULT_PUSH_INTERVAL_MS,
  describeError,
} from './sync-controller-wiring.js'
import type { PullStream } from './sync-pull-stream.js'
import type { PushLoop } from './sync-push-loop.js'
import type { ResyncScheduler } from './sync-resync-scheduler.js'
import { SyncStatusNotifier } from './sync-status-notifier.js'
import { assertWebSocketCredentials } from './transport/ws-headers.js'
import { RemoteError } from './types.js'

const SERVER_REFUSES_DEVICE_SYNC_CODES = new Set(['SYNC_UNSUPPORTED', 'DEVICE_SYNC_NOT_ACCEPTED'])

export type {
  SnapshotOptions,
  SnapshotOutcome,
  SyncControllerOptions,
  SyncState,
  SyncStatus,
} from './sync-controller-types.js'

/**
 * Keeps one device's local database in step with a server by pushing local changes, pulling the server's changes, and downloading a fresh snapshot when the server requires a resync.
 *
 * @public
 */
export class SyncController {
  private readonly baseUrl: string
  private readonly pushIntervalMs: number
  private readonly maxPushRetryDelayMs: number
  private readonly pull: PullStream
  private readonly push: PushLoop
  private readonly resync: ResyncScheduler
  private readonly statusChanges: SyncStatusNotifier

  private port: DeviceSyncPort | null = null
  private deviceId: string | null = null
  private capabilities: string[] | null = null
  private schemaVersion: number | null = null
  private syncState: SyncState = 'stopped'
  private pullRetryTimer: ReturnType<typeof setTimeout> | null = null
  private consecutivePullFailures = 0
  private pendingPushCount = 0
  private lastError: { code: string; message: string } | null = null

  constructor(
    private readonly db: Database,
    private readonly options: SyncControllerOptions,
  ) {
    this.baseUrl = toBaseUrl(options.url)
    this.pushIntervalMs = options.pushIntervalMs ?? DEFAULT_PUSH_INTERVAL_MS
    this.maxPushRetryDelayMs = options.maxPushRetryDelayMs ?? DEFAULT_MAX_PUSH_RETRY_DELAY_MS
    assertWebSocketCredentials(options.headers, options.webSocketProtocols)
    this.statusChanges = new SyncStatusNotifier(
      options.onStatusChange,
      () => this.captureStatus(),
      () => this.refreshOutboxCount(),
    )
    const collaborators = createSyncCollaborators(this.baseUrl, options, {
      state: () => this.state,
      port: () => this.port,
      schemaVersion: () => this.schemaVersion ?? 0,
      reconcileSchema: () => this.reconcileSchema(),
      recordError: err => this.recordError(err),
      clearError: () => this.setError(null),
      markResyncRequired: () => this.markResyncRequired(),
      onApplyFailure: err => this.handleApplyFailure(err),
      onApplySuccess: () => {
        this.consecutivePullFailures = 0
        this.statusChanges.notify()
      },
      download: () =>
        this.downloadSnapshot({ pageSize: options.snapshotPageSize, onProgress: options.onSnapshotProgress }),
    })
    this.push = collaborators.push
    this.pull = collaborators.pull
    this.resync = collaborators.resync
  }

  private get state(): SyncState {
    return this.syncState
  }

  private setState(next: SyncState): void {
    if (this.syncState === next) return
    this.syncState = next
    this.statusChanges.notify()
  }

  private setError(failure: { code: string; message: string } | null): void {
    this.lastError = failure
    this.statusChanges.notify()
  }

  /**
   * Connects to the server and starts pushing and pulling changes.
   */
  async start(): Promise<void> {
    if (this.state === 'running' || this.state === 'starting') return
    this.setState('starting')
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
      this.setState('running')
    } catch (err) {
      this.teardownStream()
      this.setState('stopped')
      throw err
    }
    this.push.start()
    void this.push.drain()
    if (this.resync.required) {
      this.resync.schedule()
    }
  }

  /**
   * Pauses pushing and pulling and closes the pull connection until you call {@link SyncController.resume}.
   */
  pause(): void {
    if (this.state !== 'running') return
    this.teardownStream()
    this.setState('paused')
    void this.pull.persist()
  }

  /**
   * Resumes pushing and pulling after a pause.
   */
  async resume(): Promise<void> {
    if (this.state !== 'paused') return
    this.setState('stopped')
    await this.start()
  }

  /**
   * Stops syncing and closes the connection to the server.
   */
  async stop(): Promise<void> {
    if (this.state === 'stopped') return
    this.teardownStream()
    this.setState('stopped')
    await this.pull.persist()
  }

  /**
   * Returns this device's sync status, with the pending push count read fresh from the outbox.
   *
   * @returns The device's state, cursors, pending push count, and last failure.
   */
  async status(): Promise<SyncStatus> {
    await this.refreshOutboxCount()
    return this.captureStatus()
  }

  private captureStatus(): SyncStatus {
    return {
      state: this.state,
      deviceId: this.deviceId,
      serverCapabilities: this.capabilities,
      schemaVersion: this.schemaVersion,
      pendingPushCount: this.pendingPushCount,
      lastPushedSeq: this.push.cursor,
      lastPulledSeq: this.pull.pullSeq,
      pushCaughtUp: this.pendingPushCount === 0,
      resyncRequired: this.resync.required,
      lastError: this.lastError,
    }
  }

  private async refreshOutboxCount(): Promise<boolean> {
    const counted = this.port === null ? 0 : await this.port.countOutboxPending(this.push.cursor)
    const changed = counted !== this.pendingPushCount
    this.pendingPushCount = counted
    return changed
  }

  /**
   * Pushes local changes now, ahead of the next push interval.
   */
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
      if (err instanceof RemoteError && SERVER_REFUSES_DEVICE_SYNC_CODES.has(err.code)) throw err
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
      this.setError({
        code: 'SCHEMA_AHEAD',
        message: `Device schema version ${result.schemaVersion} is ahead of server version ${result.serverVersion}`,
      })
    }
    return result.status
  }

  private markResyncRequired(): void {
    this.resync.markRequired()
    this.resync.schedule()
    this.statusChanges.notify()
  }

  /**
   * Replaces the local database with a fresh copy from the server and resumes syncing from it.
   *
   * @param options - Page size and the progress callback for this download.
   */
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
    this.setState('snapshotting')
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
      this.setError(null)
    } catch (err) {
      const failure = describeError(err)
      this.setError(failure)
      this.resync.recordFailure()
      const databaseUsable = await this.snapshotGateOpen(port)
      this.setState('stopped')
      try {
        await this.start()
      } catch {
        this.setState('paused')
      }
      this.resync.complete({ ok: false, error: failure, databaseUsable, retrying: this.resync.retryScheduled })
      throw err
    }
    this.setState('stopped')
    try {
      await this.start()
    } finally {
      this.resync.complete({ ok: true, error: null, databaseUsable: true, retrying: false })
    }
  }

  /**
   * Returns whether the local database accepts reads and writes again. When a download
   * fails before the wipe begins, the database stays intact. When it fails after, the
   * database rejects every statement with `SNAPSHOT_IN_PROGRESS` until a later download
   * succeeds, so the application can read the outcome's `databaseUsable` to tell them apart.
   */
  private async snapshotGateOpen(port: DeviceSyncPort): Promise<boolean> {
    try {
      return !(await port.snapshotLoadPending())
    } catch {
      return false
    }
  }

  private recordError(err: unknown): void {
    this.setError(describeError(err))
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
   * Opens the pull subscription, and reconciles migrations when the server refuses
   * it with `MIGRATION_REQUIRED` because this device's schema is behind. A device
   * that only reads never pushes, so the refused subscription is the only signal
   * that the server has migrated. Without this step, the controller would retry the
   * same refused subscription indefinitely and the device would receive no changes.
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
    this.setError(null)
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
