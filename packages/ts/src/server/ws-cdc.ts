import { type ChangeFeed, startChangeFeed } from '../core/cdc/change-feed.js'
import { pruneChangeLog } from '../core/cdc/change-log-prune.js'
import { ChangeTracker, DEFAULT_CHANGE_LOG_RETENTION_MS } from '../core/cdc/change-tracker.js'
import { type ChangeRetentionOptions, resolveDeviceRetentionPolicy } from '../core/cdc/device-retention.js'
import { ensureCdcEpoch } from '../core/cdc/epoch.js'
import { SubscriptionManager } from '../core/cdc/subscription.js'
import type { Database } from '../core/database.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import { DeviceAckWriter } from './ws-ack-writer.js'

const MAX_CONSECUTIVE_POLL_FAILURES = 10

export function toChangeRetentionOptions(options?: {
  cdcRetentionMs?: number
  deviceCursorRetentionMs?: number
  maxChangesHeldForDevice?: number
}): ChangeRetentionOptions {
  return {
    ...(options?.cdcRetentionMs === undefined ? {} : { cdcRetention: options.cdcRetentionMs }),
    ...(options?.deviceCursorRetentionMs === undefined
      ? {}
      : { deviceCursorRetention: options.deviceCursorRetentionMs }),
    ...(options?.maxChangesHeldForDevice === undefined
      ? {}
      : { maxChangesHeldForDevice: options.maxChangesHeldForDevice }),
  }
}

/**
 * The change feed that the server keeps for one database while a WebSocket subscription on it is open.
 *
 * @internal
 */
export interface CDCContext {
  /** Runs a read of the change log on a connection that leaves the writer free. */
  read: <T>(read: (conn: SQLiteConnection) => Promise<T>) => Promise<T>
  tracker: ChangeTracker
  manager: SubscriptionManager
  /** Stores device acknowledgements for this database in batches. */
  acks: DeviceAckWriter
  /** The interval at which a device stream checks a socket that holds unsent bytes, in milliseconds. */
  pollIntervalMs: number
  stopPolling: () => void
  releaseHold: () => void
  epoch: string
}

export class CdcContextRegistry {
  private readonly sirannon: Sirannon
  private readonly retentionMs: number | undefined
  private readonly serverDeviceRetention: ChangeRetentionOptions
  private readonly contexts = new Map<string, CDCContext>()
  private readonly pending = new Map<string, Promise<CDCContext>>()
  private closed = false

  constructor(sirannon: Sirannon, serverRetention: ChangeRetentionOptions = {}) {
    this.sirannon = sirannon
    this.retentionMs = serverRetention.cdcRetention
    this.serverDeviceRetention = serverRetention
  }

  async ensure(databaseId: string, database: Database): Promise<CDCContext> {
    const existing = this.contexts.get(databaseId)
    if (existing) return existing

    const pending = this.pending.get(databaseId)
    if (pending) return pending

    const promise = this.createContext(database)
    this.pending.set(databaseId, promise)
    try {
      const ctx = await promise
      if (this.closed) {
        ctx.stopPolling()
        ctx.releaseHold()
        throw new SirannonError('WebSocket handler is shut down', 'HANDLER_CLOSED')
      }
      this.contexts.set(databaseId, ctx)
      return ctx
    } finally {
      this.pending.delete(databaseId)
    }
  }

  get(databaseId: string): CDCContext | undefined {
    return this.contexts.get(databaseId)
  }

  maybeCleanup(databaseId: string): void {
    const ctx = this.contexts.get(databaseId)
    if (!ctx || ctx.manager.size > 0) return

    ctx.stopPolling()
    ctx.releaseHold()
    this.contexts.delete(databaseId)
  }

  async closeAll(): Promise<void> {
    if (this.closed) return
    this.closed = true

    for (const ctx of this.contexts.values()) {
      ctx.stopPolling()
      ctx.releaseHold()
    }
    this.contexts.clear()
  }

  private async createContext(database: Database): Promise<CDCContext> {
    const retention = database.changeRetention.cdcRetention ?? this.retentionMs
    const deviceRetention = resolveDeviceRetentionPolicy(database.changeRetention, this.serverDeviceRetention)
    const retentionMs = retention ?? DEFAULT_CHANGE_LOG_RETENTION_MS
    const tracker = new ChangeTracker({ retention: retentionMs })
    const manager = new SubscriptionManager(this.sirannon.metrics?.observeDispatch(database.id))
    const read = <T>(fn: (conn: SQLiteConnection) => Promise<T>): Promise<T> => database.readChangeLog(fn)
    let epoch = ''
    await read(conn => tracker.advanceToLatest(conn))
    await database.runCdcMaintenance(async writer => {
      epoch = await ensureCdcEpoch(writer)
    })
    if (epoch === '') {
      throw new SirannonError(`Database '${database.id}' is closed`, 'DATABASE_CLOSED')
    }

    const feed: ChangeFeed = startChangeFeed({
      tracker,
      manager,
      signal: database.changeSignal,
      intervalMs: database.cdcPollInterval,
      readChanges: () => read(conn => tracker.poll(conn)),
      prune: async () => {
        let next: number | null = null
        await database
          .runCdcMaintenance(async writer => {
            next = await pruneChangeLog(writer, tracker, deviceRetention, retentionMs)
          })
          .catch(() => {
            feed.requestPrune()
          })
        return next
      },
      onFailure: (_error, consecutiveFailures) => consecutiveFailures < MAX_CONSECUTIVE_POLL_FAILURES,
    })
    const releaseHold = database.holdFileConnection({
      release: () => feed.pause(),
      reopen: async () => {
        feed.resume()
      },
    })
    return {
      read,
      tracker,
      manager,
      acks: new DeviceAckWriter(database, () => feed.requestPrune()),
      pollIntervalMs: database.cdcPollInterval,
      stopPolling: () => feed.stop(),
      releaseHold,
      epoch,
    }
  }
}
