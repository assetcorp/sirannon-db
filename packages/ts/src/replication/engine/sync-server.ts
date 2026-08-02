import { createHash } from 'node:crypto'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { SyncError } from '../errors.js'
import { canonicaliseForChecksum } from '../log.js'
import type { SyncAck, SyncBatch, SyncRequest, SyncTableManifest } from '../types.js'
import { IDENTIFIER_RE } from './constants.js'
import type { ReplicationEngine } from './engine.js'
import type { ActiveSyncSession, SyncAckWaiter } from './internal-types.js'
import { advanceStreamDigest } from './sync-verification.js'

export class SyncServer {
  readonly activeSyncs = new Map<string, ActiveSyncSession>()
  readonly syncAckWaiters = new Map<string, SyncAckWaiter>()

  constructor(private readonly engine: ReplicationEngine) {}

  private async rejectSyncRequest(fromPeerId: string, request: SyncRequest, error: string): Promise<void> {
    await this.engine.config.transport.sendSyncAck(
      fromPeerId,
      this.engine.decorateSyncAck({
        requestId: request.requestId,
        joinerNodeId: request.joinerNodeId,
        table: '__schema__',
        batchIndex: -1,
        success: false,
        error,
      }),
    )
  }

  async handleSyncRequest(request: SyncRequest, fromPeerId: string): Promise<void> {
    const engine = this.engine
    const knownPeers = engine.config.transport.peers()
    if (!knownPeers.has(fromPeerId)) return

    if (engine.isCoordinatorMode()) {
      try {
        await engine.verifyPrimaryAuthority()
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        await this.rejectSyncRequest(fromPeerId, request, wrappedErr.message)
        return
      }
    } else if (!engine.config.topology.canWrite()) {
      await this.rejectSyncRequest(fromPeerId, request, 'This node cannot serve syncs')
      return
    }

    if (this.activeSyncs.has(request.requestId)) {
      await this.rejectSyncRequest(fromPeerId, request, 'Duplicate requestId')
      return
    }

    if (this.activeSyncs.size >= engine.maxConcurrentSyncs) {
      await this.rejectSyncRequest(fromPeerId, request, 'Sync capacity reached, retry later')
      return
    }

    if (!engine.snapshotConnectionFactory) {
      await this.rejectSyncRequest(fromPeerId, request, 'No snapshot connection factory configured')
      return
    }

    for (const table of request.completedTables) {
      if (!IDENTIFIER_RE.test(table)) {
        await this.rejectSyncRequest(fromPeerId, request, `Invalid table name in completedTables: ${table}`)
        return
      }
    }

    let readConn: SQLiteConnection
    try {
      readConn = await engine.snapshotConnectionFactory()
    } catch (err) {
      await this.rejectSyncRequest(
        fromPeerId,
        request,
        `Failed to open snapshot connection: ${err instanceof Error ? err.message : String(err)}`,
      )
      return
    }

    const snapshotSeq = await engine.log.getLocalSeq()

    await readConn.exec('BEGIN')
    const lockStmt = await readConn.prepare('SELECT 1 FROM sqlite_master LIMIT 1')
    await lockStmt.get()

    engine.log.registerActiveSyncSeq(snapshotSeq)

    const allTables = await engine.log.getTablesInFkOrder(readConn)
    const completedSet = new Set(request.completedTables)
    const tables = allTables.filter(t => !completedSet.has(t))

    const timeoutTimer = setTimeout(
      () => this.abortSyncSession(request.requestId),
      engine.maxSyncDurationMs,
    ) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timeoutTimer.unref?.()

    const session: ActiveSyncSession = {
      requestId: request.requestId,
      joinerNodeId: request.joinerNodeId,
      readConn,
      snapshotSeq,
      tables,
      totalTables: allTables.length,
      completedTables: new Set(request.completedTables),
      startedAt: Date.now(),
      timeoutTimer,
      aborted: false,
      streamVerification: request.supportsStreamVerification === true,
      tableDigests: new Map(),
    }

    this.activeSyncs.set(request.requestId, session)

    this.serveSyncSession(session).catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'sync-session-serve', peerId: fromPeerId, recoverable: true })
      this.abortSyncSession(request.requestId)
    })
  }

  handleSyncAckReceived(ack: SyncAck): void {
    const key = `${ack.requestId}:${ack.table}:${ack.batchIndex}`
    const waiter = this.syncAckWaiters.get(key)
    if (waiter) {
      this.syncAckWaiters.delete(key)
      clearTimeout(waiter.timer)
      waiter.resolve(ack)
    }
  }

  abortSyncSession(requestId: string): void {
    const session = this.activeSyncs.get(requestId)
    if (!session) return

    session.aborted = true

    for (const [key, waiter] of this.syncAckWaiters) {
      if (key.startsWith(`${requestId}:`)) {
        this.syncAckWaiters.delete(key)
        clearTimeout(waiter.timer)
        waiter.resolve({
          requestId,
          joinerNodeId: session.joinerNodeId,
          table: '',
          batchIndex: -1,
          success: false,
          error: 'Session aborted',
        })
      }
    }

    try {
      session.readConn.close().catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.engine.emitError({
          error: wrappedErr,
          operation: 'sync-session-abort-close',
          peerId: session.joinerNodeId,
          recoverable: true,
        })
      })
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      this.engine.emitError({
        error: wrappedErr,
        operation: 'sync-session-abort-close',
        peerId: session.joinerNodeId,
        recoverable: true,
      })
    }
    this.engine.log.unregisterActiveSyncSeq(session.snapshotSeq)
    this.activeSyncs.delete(requestId)
    clearTimeout(session.timeoutTimer)
  }

  abortAll(): void {
    for (const [requestId] of this.activeSyncs) {
      this.abortSyncSession(requestId)
    }
  }

  abortSessionsForPeer(peerId: string): void {
    for (const [requestId, session] of this.activeSyncs) {
      if (session.joinerNodeId === peerId) {
        this.abortSyncSession(requestId)
      }
    }
  }

  private waitForSyncAck(requestId: string, table: string, batchIndex: number): Promise<SyncAck> {
    return new Promise((resolve, reject) => {
      const key = `${requestId}:${table}:${batchIndex}`
      const timer = setTimeout(() => {
        this.syncAckWaiters.delete(key)
        reject(new SyncError(`Sync ack timeout for ${table} batch ${batchIndex}`, requestId))
      }, this.engine.syncAckTimeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
      timer.unref?.()
      this.syncAckWaiters.set(key, {
        resolve: (ack: SyncAck) => {
          resolve(ack)
        },
        timer,
      })
    })
  }

  private async sendSyncBatchAndWaitForAck(peerId: string, batch: SyncBatch): Promise<SyncAck> {
    const ackPromise = this.waitForSyncAck(batch.requestId, batch.table, batch.batchIndex)
    try {
      await this.engine.config.transport.sendSyncBatch(peerId, this.engine.decorateSyncBatch(batch))
    } catch (err: unknown) {
      const key = `${batch.requestId}:${batch.table}:${batch.batchIndex}`
      const waiter = this.syncAckWaiters.get(key)
      if (waiter) {
        this.syncAckWaiters.delete(key)
        clearTimeout(waiter.timer)
        waiter.resolve({
          requestId: batch.requestId,
          joinerNodeId: peerId,
          table: batch.table,
          batchIndex: batch.batchIndex,
          success: false,
          error: err instanceof Error ? err.message : String(err),
        })
      }
      await ackPromise
      throw err
    }
    return ackPromise
  }

  private refreshSessionDeadline(session: ActiveSyncSession): void {
    clearTimeout(session.timeoutTimer)
    const timer = setTimeout(
      () => this.abortSyncSession(session.requestId),
      this.engine.maxSyncDurationMs,
    ) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timer.unref?.()
    session.timeoutTimer = timer
  }

  private async serveSyncSession(session: ActiveSyncSession): Promise<void> {
    const engine = this.engine
    const schemaDdl = await engine.log.dumpSchema(session.readConn)
    const schemaChecksum = createHash('sha256').update(canonicaliseForChecksum(schemaDdl)).digest('hex')

    const schemaAck = await this.sendSyncBatchAndWaitForAck(session.joinerNodeId, {
      requestId: session.requestId,
      table: '__schema__',
      batchIndex: 0,
      rows: [],
      schema: schemaDdl,
      checksum: schemaChecksum,
      isLastBatchForTable: true,
      totalTables: session.totalTables,
    })
    if (!schemaAck.success) {
      this.abortSyncSession(session.requestId)
      return
    }
    this.refreshSessionDeadline(session)

    for (const table of session.tables) {
      if (session.aborted) return

      let batchIndex = 0
      for await (const { rows, checksum, isLast } of engine.log.dumpTableOnConnection(
        session.readConn,
        table,
        engine.syncBatchSize,
      )) {
        if (session.aborted) return

        const ack = await this.sendSyncBatchAndWaitForAck(session.joinerNodeId, {
          requestId: session.requestId,
          table,
          batchIndex,
          rows,
          checksum,
          isLastBatchForTable: isLast,
        })
        if (!ack.success) {
          this.abortSyncSession(session.requestId)
          return
        }
        this.refreshSessionDeadline(session)
        session.tableDigests.set(table, advanceStreamDigest(session.tableDigests.get(table), checksum, rows.length))

        batchIndex += 1
      }

      if (batchIndex === 0) {
        const emptyChecksum = createHash('sha256').update(canonicaliseForChecksum([])).digest('hex')
        const emptyAck = await this.sendSyncBatchAndWaitForAck(session.joinerNodeId, {
          requestId: session.requestId,
          table,
          batchIndex: 0,
          rows: [],
          checksum: emptyChecksum,
          isLastBatchForTable: true,
        })
        if (!emptyAck.success) {
          this.abortSyncSession(session.requestId)
          return
        }
        this.refreshSessionDeadline(session)
        session.tableDigests.set(table, advanceStreamDigest(undefined, emptyChecksum, 0))
      }

      session.completedTables.add(table)
    }

    if (session.aborted) return

    const manifests: SyncTableManifest[] = []
    for (const table of session.tables) {
      if (session.streamVerification) {
        const streamDigest = session.tableDigests.get(table)
        if (!streamDigest) {
          throw new SyncError(`Missing stream digest for table ${table}`, session.requestId)
        }
        manifests.push({ table, rowCount: streamDigest.rowCount, batchDigest: streamDigest.digest })
      } else {
        manifests.push(await engine.log.generateManifest(session.readConn, table))
      }
    }

    await engine.config.transport.sendSyncComplete(
      session.joinerNodeId,
      engine.decorateSyncComplete({
        requestId: session.requestId,
        snapshotSeq: session.snapshotSeq,
        manifests,
      }),
    )

    try {
      await session.readConn.exec('COMMIT')
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({
        error: wrappedErr,
        operation: 'sync-session-commit',
        peerId: session.joinerNodeId,
        recoverable: true,
      })
    }
    try {
      await session.readConn.close()
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({
        error: wrappedErr,
        operation: 'sync-session-close',
        peerId: session.joinerNodeId,
        recoverable: true,
      })
    }
    engine.log.unregisterActiveSyncSeq(session.snapshotSeq)
    clearTimeout(session.timeoutTimer)
    this.activeSyncs.delete(session.requestId)
  }
}
