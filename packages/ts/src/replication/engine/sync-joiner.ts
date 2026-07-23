import { createHash, randomUUID } from 'node:crypto'
import { APPLIED_CHANGES_TABLE } from '../../core/internal-tables.js'
import { SyncError } from '../errors.js'
import { canonicaliseForChecksum } from '../log.js'
import type { SyncAck, SyncBatch, SyncComplete } from '../types.js'
import { IDENTIFIER_RE, isSyncSafeDdl } from './constants.js'
import type { ReplicationEngine } from './engine.js'
import { advanceStreamDigest, matchesStreamDigest } from './sync-verification.js'
import { delayAckIfConfigured } from './test-hooks.js'

export class SyncJoiner {
  private catchUpCheckTimer: ReturnType<typeof setInterval> | null = null

  constructor(private readonly engine: ReplicationEngine) {}

  async initiateSync(): Promise<void> {
    const engine = this.engine
    if (engine.syncState.phase !== 'pending') return
    if (!engine.tracker) {
      throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
    }

    const peers = engine.config.transport.peers()
    let sourcePeerId: string | null = engine.isCoordinatorMode() ? engine.getCurrentPrimaryPeerId() : null

    if (engine.isCoordinatorMode() && sourcePeerId === null) {
      return
    }

    if (sourcePeerId === null) {
      for (const [peerId, info] of peers) {
        if (info.role === 'primary') {
          sourcePeerId = peerId
          break
        }
      }
    }

    if (sourcePeerId === null) {
      for (const [peerId] of peers) {
        sourcePeerId = peerId
        break
      }
    }

    if (sourcePeerId === null) {
      return
    }

    engine.syncState.phase = 'syncing'
    engine.syncState.sourcePeerId = sourcePeerId
    engine.syncState.startedAt = Date.now()
    engine.syncState.error = null

    try {
      const savedState = await engine.log.getSyncState()
      const completedTables = savedState.phase === 'pending' ? [] : savedState.completedTables
      const requestId = randomUUID()
      engine.syncTableDigests.clear()
      await engine.log.setSyncMeta('syncing', undefined, sourcePeerId, requestId)
      await engine.config.transport.requestSync(
        sourcePeerId,
        engine.decorateSyncRequest({
          requestId,
          joinerNodeId: engine.nodeId,
          completedTables,
          supportsStreamVerification: true,
        }),
      )
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      if (engine.syncState.phase === 'syncing' && engine.syncState.sourcePeerId === sourcePeerId) {
        engine.syncState.phase = 'pending'
        engine.syncState.sourcePeerId = null
        engine.syncState.startedAt = null
        engine.syncState.error = wrappedErr.message
        try {
          await engine.log.setSyncMeta('pending')
        } catch (rollbackErr: unknown) {
          const wrappedRollbackErr = rollbackErr instanceof Error ? rollbackErr : new Error(String(rollbackErr))
          engine.emitError({
            error: wrappedRollbackErr,
            operation: 'sync-request-state-rollback',
            peerId: sourcePeerId,
            recoverable: false,
          })
        }
      }
      throw wrappedErr
    }
  }

  async handleSyncBatchReceived(batch: SyncBatch, fromPeerId: string): Promise<void> {
    const engine = this.engine
    if (engine.syncState.phase !== 'syncing') return
    if (fromPeerId !== engine.syncState.sourcePeerId) return

    if (!engine.tracker) {
      throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
    }

    const expectedIndex = engine.expectedBatchIndex.get(batch.table) ?? 0
    if (batch.batchIndex !== expectedIndex) {
      await this.sendSyncAck(fromPeerId, {
        requestId: batch.requestId,
        joinerNodeId: engine.nodeId,
        table: batch.table,
        batchIndex: batch.batchIndex,
        success: false,
        error: `Out of order batch: expected ${expectedIndex}, got ${batch.batchIndex}`,
      })
      return
    }
    engine.expectedBatchIndex.set(batch.table, expectedIndex + 1)

    try {
      if (batch.table === '__schema__') {
        if (typeof batch.totalTables === 'number' && Number.isInteger(batch.totalTables) && batch.totalTables >= 0) {
          engine.syncState.totalTables = batch.totalTables
        }
        if (batch.schema) {
          for (const ddl of batch.schema) {
            if (!isSyncSafeDdl(ddl)) {
              throw new SyncError(`Unsafe DDL in schema batch: ${ddl}`)
            }
          }
          await engine.writerConn.exec('PRAGMA foreign_keys = OFF')
          for (const ddl of batch.schema) {
            let saferDdl = ddl.replace(/^\s*CREATE\s+TABLE\b/i, 'CREATE TABLE IF NOT EXISTS')
            saferDdl = saferDdl.replace(/^\s*CREATE\s+INDEX\b/i, 'CREATE INDEX IF NOT EXISTS')
            await engine.writerConn.exec(saferDdl)
          }
        }

        await this.sendSyncAck(fromPeerId, {
          requestId: batch.requestId,
          joinerNodeId: engine.nodeId,
          table: batch.table,
          batchIndex: batch.batchIndex,
          success: true,
        })
        return
      }

      if (!IDENTIFIER_RE.test(batch.table)) {
        throw new SyncError(`Invalid table name in sync batch: ${batch.table}`)
      }

      const expectedChecksum = createHash('sha256').update(canonicaliseForChecksum(batch.rows)).digest('hex')
      if (batch.checksum !== expectedChecksum) {
        throw new SyncError(`Checksum mismatch for ${batch.table} batch ${batch.batchIndex}`)
      }

      if (batch.batchIndex === 0 && !engine.syncState.completedTables.includes(batch.table)) {
        engine.syncTableDigests.delete(batch.table)
        await engine.tracker.unwatch(engine.writerConn, batch.table)
        await engine.writerConn.exec(`DELETE FROM "${batch.table}"`)
      }

      if (batch.rows.length > 0) {
        const columns = Object.keys(batch.rows[0]).filter(c => IDENTIFIER_RE.test(c))
        if (columns.length > 0) {
          const placeholders = columns.map(() => '?').join(', ')
          const colNames = columns.map(c => `"${c}"`).join(', ')
          const insertSql = `INSERT INTO "${batch.table}" (${colNames}) VALUES (${placeholders})`

          await engine.writerConn.transaction(async tx => {
            const stmt = await tx.prepare(insertSql)
            for (const row of batch.rows) {
              const values = columns.map(c => row[c])
              await stmt.run(...values)
            }
          })
        }
      }

      engine.syncTableDigests.set(
        batch.table,
        advanceStreamDigest(engine.syncTableDigests.get(batch.table), batch.checksum, batch.rows.length),
      )

      if (batch.isLastBatchForTable) {
        await engine.log.setSyncTableStatus(batch.table, 'completed')
        engine.syncState.completedTables.push(batch.table)
        engine.expectedBatchIndex.delete(batch.table)
      }

      await this.sendSyncAck(fromPeerId, {
        requestId: batch.requestId,
        joinerNodeId: engine.nodeId,
        table: batch.table,
        batchIndex: batch.batchIndex,
        success: true,
      })
    } catch (err) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'sync-batch-processing', peerId: fromPeerId, recoverable: true })
      await this.sendSyncAck(fromPeerId, {
        requestId: batch.requestId,
        joinerNodeId: engine.nodeId,
        table: batch.table,
        batchIndex: batch.batchIndex,
        success: false,
        error: wrappedErr.message,
      }).catch((ackErr: unknown) => {
        const ackWrapped = ackErr instanceof Error ? ackErr : new Error(String(ackErr))
        engine.emitError({ error: ackWrapped, operation: 'sync-ack-send', peerId: fromPeerId, recoverable: true })
      })
    }
  }

  async handleSyncCompleteReceived(complete: SyncComplete, fromPeerId: string): Promise<void> {
    const engine = this.engine
    if (engine.syncState.phase !== 'syncing') return
    if (fromPeerId !== engine.syncState.sourcePeerId) return

    if (!engine.tracker) {
      throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
    }

    try {
      for (const manifest of complete.manifests) {
        if (IDENTIFIER_RE.test(manifest.table)) {
          await engine.tracker.watch(engine.writerConn, manifest.table)
        }
      }

      await engine.writerConn.exec('PRAGMA foreign_keys = ON')

      for (const manifest of complete.manifests) {
        const valid =
          manifest.batchDigest !== undefined
            ? matchesStreamDigest(manifest, engine.syncTableDigests.get(manifest.table))
            : await engine.log.verifyManifest(manifest)
        if (!valid) {
          await engine.log.wipeTables(
            engine.writerConn,
            complete.manifests.map(m => m.table).filter(t => IDENTIFIER_RE.test(t)),
            engine.tracker,
          )
          engine.syncState.phase = 'pending'
          engine.syncState.completedTables = []
          engine.expectedBatchIndex.clear()
          engine.syncTableDigests.clear()
          await engine.log.setSyncMeta('pending')
          await this.initiateSync()
          return
        }
      }
      engine.syncTableDigests.clear()

      await engine.log.setLastAppliedSeq(fromPeerId, complete.snapshotSeq)

      const recordStmt = await engine.writerConn.prepare(
        `INSERT OR IGNORE INTO ${APPLIED_CHANGES_TABLE} (source_node_id, source_seq, applied_at) VALUES (?, ?, ?)`,
      )
      await recordStmt.run(fromPeerId, complete.snapshotSeq.toString(), Date.now() / 1000)

      const previousApplied = engine.appliedSeqByPeer.get(fromPeerId) ?? 0n
      if (complete.snapshotSeq > previousApplied) {
        engine.appliedSeqByPeer.set(fromPeerId, complete.snapshotSeq)
      }

      engine.syncState.phase = 'catching-up'
      engine.syncState.snapshotSeq = complete.snapshotSeq
      await engine.log.setSyncMeta('catching-up', complete.snapshotSeq)

      engine.startSenderLoop()
      this.startCatchUpCheck()
    } catch {
      try {
        await engine.writerConn.exec('PRAGMA foreign_keys = ON')
      } catch (pragmaErr: unknown) {
        const wrappedErr = pragmaErr instanceof Error ? pragmaErr : new Error(String(pragmaErr))
        engine.emitError({ error: wrappedErr, operation: 'sync-complete-pragma-restore', recoverable: false })
      }
      engine.emitError({
        error: new SyncError('Failed during sync completion handling'),
        operation: 'sync-complete-handling',
        recoverable: false,
      })
      return
    }
  }

  startCatchUpCheck(): void {
    const engine = this.engine
    const catchUpStartedAt = Date.now()
    this.catchUpCheckTimer = setInterval(async () => {
      if (engine.syncState.phase !== 'catching-up') {
        this.stopCatchUpCheck()
        return
      }

      if (Date.now() - catchUpStartedAt > engine.catchUpDeadlineMs) {
        await this.finishCatchUpAsReady()
        return
      }

      const sourcePeerId = engine.syncState.sourcePeerId
      if (!sourcePeerId) return

      if (engine.highestSourceSeqSeen === 0n) {
        const snapshotSeq = engine.syncState.snapshotSeq ?? 0n
        if (snapshotSeq === 0n) {
          await this.finishCatchUpAsReady()
          return
        }
        const localSeq = await engine.log.getLocalSeq()
        if (localSeq === 0n) {
          await this.finishCatchUpAsReady()
          return
        }
        return
      }

      const appliedSeq = await engine.log.getLastAppliedSeq(sourcePeerId)
      const lag = engine.highestSourceSeqSeen - appliedSeq
      if (lag <= BigInt(engine.maxSyncLagBeforeReady)) {
        await this.finishCatchUpAsReady()
      }
    }, engine.batchIntervalMs * 2)
  }

  stopCatchUpCheck(): void {
    if (this.catchUpCheckTimer) {
      clearInterval(this.catchUpCheckTimer)
      this.catchUpCheckTimer = null
    }
  }

  private async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    await delayAckIfConfigured(this.engine)
    await this.engine.config.transport.sendSyncAck(peerId, this.engine.decorateSyncAck(ack))
  }

  private async finishCatchUpAsReady(): Promise<void> {
    const engine = this.engine
    await engine.log.setSyncMeta('ready')
    await engine.markCoordinatorSyncReady()
    engine.syncState.phase = 'ready'
    this.stopCatchUpCheck()
  }
}
