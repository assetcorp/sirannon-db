import { randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import type { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions } from '../../core/types.js'
import { LWWResolver } from '../conflict/lww.js'
import { SyncError, TopologyError } from '../errors.js'
import { HLC } from '../hlc.js'
import { ReplicationLog } from '../log.js'
import { generateNodeId } from '../node-id.js'
import { PeerTracker } from '../peer-tracker.js'
import type {
  ConflictResolver,
  ForwardedTransactionResult,
  ReplicationConfig,
  ReplicationErrorEvent,
  ReplicationStatus,
  SyncState,
} from '../types.js'
import {
  DEFAULT_ACK_TIMEOUT_MS,
  DEFAULT_BATCH_INTERVAL_MS,
  DEFAULT_BATCH_SIZE,
  DEFAULT_CATCH_UP_DEADLINE_MS,
  DEFAULT_MAX_BATCH_CHANGES,
  DEFAULT_MAX_CLOCK_DRIFT_MS,
  DEFAULT_MAX_CONCURRENT_SYNCS,
  DEFAULT_MAX_PENDING_BATCHES,
  DEFAULT_MAX_SYNC_DURATION_MS,
  DEFAULT_MAX_SYNC_LAG_BEFORE_READY,
  DEFAULT_SYNC_ACK_TIMEOUT_MS,
  DEFAULT_SYNC_BATCH_SIZE,
} from './constants.js'
import { LocalExecutor } from './local-executor.js'
import { SenderLoop } from './sender-loop.js'
import { startEngine } from './startup.js'
import { SyncJoiner } from './sync-joiner.js'
import { SyncServer } from './sync-server.js'
import { installTestHooks } from './test-hooks.js'

/**
 * Coordinates replication for a single database node.
 *
 * State and dependencies are exposed as readable properties so that the
 * collaborating modules in `./engine/` (LocalExecutor, SyncServer, SyncJoiner,
 * SenderLoop, transport wiring) can operate against a shared, mutable engine
 * instance without duplicating constructor wiring.
 */
export class ReplicationEngine extends EventEmitter {
  readonly database: Database
  readonly writerConn: SQLiteConnection
  readonly config: ReplicationConfig
  readonly nodeId: string
  readonly hlc: HLC
  readonly log: ReplicationLog
  readonly peerTracker = new PeerTracker()
  readonly defaultResolver: ConflictResolver
  readonly tracker: ChangeTracker | undefined
  readonly snapshotConnectionFactory: (() => Promise<SQLiteConnection>) | undefined

  readonly batchSize: number
  readonly batchIntervalMs: number
  readonly maxClockDriftMs: number
  readonly maxPendingBatches: number
  readonly maxBatchChanges: number
  readonly ackTimeoutMs: number

  readonly initialSync: boolean
  readonly syncBatchSize: number
  readonly maxConcurrentSyncs: number
  readonly maxSyncDurationMs: number
  readonly maxSyncLagBeforeReady: number
  readonly syncAckTimeoutMs: number
  readonly catchUpDeadlineMs: number
  readonly resumeFromSeq: bigint | undefined

  running = false
  lastSentSeq = 0n
  lastLocalSeq = 0n
  highestSourceSeqSeen = 0n
  readonly appliedSeqByPeer = new Map<string, bigint>()
  readonly expectedBatchIndex = new Map<string, number>()
  syncState: SyncState = {
    phase: 'ready',
    sourcePeerId: null,
    snapshotSeq: null,
    completedTables: [],
    totalTables: 0,
    startedAt: null,
    error: null,
  }

  readonly localExecutor: LocalExecutor
  readonly syncServer: SyncServer
  readonly syncJoiner: SyncJoiner
  readonly senderLoop: SenderLoop

  constructor(database: Database, writerConn: SQLiteConnection, config: ReplicationConfig) {
    super()
    this.database = database
    this.writerConn = writerConn
    this.config = config
    this.nodeId = config.nodeId ?? generateNodeId()
    this.hlc = new HLC(this.nodeId)
    this.tracker = config.changeTracker
    this.log = new ReplicationLog(writerConn, this.nodeId, this.hlc, '_sirannon_changes', this.tracker)
    this.defaultResolver = config.defaultConflictResolver ?? new LWWResolver()
    this.batchSize = config.batchSize ?? DEFAULT_BATCH_SIZE
    this.batchIntervalMs = config.batchIntervalMs ?? DEFAULT_BATCH_INTERVAL_MS
    this.maxClockDriftMs = config.maxClockDriftMs ?? DEFAULT_MAX_CLOCK_DRIFT_MS
    this.maxPendingBatches = config.maxPendingBatches ?? DEFAULT_MAX_PENDING_BATCHES
    this.maxBatchChanges = config.maxBatchChanges ?? DEFAULT_MAX_BATCH_CHANGES
    this.ackTimeoutMs = config.ackTimeoutMs ?? DEFAULT_ACK_TIMEOUT_MS
    this.initialSync = config.initialSync ?? true
    this.syncBatchSize = config.syncBatchSize ?? DEFAULT_SYNC_BATCH_SIZE
    this.maxConcurrentSyncs = config.maxConcurrentSyncs ?? DEFAULT_MAX_CONCURRENT_SYNCS
    this.maxSyncDurationMs = config.maxSyncDurationMs ?? DEFAULT_MAX_SYNC_DURATION_MS
    this.maxSyncLagBeforeReady = config.maxSyncLagBeforeReady ?? DEFAULT_MAX_SYNC_LAG_BEFORE_READY
    this.syncAckTimeoutMs = config.syncAckTimeoutMs ?? DEFAULT_SYNC_ACK_TIMEOUT_MS
    this.catchUpDeadlineMs = config.catchUpDeadlineMs ?? DEFAULT_CATCH_UP_DEADLINE_MS
    this.resumeFromSeq = config.resumeFromSeq
    this.snapshotConnectionFactory = config.snapshotConnectionFactory

    this.localExecutor = new LocalExecutor(this)
    this.syncServer = new SyncServer(this)
    this.syncJoiner = new SyncJoiner(this)
    this.senderLoop = new SenderLoop(this)
    installTestHooks(this)
  }

  start(): Promise<void> {
    return startEngine(this)
  }

  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    this.syncJoiner.stopCatchUpCheck()
    this.syncServer.abortAll()

    if (this.syncState.phase === 'syncing') {
      try {
        await this.writerConn.exec('PRAGMA foreign_keys = ON')
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.emitError({ error: wrappedErr, operation: 'engine-stop-pragma-restore', recoverable: false })
      }
    }

    this.senderLoop.stop()
    if (this.tracker) {
      this.tracker.clearPruneBoundary()
    }
    await this.config.transport.disconnect()
  }

  status(): ReplicationStatus {
    return {
      nodeId: this.nodeId,
      role: this.config.topology.role,
      peers: this.peerTracker.allPeerStates(),
      localSeq: this.lastSentSeq,
      replicating: this.running,
      syncState: { ...this.syncState },
    }
  }

  getCurrentSeq(): bigint {
    return this.lastLocalSeq
  }

  getAppliedSeq(peerId: string): bigint {
    return this.appliedSeqByPeer.get(peerId) ?? 0n
  }

  async query<T>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    if (this.syncState.phase !== 'ready') {
      throw new SyncError(`Node is in '${this.syncState.phase}' phase and cannot serve reads`)
    }
    return this.database.query<T>(sql, params, options)
  }

  async execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    if (this.syncState.phase !== 'ready') {
      throw new SyncError(`Node is in '${this.syncState.phase}' phase and cannot accept operations`)
    }

    if (!this.config.topology.canWrite()) {
      if (this.config.writeForwarding) {
        const result = await this.forwardStatements([{ sql, params }], options)
        const first = result.results[0]
        if (!first) {
          return { changes: 0, lastInsertRowId: 0 }
        }
        return {
          changes: first.changes,
          lastInsertRowId:
            typeof first.lastInsertRowId === 'string' ? BigInt(first.lastInsertRowId) : first.lastInsertRowId,
        }
      }
      throw new TopologyError('This node cannot accept writes')
    }

    return this.localExecutor.executeLocally(sql, params, options)
  }

  async executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    if (this.syncState.phase !== 'ready') {
      throw new SyncError(`Node is in '${this.syncState.phase}' phase and cannot accept operations`)
    }

    if (!this.config.topology.canWrite()) {
      if (this.config.writeForwarding) {
        const statements = paramsBatch.map(p => ({ sql, params: p }))
        const result = await this.forwardStatements(statements, options)
        return result.results.map(r => ({
          changes: r.changes,
          lastInsertRowId:
            typeof r.lastInsertRowId === 'string' ? BigInt(r.lastInsertRowId) : Number(r.lastInsertRowId),
        }))
      }
      throw new TopologyError('This node cannot accept writes')
    }

    const results: ExecuteResult[] = []
    for (const params of paramsBatch) {
      const r = await this.localExecutor.executeLocally(sql, params, options)
      results.push(r)
    }
    return results
  }

  async transaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T> {
    if (this.syncState.phase !== 'ready') {
      throw new SyncError(`Node is in '${this.syncState.phase}' phase and cannot accept operations`)
    }
    if (!this.config.topology.canWrite()) {
      throw new TopologyError('This node cannot accept writes in transaction mode')
    }
    return this.localExecutor.executeTransactionLocally(fn, options)
  }

  async forwardStatements(
    statements: Array<{ sql: string; params?: Params }>,
    _options?: QueryOptions,
  ): Promise<ForwardedTransactionResult> {
    if (this.config.topology.canWrite()) {
      return this.localExecutor.executeForwardedLocally(statements)
    }

    const peers = this.config.transport.peers()
    let primaryPeerId: string | null = null

    for (const [peerId, info] of peers) {
      if (info.role === 'primary') {
        primaryPeerId = peerId
        break
      }
    }

    if (primaryPeerId === null) {
      throw new TopologyError('No primary node available for write forwarding')
    }

    return this.config.transport.forward(primaryPeerId, {
      statements,
      requestId: randomUUID(),
    })
  }

  startSenderLoop(): void {
    this.senderLoop.start()
  }

  emitError(event: ReplicationErrorEvent): void {
    if (this.listenerCount('replication-error') > 0) {
      try {
        this.emit('replication-error', event)
      } catch {
        /* Listener failures must not disrupt engine operation */
      }
    }
  }

  getResolver(table?: string): ConflictResolver {
    if (table && this.config.conflictResolvers) {
      const specific = this.config.conflictResolvers[table]
      if (specific) return specific
    }
    return this.defaultResolver
  }

  checkClockDrift(remoteHlc: string): number {
    const decoded = HLC.decode(remoteHlc)
    return Math.abs(Date.now() - decoded.wallMs)
  }

  async refreshTriggersAfterDdl(): Promise<void> {
    if (!this.tracker) return
    const tables = Array.from(this.tracker.watchedTables)
    for (const table of tables) {
      try {
        await this.tracker.watch(this.writerConn, table)
      } catch {
        /* Table may have been dropped by the DDL; the tracker entry will be cleaned up on next unwatch */
      }
    }
  }

  async waitForWriteConcern(seq: bigint, wc: { level: string; timeoutMs?: number }): Promise<void> {
    const timeout = wc.timeoutMs ?? 5000

    if (wc.level === 'majority') {
      await this.peerTracker.waitForMajority(seq, timeout)
    } else if (wc.level === 'all') {
      await this.peerTracker.waitForAll(seq, timeout)
    }
  }

  async loadAppliedSeqs(): Promise<void> {
    const stmt = await this.writerConn.prepare(
      'SELECT source_node_id, MAX(source_seq) AS max_seq FROM _sirannon_applied_changes GROUP BY source_node_id',
    )
    const rows = (await stmt.all()) as Array<{ source_node_id: string; max_seq: number | string | null }>
    for (const row of rows) {
      if (row.max_seq === null) continue
      this.appliedSeqByPeer.set(row.source_node_id, BigInt(row.max_seq))
    }
  }
}
