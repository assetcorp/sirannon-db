import { createHash, randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import type { ChangeTracker } from '../core/cdc/change-tracker.js'
import type { Database } from '../core/database.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import type { Transaction } from '../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions } from '../core/types.js'
import { LWWResolver } from './conflict/lww.js'
import { BatchValidationError, ReplicationError, SyncError, TopologyError } from './errors.js'
import { HLC } from './hlc.js'
import { ReplicationLog } from './log.js'
import { generateNodeId } from './node-id.js'
import { PeerTracker } from './peer-tracker.js'
import type {
  ConflictResolver,
  ForwardedTransactionResult,
  ReplicationConfig,
  ReplicationErrorEvent,
  ReplicationStatus,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
  SyncState,
  SyncTableManifest,
} from './types.js'

const DEFAULT_BATCH_SIZE = 100
const DEFAULT_BATCH_INTERVAL_MS = 100
const DEFAULT_MAX_CLOCK_DRIFT_MS = 60_000
const DEFAULT_MAX_PENDING_BATCHES = 10
const DEFAULT_MAX_BATCH_CHANGES = 1000

const DDL_PREFIX_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

const SAFE_SQL_PREFIX_RE =
  /^\s*(INSERT|UPDATE|DELETE|SELECT|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

const DDL_DENY_RE = /\b(load_extension|ATTACH|randomblob|zeroblob|writefile|readfile|fts3_tokenizer)\b/i

interface ActiveSyncSession {
  requestId: string
  joinerNodeId: string
  readConn: SQLiteConnection
  snapshotSeq: bigint
  tables: string[]
  completedTables: Set<string>
  startedAt: number
  timeoutTimer: ReturnType<typeof setTimeout>
  aborted: boolean
}

export class ReplicationEngine extends EventEmitter {
  private readonly database: Database
  private readonly writerConn: SQLiteConnection
  private readonly config: ReplicationConfig
  private readonly nodeId: string
  private readonly hlc: HLC
  private readonly log: ReplicationLog
  private readonly peerTracker = new PeerTracker()
  private readonly defaultResolver: ConflictResolver

  private senderTimer: ReturnType<typeof setTimeout> | null = null
  private lastSentSeq = 0n
  private running = false
  private readonly batchSize: number
  private readonly batchIntervalMs: number
  private readonly maxClockDriftMs: number
  private readonly maxPendingBatches: number
  private readonly maxBatchChanges: number
  private readonly ackTimeoutMs: number

  private readonly initialSync: boolean
  private readonly syncBatchSize: number
  private readonly maxConcurrentSyncs: number
  private readonly maxSyncDurationMs: number
  private readonly maxSyncLagBeforeReady: number
  private readonly syncAckTimeoutMs: number
  private readonly catchUpDeadlineMs: number
  private readonly resumeFromSeq: bigint | undefined
  private readonly snapshotConnectionFactory: (() => Promise<SQLiteConnection>) | undefined
  private readonly tracker: ChangeTracker | undefined

  private syncState: SyncState = {
    phase: 'ready',
    sourcePeerId: null,
    snapshotSeq: null,
    completedTables: [],
    totalTables: 0,
    startedAt: null,
    error: null,
  }

  private readonly activeSyncs = new Map<string, ActiveSyncSession>()
  private readonly syncAckWaiters = new Map<string, { resolve: (ack: SyncAck) => void }>()
  private highestSourceSeqSeen = 0n
  private catchUpCheckTimer: ReturnType<typeof setInterval> | null = null
  private readonly expectedBatchIndex = new Map<string, number>()

  constructor(database: Database, writerConn: SQLiteConnection, config: ReplicationConfig) {
    super()
    this.database = database
    this.writerConn = writerConn
    this.config = config
    this.nodeId = config.nodeId ?? generateNodeId()
    this.hlc = new HLC(this.nodeId)
    this.log = new ReplicationLog(writerConn, this.nodeId, this.hlc)
    this.defaultResolver = config.defaultConflictResolver ?? new LWWResolver()
    this.batchSize = config.batchSize ?? DEFAULT_BATCH_SIZE
    this.batchIntervalMs = config.batchIntervalMs ?? DEFAULT_BATCH_INTERVAL_MS
    this.maxClockDriftMs = config.maxClockDriftMs ?? DEFAULT_MAX_CLOCK_DRIFT_MS
    this.maxPendingBatches = config.maxPendingBatches ?? DEFAULT_MAX_PENDING_BATCHES
    this.maxBatchChanges = config.maxBatchChanges ?? DEFAULT_MAX_BATCH_CHANGES
    this.ackTimeoutMs = config.ackTimeoutMs ?? 5000
    this.initialSync = config.initialSync ?? true
    this.syncBatchSize = config.syncBatchSize ?? 10_000
    this.maxConcurrentSyncs = config.maxConcurrentSyncs ?? 2
    this.maxSyncDurationMs = config.maxSyncDurationMs ?? 1_800_000
    this.maxSyncLagBeforeReady = config.maxSyncLagBeforeReady ?? 100
    this.syncAckTimeoutMs = config.syncAckTimeoutMs ?? 30_000
    this.catchUpDeadlineMs = config.catchUpDeadlineMs ?? 600_000
    this.resumeFromSeq = config.resumeFromSeq
    this.snapshotConnectionFactory = config.snapshotConnectionFactory
    this.tracker = config.changeTracker
  }

  private emitError(event: ReplicationErrorEvent): void {
    if (this.listenerCount('replication-error') > 0) {
      try {
        this.emit('replication-error', event)
      } catch {
        /* Listener failures must not disrupt engine operation */
      }
    }
  }

  async start(): Promise<void> {
    if (this.running) return
    this.running = true

    await this.log.ensureReplicationTables()
    this.lastSentSeq = await this.log.getLocalSeq()

    this.setupTransportHandlers()
    const transportConfig = {
      ...this.config.transportConfig,
      localRole: this.config.topology.role,
    }
    await this.config.transport.connect(this.nodeId, transportConfig)

    const isPrimary = this.config.topology.role === 'primary'
    const syncCompleted = await this.log.isSyncCompleted()

    if (this.initialSync && !isPrimary && !syncCompleted) {
      const savedState = await this.log.getSyncState()
      if (savedState.phase === 'syncing') {
        if (!this.tracker) {
          throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
        }
        await this.log.wipeTables(this.writerConn, await this.log.getTablesInFkOrder(this.writerConn), this.tracker)
      }
      this.syncState = {
        phase: 'pending',
        sourcePeerId: null,
        snapshotSeq: null,
        completedTables: [],
        totalTables: 0,
        startedAt: null,
        error: null,
      }
      await this.log.setSyncMeta('pending')
      await this.initiateSync()
      return
    }

    if (this.initialSync && !isPrimary && syncCompleted) {
      const savedState = await this.log.getSyncState()
      if (savedState.phase === 'catching-up') {
        this.syncState = {
          phase: 'catching-up',
          sourcePeerId: savedState.sourcePeerId,
          snapshotSeq: savedState.snapshotSeq,
          completedTables: [],
          totalTables: 0,
          startedAt: null,
          error: null,
        }
        this.startSenderLoop()
        this.startCatchUpCheck()
        return
      }
      this.syncState.phase = 'ready'
    }

    if (!this.initialSync && this.resumeFromSeq !== undefined) {
      this.lastSentSeq = this.resumeFromSeq
      await this.log.setSyncMeta('ready', this.resumeFromSeq)
      this.syncState.phase = 'ready'
    } else if (!this.initialSync && !syncCompleted) {
      const localSeq = await this.log.getLocalSeq()
      if (localSeq > 0n) {
        await this.log.setSyncMeta('ready', localSeq)
      }
      this.syncState.phase = 'ready'
    } else {
      this.syncState.phase = 'ready'
    }

    this.startSenderLoop()
  }

  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    if (this.catchUpCheckTimer) {
      clearInterval(this.catchUpCheckTimer)
      this.catchUpCheckTimer = null
    }

    for (const [requestId] of this.activeSyncs) {
      this.abortSyncSession(requestId)
    }

    if (this.syncState.phase === 'syncing') {
      try {
        await this.writerConn.exec('PRAGMA foreign_keys = ON')
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.emitError({ error: wrappedErr, operation: 'engine-stop-pragma-restore', recoverable: false })
      }
    }

    this.clearSenderTimer()
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

    return this.executeLocally(sql, params, options)
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
      const r = await this.executeLocally(sql, params, options)
      results.push(r)
    }
    return results
  }

  async transaction<T>(fn: (tx: Transaction) => Promise<T>, _options?: QueryOptions): Promise<T> {
    if (this.syncState.phase !== 'ready') {
      throw new SyncError(`Node is in '${this.syncState.phase}' phase and cannot accept operations`)
    }
    if (!this.config.topology.canWrite()) {
      throw new TopologyError('This node cannot accept writes in transaction mode')
    }
    return this.database.transaction(fn)
  }

  async forwardStatements(
    statements: Array<{ sql: string; params?: Params }>,
    _options?: QueryOptions,
  ): Promise<ForwardedTransactionResult> {
    if (this.config.topology.canWrite()) {
      return this.executeForwardedLocally(statements)
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

  private async executeLocally(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    const isDdl = DDL_PREFIX_RE.test(sql)
    if (isDdl && sql.includes(';')) {
      throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
    }
    const txId = randomUUID()

    const result = await this.writerConn.transaction(async tx => {
      const seqBefore = await this.log.getLocalSeq()

      const bindValues = params ? (Array.isArray(params) ? params : [params]) : []
      const stmt = await tx.prepare(sql)
      const r = await stmt.run(...bindValues)

      if (isDdl) {
        const ddlStmt = await tx.prepare(
          `INSERT INTO "_sirannon_changes" (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
					 VALUES ('__ddl__', 'DDL', '', ?, ?, ?, ?)`,
        )
        const hlcVal = this.hlc.now()
        await ddlStmt.run(JSON.stringify({ ddlStatement: sql }), this.nodeId, txId, hlcVal)
      } else {
        await this.log.stampChanges(tx, seqBefore, txId)
        await this.log.updateColumnVersions(tx, seqBefore)
      }

      return { changes: r.changes, lastInsertRowId: r.lastInsertRowId }
    })

    if (options?.writeConcern) {
      const newSeq = await this.log.getLocalSeq()
      await this.waitForWriteConcern(newSeq, options.writeConcern)
    }

    return result
  }

  private async executeForwardedLocally(
    statements: Array<{ sql: string; params?: Params }>,
  ): Promise<ForwardedTransactionResult> {
    const requestId = randomUUID()
    const results: Array<{ changes: number; lastInsertRowId: number | string }> = []
    const txId = randomUUID()
    const hook = this.config.onBeforeForwardedQuery

    for (const { sql } of statements) {
      if (!SAFE_SQL_PREFIX_RE.test(sql)) {
        throw new ReplicationError(`Forwarded statement rejected: only DML and safe DDL are allowed`)
      }
    }

    if (hook) {
      for (const { sql, params } of statements) {
        hook(sql, params)
      }
    }

    await this.writerConn.transaction(async tx => {
      const seqBefore = await this.log.getLocalSeq()

      for (const { sql, params } of statements) {
        const isDdl = DDL_PREFIX_RE.test(sql)
        if (isDdl && sql.includes(';')) {
          throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
        }

        const bindValues = params ? (Array.isArray(params) ? params : [params]) : []
        const stmt = await tx.prepare(sql)
        const r = await stmt.run(...bindValues)
        results.push({
          changes: r.changes,
          lastInsertRowId: typeof r.lastInsertRowId === 'bigint' ? r.lastInsertRowId.toString() : r.lastInsertRowId,
        })

        if (isDdl) {
          const ddlStmt = await tx.prepare(
            `INSERT INTO "_sirannon_changes" (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
             VALUES ('__ddl__', 'DDL', '', ?, ?, ?, ?)`,
          )
          const hlcVal = this.hlc.now()
          await ddlStmt.run(JSON.stringify({ ddlStatement: sql }), this.nodeId, txId, hlcVal)
        }
      }

      await this.log.stampChanges(tx, seqBefore, txId)
      await this.log.updateColumnVersions(tx, seqBefore)
    })

    return { results, requestId }
  }

  private setupTransportHandlers(): void {
    this.config.transport.onBatchReceived(async (batch, fromPeerId) => {
      if (!this.running) return
      if (this.syncState.phase !== 'ready' && this.syncState.phase !== 'catching-up') return

      try {
        if (batch.sourceNodeId !== fromPeerId) {
          throw new BatchValidationError(
            `Batch sourceNodeId '${batch.sourceNodeId}' does not match sender '${fromPeerId}'`,
          )
        }

        const knownPeers = this.config.transport.peers()
        if (!knownPeers.has(fromPeerId)) {
          throw new BatchValidationError(`Batch from unknown peer: ${fromPeerId}`)
        }

        const peerInfo = knownPeers.get(fromPeerId)
        if (!peerInfo || !this.config.topology.shouldAcceptFrom(fromPeerId, peerInfo.role)) {
          throw new ReplicationError(`Rejected batch from unauthorized peer: ${fromPeerId}`)
        }

        if (batch.changes.length > this.maxBatchChanges) {
          throw new BatchValidationError(
            `Batch too large: ${batch.changes.length} changes exceeds max ${this.maxBatchChanges}`,
          )
        }

        const drift = this.checkClockDrift(batch.hlcRange.max)
        if (drift > this.maxClockDriftMs) {
          throw new BatchValidationError(`Clock drift too high: ${drift}ms exceeds max ${this.maxClockDriftMs}ms`)
        }

        await this.log.applyBatch(batch, table => this.getResolver(table))

        await this.log.setLastAppliedSeq(fromPeerId, batch.toSeq)

        if (batch.toSeq > this.highestSourceSeqSeen) {
          this.highestSourceSeqSeen = batch.toSeq
        }

        await this.config.transport.sendAck(fromPeerId, {
          batchId: batch.batchId,
          ackedSeq: batch.toSeq,
          nodeId: this.nodeId,
        })
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.emitError({ error: wrappedErr, operation: 'batch-received', peerId: fromPeerId, recoverable: true })
      }
    })

    this.config.transport.onAckReceived((ack, _fromPeerId) => {
      if (!this.running) return
      if (this.syncState.phase !== 'ready' && this.syncState.phase !== 'catching-up') return
      this.peerTracker.onAckReceived(ack.nodeId, ack.ackedSeq)
    })

    if (this.config.topology.canWrite()) {
      this.config.transport.onForwardReceived(async (request, fromPeerId) => {
        if (this.syncState.phase !== 'ready' && this.syncState.phase !== 'catching-up') {
          throw new ReplicationError('Node is not ready to handle forwarded requests')
        }
        const knownPeers = this.config.transport.peers()
        if (!knownPeers.has(fromPeerId)) {
          throw new ReplicationError(`Rejected forward from unknown peer: ${fromPeerId}`)
        }
        try {
          return await this.executeForwardedLocally(request.statements)
        } catch (err: unknown) {
          const wrappedErr = err instanceof Error ? err : new Error(String(err))
          this.emitError({ error: wrappedErr, operation: 'forward-execution', peerId: fromPeerId, recoverable: true })
          throw wrappedErr
        }
      })
    }

    this.config.transport.onPeerConnected(peer => {
      this.peerTracker.addPeer(peer.id)
      if (this.syncState.phase === 'pending') {
        this.initiateSync().catch((err: unknown) => {
          const wrappedErr = err instanceof Error ? err : new Error(String(err))
          this.emitError({ error: wrappedErr, operation: 'sync-initiation', peerId: peer.id, recoverable: true })
        })
      }
    })

    this.config.transport.onPeerDisconnected(peerId => {
      this.peerTracker.removePeer(peerId)

      for (const [requestId, session] of this.activeSyncs) {
        if (session.joinerNodeId === peerId) {
          this.abortSyncSession(requestId)
        }
      }

      if (this.syncState.phase === 'syncing' && this.syncState.sourcePeerId === peerId) {
        this.syncState.phase = 'pending'
        this.syncState.sourcePeerId = null
        this.writerConn.exec('PRAGMA foreign_keys = ON').catch((err: unknown) => {
          const wrappedErr = err instanceof Error ? err : new Error(String(err))
          this.emitError({ error: wrappedErr, operation: 'peer-disconnect-pragma-restore', peerId, recoverable: false })
        })
        this.expectedBatchIndex.clear()
        this.log.setSyncMeta('pending').catch((err: unknown) => {
          const wrappedErr = err instanceof Error ? err : new Error(String(err))
          this.emitError({ error: wrappedErr, operation: 'sync-meta-write', peerId, recoverable: false })
        })
      }
    })

    this.config.transport.onSyncRequested(async (request, fromPeerId) => {
      if (!this.running) return
      await this.handleSyncRequest(request, fromPeerId)
    })

    this.config.transport.onSyncBatchReceived(async (batch, fromPeerId) => {
      if (!this.running) return
      await this.handleSyncBatchReceived(batch, fromPeerId)
    })

    this.config.transport.onSyncCompleteReceived(async (complete, fromPeerId) => {
      if (!this.running) return
      await this.handleSyncCompleteReceived(complete, fromPeerId)
    })

    this.config.transport.onSyncAckReceived((ack, _fromPeerId) => {
      if (!this.running) return
      this.handleSyncAckReceived(ack)
    })
  }

  private async handleSyncRequest(request: SyncRequest, fromPeerId: string): Promise<void> {
    const knownPeers = this.config.transport.peers()
    if (!knownPeers.has(fromPeerId)) return

    if (!this.config.topology.canWrite()) {
      await this.config.transport.sendSyncAck(fromPeerId, {
        requestId: request.requestId,
        joinerNodeId: request.joinerNodeId,
        table: '__schema__',
        batchIndex: -1,
        success: false,
        error: 'This node cannot serve syncs',
      })
      return
    }

    if (this.activeSyncs.has(request.requestId)) {
      await this.config.transport.sendSyncAck(fromPeerId, {
        requestId: request.requestId,
        joinerNodeId: request.joinerNodeId,
        table: '__schema__',
        batchIndex: -1,
        success: false,
        error: 'Duplicate requestId',
      })
      return
    }

    if (this.activeSyncs.size >= this.maxConcurrentSyncs) {
      await this.config.transport.sendSyncAck(fromPeerId, {
        requestId: request.requestId,
        joinerNodeId: request.joinerNodeId,
        table: '__schema__',
        batchIndex: -1,
        success: false,
        error: 'Sync capacity reached, retry later',
      })
      return
    }

    if (!this.snapshotConnectionFactory) {
      await this.config.transport.sendSyncAck(fromPeerId, {
        requestId: request.requestId,
        joinerNodeId: request.joinerNodeId,
        table: '__schema__',
        batchIndex: -1,
        success: false,
        error: 'No snapshot connection factory configured',
      })
      return
    }

    for (const table of request.completedTables) {
      if (!IDENTIFIER_RE.test(table)) {
        await this.config.transport.sendSyncAck(fromPeerId, {
          requestId: request.requestId,
          joinerNodeId: request.joinerNodeId,
          table: '__schema__',
          batchIndex: -1,
          success: false,
          error: `Invalid table name in completedTables: ${table}`,
        })
        return
      }
    }

    let readConn: SQLiteConnection
    try {
      readConn = await this.snapshotConnectionFactory()
    } catch (err) {
      await this.config.transport.sendSyncAck(fromPeerId, {
        requestId: request.requestId,
        joinerNodeId: request.joinerNodeId,
        table: '__schema__',
        batchIndex: -1,
        success: false,
        error: `Failed to open snapshot connection: ${err instanceof Error ? err.message : String(err)}`,
      })
      return
    }

    const snapshotSeq = await this.log.getLocalSeq()

    await readConn.exec('BEGIN')
    const lockStmt = await readConn.prepare('SELECT 1 FROM sqlite_master LIMIT 1')
    await lockStmt.get()

    this.log.registerActiveSyncSeq(snapshotSeq)

    const allTables = await this.log.getTablesInFkOrder(readConn)
    const completedSet = new Set(request.completedTables)
    const tables = allTables.filter(t => !completedSet.has(t))

    const timeoutTimer = setTimeout(() => this.abortSyncSession(request.requestId), this.maxSyncDurationMs)
    timeoutTimer.unref()

    const session: ActiveSyncSession = {
      requestId: request.requestId,
      joinerNodeId: request.joinerNodeId,
      readConn,
      snapshotSeq,
      tables,
      completedTables: new Set(request.completedTables),
      startedAt: Date.now(),
      timeoutTimer,
      aborted: false,
    }

    this.activeSyncs.set(request.requestId, session)

    this.serveSyncSession(session).catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      this.emitError({ error: wrappedErr, operation: 'sync-session-serve', peerId: fromPeerId, recoverable: true })
      this.abortSyncSession(request.requestId)
    })
  }

  private async serveSyncSession(session: ActiveSyncSession): Promise<void> {
    const schemaDdl = await this.log.dumpSchema(session.readConn)
    const schemaChecksum = createHash('sha256').update(JSON.stringify(schemaDdl)).digest('hex')

    await this.config.transport.sendSyncBatch(session.joinerNodeId, {
      requestId: session.requestId,
      table: '__schema__',
      batchIndex: 0,
      rows: [],
      schema: schemaDdl,
      checksum: schemaChecksum,
      isLastBatchForTable: true,
    })

    const schemaAck = await this.waitForSyncAck(session.requestId, '__schema__', 0)
    if (!schemaAck.success) {
      this.abortSyncSession(session.requestId)
      return
    }

    for (const table of session.tables) {
      if (session.aborted) return

      let batchIndex = 0
      for await (const { rows, checksum, isLast } of this.log.dumpTableOnConnection(
        session.readConn,
        table,
        this.syncBatchSize,
      )) {
        if (session.aborted) return

        await this.config.transport.sendSyncBatch(session.joinerNodeId, {
          requestId: session.requestId,
          table,
          batchIndex,
          rows,
          checksum,
          isLastBatchForTable: isLast,
        })

        const ack = await this.waitForSyncAck(session.requestId, table, batchIndex)
        if (!ack.success) {
          this.abortSyncSession(session.requestId)
          return
        }

        batchIndex += 1
      }

      if (batchIndex === 0) {
        await this.config.transport.sendSyncBatch(session.joinerNodeId, {
          requestId: session.requestId,
          table,
          batchIndex: 0,
          rows: [],
          checksum: createHash('sha256').update(JSON.stringify([])).digest('hex'),
          isLastBatchForTable: true,
        })

        const emptyAck = await this.waitForSyncAck(session.requestId, table, 0)
        if (!emptyAck.success) {
          this.abortSyncSession(session.requestId)
          return
        }
      }

      session.completedTables.add(table)
    }

    if (session.aborted) return

    const manifests: SyncTableManifest[] = []
    for (const table of session.tables) {
      manifests.push(await this.log.generateManifest(session.readConn, table))
    }

    await this.config.transport.sendSyncComplete(session.joinerNodeId, {
      requestId: session.requestId,
      snapshotSeq: session.snapshotSeq,
      manifests,
    })

    try {
      await session.readConn.exec('COMMIT')
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      this.emitError({
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
      this.emitError({
        error: wrappedErr,
        operation: 'sync-session-close',
        peerId: session.joinerNodeId,
        recoverable: true,
      })
    }
    this.log.unregisterActiveSyncSeq(session.snapshotSeq)
    clearTimeout(session.timeoutTimer)
    this.activeSyncs.delete(session.requestId)
  }

  private waitForSyncAck(requestId: string, table: string, batchIndex: number): Promise<SyncAck> {
    return new Promise((resolve, reject) => {
      const key = `${requestId}:${table}:${batchIndex}`
      const timer = setTimeout(() => {
        this.syncAckWaiters.delete(key)
        reject(new SyncError(`Sync ack timeout for ${table} batch ${batchIndex}`, requestId))
      }, this.syncAckTimeoutMs)
      timer.unref()
      this.syncAckWaiters.set(key, {
        resolve: (ack: SyncAck) => {
          clearTimeout(timer)
          resolve(ack)
        },
      })
    })
  }

  private handleSyncAckReceived(ack: SyncAck): void {
    const key = `${ack.requestId}:${ack.table}:${ack.batchIndex}`
    const waiter = this.syncAckWaiters.get(key)
    if (waiter) {
      this.syncAckWaiters.delete(key)
      waiter.resolve(ack)
    }
  }

  private abortSyncSession(requestId: string): void {
    const session = this.activeSyncs.get(requestId)
    if (!session) return

    session.aborted = true

    for (const [key, waiter] of this.syncAckWaiters) {
      if (key.startsWith(`${requestId}:`)) {
        this.syncAckWaiters.delete(key)
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
        this.emitError({
          error: wrappedErr,
          operation: 'sync-session-abort-close',
          peerId: session.joinerNodeId,
          recoverable: true,
        })
      })
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      this.emitError({
        error: wrappedErr,
        operation: 'sync-session-abort-close',
        peerId: session.joinerNodeId,
        recoverable: true,
      })
    }
    this.log.unregisterActiveSyncSeq(session.snapshotSeq)
    this.activeSyncs.delete(requestId)
    clearTimeout(session.timeoutTimer)
  }

  private async initiateSync(): Promise<void> {
    if (!this.tracker) {
      throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
    }

    const peers = this.config.transport.peers()
    let sourcePeerId: string | null = null

    for (const [peerId, info] of peers) {
      if (info.role === 'primary') {
        sourcePeerId = peerId
        break
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

    const savedState = await this.log.getSyncState()
    const completedTables = savedState.phase === 'pending' ? [] : savedState.completedTables

    const requestId = randomUUID()
    await this.config.transport.requestSync(sourcePeerId, {
      requestId,
      joinerNodeId: this.nodeId,
      completedTables,
    })

    this.syncState.phase = 'syncing'
    this.syncState.sourcePeerId = sourcePeerId
    this.syncState.startedAt = Date.now()
    await this.log.setSyncMeta('syncing', undefined, sourcePeerId, requestId)
  }

  private async handleSyncBatchReceived(batch: SyncBatch, fromPeerId: string): Promise<void> {
    if (this.syncState.phase !== 'syncing') return
    if (fromPeerId !== this.syncState.sourcePeerId) return

    if (!this.tracker) {
      throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
    }

    const expectedIndex = this.expectedBatchIndex.get(batch.table) ?? 0
    if (batch.batchIndex !== expectedIndex) {
      await this.config.transport.sendSyncAck(fromPeerId, {
        requestId: batch.requestId,
        joinerNodeId: this.nodeId,
        table: batch.table,
        batchIndex: batch.batchIndex,
        success: false,
        error: `Out of order batch: expected ${expectedIndex}, got ${batch.batchIndex}`,
      })
      return
    }
    this.expectedBatchIndex.set(batch.table, expectedIndex + 1)

    try {
      if (batch.table === '__schema__') {
        if (batch.schema) {
          for (const ddl of batch.schema) {
            if (!this.validateSyncDdl(ddl)) {
              throw new SyncError(`Unsafe DDL in schema batch: ${ddl}`)
            }
          }
          await this.writerConn.exec('PRAGMA foreign_keys = OFF')
          for (const ddl of batch.schema) {
            let saferDdl = ddl.replace(/^\s*CREATE\s+TABLE\b/i, 'CREATE TABLE IF NOT EXISTS')
            saferDdl = saferDdl.replace(/^\s*CREATE\s+INDEX\b/i, 'CREATE INDEX IF NOT EXISTS')
            await this.writerConn.exec(saferDdl)
          }
        }

        await this.config.transport.sendSyncAck(fromPeerId, {
          requestId: batch.requestId,
          joinerNodeId: this.nodeId,
          table: batch.table,
          batchIndex: batch.batchIndex,
          success: true,
        })
        return
      }

      if (!IDENTIFIER_RE.test(batch.table)) {
        throw new SyncError(`Invalid table name in sync batch: ${batch.table}`)
      }

      const expectedChecksum = createHash('sha256').update(JSON.stringify(batch.rows)).digest('hex')
      if (batch.checksum !== expectedChecksum) {
        throw new SyncError(`Checksum mismatch for ${batch.table} batch ${batch.batchIndex}`)
      }

      if (batch.batchIndex === 0 && !this.syncState.completedTables.includes(batch.table)) {
        await this.tracker.unwatch(this.writerConn, batch.table)
        await this.writerConn.exec(`DELETE FROM "${batch.table}"`)
      }

      if (batch.rows.length > 0) {
        const columns = Object.keys(batch.rows[0]).filter(c => IDENTIFIER_RE.test(c))
        if (columns.length > 0) {
          const placeholders = columns.map(() => '?').join(', ')
          const colNames = columns.map(c => `"${c}"`).join(', ')
          const insertSql = `INSERT INTO "${batch.table}" (${colNames}) VALUES (${placeholders})`

          await this.writerConn.transaction(async tx => {
            const stmt = await tx.prepare(insertSql)
            for (const row of batch.rows) {
              const values = columns.map(c => row[c])
              await stmt.run(...values)
            }
          })
        }
      }

      if (batch.isLastBatchForTable) {
        await this.log.setSyncTableStatus(batch.table, 'completed')
        this.syncState.completedTables.push(batch.table)
        this.expectedBatchIndex.delete(batch.table)
      }

      await this.config.transport.sendSyncAck(fromPeerId, {
        requestId: batch.requestId,
        joinerNodeId: this.nodeId,
        table: batch.table,
        batchIndex: batch.batchIndex,
        success: true,
      })
    } catch (err) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      this.emitError({ error: wrappedErr, operation: 'sync-batch-processing', peerId: fromPeerId, recoverable: true })
      await this.config.transport
        .sendSyncAck(fromPeerId, {
          requestId: batch.requestId,
          joinerNodeId: this.nodeId,
          table: batch.table,
          batchIndex: batch.batchIndex,
          success: false,
          error: wrappedErr.message,
        })
        .catch((ackErr: unknown) => {
          const ackWrapped = ackErr instanceof Error ? ackErr : new Error(String(ackErr))
          this.emitError({ error: ackWrapped, operation: 'sync-ack-send', peerId: fromPeerId, recoverable: true })
        })
    }
  }

  private async handleSyncCompleteReceived(complete: SyncComplete, fromPeerId: string): Promise<void> {
    if (this.syncState.phase !== 'syncing') return
    if (fromPeerId !== this.syncState.sourcePeerId) return

    if (!this.tracker) {
      throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
    }

    try {
      for (const manifest of complete.manifests) {
        if (IDENTIFIER_RE.test(manifest.table)) {
          await this.tracker.watch(this.writerConn, manifest.table)
        }
      }

      await this.writerConn.exec('PRAGMA foreign_keys = ON')

      for (const manifest of complete.manifests) {
        const valid = await this.log.verifyManifest(manifest)
        if (!valid) {
          await this.log.wipeTables(
            this.writerConn,
            complete.manifests.map(m => m.table).filter(t => IDENTIFIER_RE.test(t)),
            this.tracker,
          )
          this.syncState.phase = 'pending'
          this.syncState.completedTables = []
          this.expectedBatchIndex.clear()
          await this.log.setSyncMeta('pending')
          await this.initiateSync()
          return
        }
      }

      await this.log.setLastAppliedSeq(fromPeerId, complete.snapshotSeq)

      const recordStmt = await this.writerConn.prepare(
        'INSERT OR IGNORE INTO _sirannon_applied_changes (source_node_id, source_seq, applied_at) VALUES (?, ?, ?)',
      )
      await recordStmt.run(fromPeerId, complete.snapshotSeq.toString(), Date.now() / 1000)

      this.syncState.phase = 'catching-up'
      this.syncState.snapshotSeq = complete.snapshotSeq
      await this.log.setSyncMeta('catching-up', complete.snapshotSeq)

      this.startSenderLoop()
      this.startCatchUpCheck()
    } catch {
      try {
        await this.writerConn.exec('PRAGMA foreign_keys = ON')
      } catch (pragmaErr: unknown) {
        const wrappedErr = pragmaErr instanceof Error ? pragmaErr : new Error(String(pragmaErr))
        this.emitError({ error: wrappedErr, operation: 'sync-complete-pragma-restore', recoverable: false })
      }
      this.emitError({
        error: new SyncError('Failed during sync completion handling'),
        operation: 'sync-complete-handling',
        recoverable: false,
      })
      return
    }
  }

  private startCatchUpCheck(): void {
    const catchUpStartedAt = Date.now()
    this.catchUpCheckTimer = setInterval(async () => {
      if (this.syncState.phase !== 'catching-up') {
        if (this.catchUpCheckTimer) clearInterval(this.catchUpCheckTimer)
        this.catchUpCheckTimer = null
        return
      }

      if (Date.now() - catchUpStartedAt > this.catchUpDeadlineMs) {
        this.syncState.phase = 'ready'
        await this.log.setSyncMeta('ready')
        if (this.catchUpCheckTimer) clearInterval(this.catchUpCheckTimer)
        this.catchUpCheckTimer = null
        return
      }

      const sourcePeerId = this.syncState.sourcePeerId
      if (!sourcePeerId) return

      if (this.highestSourceSeqSeen === 0n) {
        const snapshotSeq = this.syncState.snapshotSeq ?? 0n
        if (snapshotSeq === 0n) {
          this.syncState.phase = 'ready'
          await this.log.setSyncMeta('ready')
          if (this.catchUpCheckTimer) clearInterval(this.catchUpCheckTimer)
          this.catchUpCheckTimer = null
          return
        }
        const localSeq = await this.log.getLocalSeq()
        if (localSeq === 0n) {
          this.syncState.phase = 'ready'
          await this.log.setSyncMeta('ready')
          if (this.catchUpCheckTimer) clearInterval(this.catchUpCheckTimer)
          this.catchUpCheckTimer = null
          return
        }
        return
      }

      const appliedSeq = await this.log.getLastAppliedSeq(sourcePeerId)
      const lag = this.highestSourceSeqSeen - appliedSeq
      if (lag <= BigInt(this.maxSyncLagBeforeReady)) {
        this.syncState.phase = 'ready'
        await this.log.setSyncMeta('ready')
        if (this.catchUpCheckTimer) clearInterval(this.catchUpCheckTimer)
        this.catchUpCheckTimer = null
      }
    }, this.batchIntervalMs * 2)
  }

  private validateSyncDdl(sql: string): boolean {
    const safePrefixRe =
      /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i
    if (!safePrefixRe.test(sql)) return false
    if (sql.includes(';')) return false
    if (/\bAS\s+SELECT\b/i.test(sql)) return false
    if (DDL_DENY_RE.test(sql)) return false
    const body = sql.replace(safePrefixRe, '')
    if (/\bSELECT\b/i.test(body)) return false
    return true
  }

  private startSenderLoop(): void {
    if (!this.running) return

    this.senderTimer = setTimeout(async () => {
      if (!this.running) return

      try {
        this.lastSentSeq = await this.log.getLocalSeq()
        await this.sendPendingBatches()
        await this.updatePruneBoundary()
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.emitError({ error: wrappedErr, operation: 'sender-loop', recoverable: true })
      }

      this.startSenderLoop()
    }, this.batchIntervalMs)
    this.senderTimer.unref()
  }

  private clearSenderTimer(): void {
    if (this.senderTimer !== null) {
      clearTimeout(this.senderTimer)
      this.senderTimer = null
    }
  }

  private async updatePruneBoundary(): Promise<void> {
    if (!this.tracker) return
    const minAcked = await this.log.getMinAckedSeq()
    if (minAcked === null) {
      this.tracker.clearPruneBoundary()
    } else {
      this.tracker.setPruneBoundary(minAcked)
    }
  }

  private async sendPendingBatches(): Promise<void> {
    const peers = this.config.transport.peers()
    const nowMs = Date.now()

    for (const [peerId, peerInfo] of peers) {
      if (!this.config.topology.shouldReplicateTo(peerId, peerInfo.role)) {
        continue
      }

      const peerState = this.peerTracker.getPeerState(peerId)
      if (peerState) {
        this.peerTracker.expireTimedOutBatches(peerId, nowMs, this.ackTimeoutMs)
      }

      if (peerState && peerState.pendingBatches >= this.maxPendingBatches) {
        continue
      }

      const fromSeq = peerState?.lastSentSeq ?? this.lastSentSeq
      const batch = await this.log.readBatch(fromSeq, this.batchSize)
      if (!batch) continue

      const previousSeq = peerState?.lastSentSeq ?? 0n
      if (peerState) {
        peerState.pendingBatches += 1
        peerState.lastSentSeq = batch.toSeq
        this.peerTracker.recordInFlightBatch(peerId, {
          batchId: batch.batchId,
          fromSeq: batch.fromSeq,
          toSeq: batch.toSeq,
          sentAt: nowMs,
        })
      }

      this.config.transport.send(peerId, batch).catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.emitError({ error: wrappedErr, operation: 'transport-send', peerId, recoverable: true })
        if (peerState) {
          const idx = peerState.inFlightBatches.findIndex(b => b.batchId === batch.batchId)
          if (idx >= 0) {
            peerState.inFlightBatches.splice(idx, 1)
          }
          if (peerState.pendingBatches > 0) {
            peerState.pendingBatches -= 1
          }
          if (previousSeq < peerState.lastSentSeq) {
            peerState.lastSentSeq = previousSeq
          }
        }
      })

      if (this.config.flowControl?.maxLagSeconds && peerState) {
        const lagMs = Number(batch.toSeq - peerState.lastAckedSeq) * this.batchIntervalMs
        const maxLagMs = this.config.flowControl.maxLagSeconds * 1000
        if (lagMs > maxLagMs && this.config.flowControl.onLagExceeded) {
          this.config.flowControl.onLagExceeded(peerId, lagMs)
        }
      }
    }
  }

  private async waitForWriteConcern(seq: bigint, wc: { level: string; timeoutMs?: number }): Promise<void> {
    const timeout = wc.timeoutMs ?? 5000

    if (wc.level === 'majority') {
      await this.peerTracker.waitForMajority(seq, timeout)
    } else if (wc.level === 'all') {
      await this.peerTracker.waitForAll(seq, timeout)
    }
  }

  private checkClockDrift(remoteHlc: string): number {
    const decoded = HLC.decode(remoteHlc)
    return Math.abs(Date.now() - decoded.wallMs)
  }

  private getResolver(table?: string): ConflictResolver {
    if (table && this.config.conflictResolvers) {
      const specific = this.config.conflictResolvers[table]
      if (specific) return specific
    }
    return this.defaultResolver
  }
}
