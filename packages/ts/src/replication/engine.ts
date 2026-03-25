import { randomUUID } from 'node:crypto'
import type { Database } from '../core/database.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import type { Transaction } from '../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions } from '../core/types.js'
import { LWWResolver } from './conflict/lww.js'
import { BatchValidationError, ReplicationError, TopologyError } from './errors.js'
import { HLC } from './hlc.js'
import { ReplicationLog } from './log.js'
import { generateNodeId } from './node-id.js'
import { PeerTracker } from './peer-tracker.js'
import type { ConflictResolver, ForwardedTransactionResult, ReplicationConfig, ReplicationStatus } from './types.js'

const DEFAULT_BATCH_SIZE = 100
const DEFAULT_BATCH_INTERVAL_MS = 100
const DEFAULT_MAX_CLOCK_DRIFT_MS = 60_000
const DEFAULT_MAX_PENDING_BATCHES = 10
const DEFAULT_MAX_BATCH_CHANGES = 1000

const DDL_PREFIX_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

const SAFE_SQL_PREFIX_RE =
  /^\s*(INSERT|UPDATE|DELETE|SELECT|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

/**
 * The central coordinator for distributed replication in Sirannon.
 *
 * ReplicationEngine wraps a local Database and adds multi-node awareness: it
 * stamps local writes with HLC timestamps, groups them into batches on a
 * configurable interval, and ships those batches to connected peers through a
 * pluggable transport layer. Inbound batches from remote peers are validated
 * (checksum, clock-drift, topology authorization) and applied through the
 * ReplicationLog, which delegates row-level conflicts to a per-table
 * ConflictResolver.
 *
 * Write routing respects the configured Topology: on a replica node, writes
 * are either rejected with a TopologyError or forwarded to the primary when
 * writeForwarding is enabled. Read queries are passed through to the
 * underlying Database unchanged.
 *
 * Each peer maintains an independent outbound cursor so that a slow or
 * temporarily disconnected peer does not block delivery to others. Ack
 * tracking feeds into optional write-concern levels (majority, all) that let
 * callers await durable replication before returning.
 */
export class ReplicationEngine {
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

  constructor(database: Database, writerConn: SQLiteConnection, config: ReplicationConfig) {
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
  }

  async start(): Promise<void> {
    if (this.running) return
    this.running = true

    await this.log.ensureReplicationTables()
    this.lastSentSeq = await this.log.getLocalSeq()

    this.setupTransportHandlers()
    await this.config.transport.connect(this.nodeId, this.config.transportConfig ?? {})
    this.startSenderLoop()
  }

  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    this.clearSenderTimer()
    await this.config.transport.disconnect()
  }

  status(): ReplicationStatus {
    return {
      nodeId: this.nodeId,
      role: this.config.topology.role,
      peers: this.peerTracker.allPeerStates(),
      localSeq: this.lastSentSeq,
      replicating: this.running,
    }
  }

  async query<T>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    return this.database.query<T>(sql, params, options)
  }

  async execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
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
        await this.log.stampChanges(tx, Number(seqBefore), txId)
        await this.log.updateColumnVersions(tx, Number(seqBefore))
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

      await this.log.stampChanges(tx, Number(seqBefore), txId)
      await this.log.updateColumnVersions(tx, Number(seqBefore))
    })

    return { results, requestId }
  }

  private setupTransportHandlers(): void {
    this.config.transport.onBatchReceived(async (batch, fromPeerId) => {
      if (!this.running) return

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
      if (!this.config.topology.shouldAcceptFrom(fromPeerId, peerInfo?.role ?? 'peer')) {
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

      this.config.transport
        .sendAck(fromPeerId, {
          batchId: batch.batchId,
          ackedSeq: batch.toSeq,
          nodeId: this.nodeId,
        })
        .catch(() => {})
    })

    this.config.transport.onAckReceived((ack, _fromPeerId) => {
      if (!this.running) return
      this.peerTracker.onAckReceived(ack.nodeId, ack.ackedSeq)
    })

    if (this.config.topology.canWrite()) {
      this.config.transport.onForwardReceived(async (request, fromPeerId) => {
        const knownPeers = this.config.transport.peers()
        if (!knownPeers.has(fromPeerId)) {
          throw new ReplicationError(`Rejected forward from unknown peer: ${fromPeerId}`)
        }
        return this.executeForwardedLocally(request.statements)
      })
    }

    this.config.transport.onPeerConnected(peer => {
      this.peerTracker.addPeer(peer.id)
    })

    this.config.transport.onPeerDisconnected(peerId => {
      this.peerTracker.removePeer(peerId)
    })
  }

  private startSenderLoop(): void {
    if (!this.running) return

    this.senderTimer = setTimeout(async () => {
      if (!this.running) return

      try {
        this.lastSentSeq = await this.log.getLocalSeq()
        await this.sendPendingBatches()
      } catch {
        /* sender loop failures are transient */
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

      const sentToSeq = batch.toSeq
      this.config.transport.send(peerId, batch).catch(() => {
        if (peerState) {
          const idx = peerState.inFlightBatches.findIndex(b => b.batchId === batch.batchId)
          if (idx >= 0) {
            peerState.inFlightBatches.splice(idx, 1)
          }
          if (peerState.pendingBatches > 0) {
            peerState.pendingBatches -= 1
          }
          if (peerState.lastSentSeq === sentToSeq) {
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
