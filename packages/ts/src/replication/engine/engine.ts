import { EventEmitter } from 'node:events'
import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import { LWWResolver } from '../../core/sync/conflict/lww.js'
import { HLC } from '../../core/sync/hlc.js'
import { setForeignKeysEnabled } from '../../core/system-catalog/index.js'
import type { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions } from '../../core/types.js'
import type { CoordinatorWatchDisposer, ReplicationGroupState } from '../coordinator/types.js'
import { AuthorityError } from '../errors.js'
import { ReplicationLog } from '../log.js'
import { generateNodeId } from '../node-id.js'
import { PeerTracker } from '../peer-tracker.js'
import type {
  ConflictResolver,
  ForwardedTransactionResult,
  ReplicationAck,
  ReplicationBatch,
  ReplicationConfig,
  ReplicationErrorEvent,
  ReplicationStatus,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
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
import {
  effectiveTopologyRole,
  getCoordinatorMessageFields,
  getCoordinatorRuntimeStatus,
  getForwardingPrimaryPeerId,
  verifyPrimaryAuthority,
} from './coordinator-authority.js'
import { stopCoordinatorMode, stopCoordinatorTimers } from './coordinator-lifecycle.js'
import { markCoordinatorSyncReady } from './coordinator-membership.js'
import { execute, executeBatch, forwardStatements, query, transaction } from './data-api.js'
import { initialSyncState } from './internal-types.js'
import { LocalExecutor } from './local-executor.js'
import { computeNodeHealth } from './node-health.js'
import { SenderLoop } from './sender-loop.js'
import { startEngine } from './startup.js'
import { SyncJoiner } from './sync-joiner.js'
import { SyncServer } from './sync-server.js'
import type { TableStreamDigest } from './sync-verification.js'
import { installTestHooks } from './test-hooks.js'

type CoordinatorStampedMessage =
  | ReplicationBatch
  | ReplicationAck
  | ForwardedTransactionResult
  | SyncRequest
  | SyncBatch
  | SyncComplete
  | SyncAck

/**
 * Coordinates replication for a single database node.
 *
 * Its state and dependencies are readable properties so that the collaborating
 * modules in `./engine/` share one mutable engine instance.
 *
 * @public
 */
export class ReplicationEngine extends EventEmitter {
  /** @internal */
  readonly database: Database
  /** @internal */
  readonly writerConn: SQLiteConnection
  /** @internal */
  readonly config: ReplicationConfig
  /**
   * Identifier of this node, which every change it authors carries.
   */
  readonly nodeId: string
  /** @internal */
  readonly hlc: HLC
  /** @internal */
  readonly log: ReplicationLog
  /** @internal */
  readonly peerTracker = new PeerTracker()
  /** @internal */
  readonly defaultResolver: ConflictResolver
  /** @internal */
  readonly tracker: ChangeTracker | undefined
  /** @internal */
  readonly snapshotConnectionFactory: (() => Promise<SQLiteConnection>) | undefined

  /** @internal */
  readonly batchSize: number
  /** @internal */
  readonly batchIntervalMs: number
  /** @internal */
  readonly maxClockDriftMs: number
  /** @internal */
  readonly maxPendingBatches: number
  /** @internal */
  readonly maxBatchChanges: number
  /** @internal */
  readonly ackTimeoutMs: number

  /** @internal */
  readonly initialSync: boolean
  /** @internal */
  readonly syncBatchSize: number
  /** @internal */
  readonly maxConcurrentSyncs: number
  /** @internal */
  readonly maxSyncDurationMs: number
  /** @internal */
  readonly maxSyncLagBeforeReady: number
  /** @internal */
  readonly syncAckTimeoutMs: number
  /** @internal */
  readonly catchUpDeadlineMs: number
  /** @internal */
  readonly resumeFromSeq: bigint | undefined

  /** @internal */
  running = false
  /** @internal */
  coordinatorState: ReplicationGroupState | null = null
  /** @internal */
  coordinatorAuthority = false
  /** @internal */
  controllerState: 'disabled' | 'standby' | 'active' | 'lost' = 'disabled'
  /** @internal */
  nodeSessionLeaseId: string | null = null
  /** @internal */
  controllerLeaseId: string | null = null
  /** @internal */
  coordinatorWatchDisposer: CoordinatorWatchDisposer | null = null
  /** @internal */
  coordinatorLeaseTimer: ReturnType<typeof setInterval> | null = null
  /** @internal */
  controllerTimer: ReturnType<typeof setInterval> | null = null
  /** @internal */
  coordinatorRejoinSyncStarting = false
  /** @internal */
  coordinatorSessionRestoring = false
  /** @internal */
  coordinatorLastContactMs = 0
  /** @internal */
  inSyncReconcileTimer: ReturnType<typeof setInterval> | null = null
  /** @internal */
  inSyncReconciling = false
  /** @internal */
  lastSentSeq = 0n
  /** @internal */
  lastLocalSeq = 0n
  /** @internal */
  highestSourceSeqSeen = 0n
  /** @internal */
  readonly appliedSeqByPeer = new Map<string, bigint>()
  /** @internal */
  readonly expectedBatchIndex = new Map<string, number>()
  /** @internal */
  readonly syncTableDigests = new Map<string, TableStreamDigest>()
  /** @internal */
  syncState: SyncState = initialSyncState()

  /** @internal */
  readonly localExecutor: LocalExecutor
  /** @internal */
  readonly syncServer: SyncServer
  /** @internal */
  readonly syncJoiner: SyncJoiner
  /** @internal */
  readonly senderLoop: SenderLoop

  constructor(database: Database, writerConn: SQLiteConnection, config: ReplicationConfig) {
    super()
    this.database = database
    this.writerConn = writerConn
    this.config = config
    if (config.coordinator && !config.nodeId) {
      throw new AuthorityError('Coordinator mode requires a stable persisted nodeId')
    }
    this.nodeId = config.nodeId ?? generateNodeId()
    this.hlc = new HLC(this.nodeId)
    this.tracker = config.changeTracker
    this.log = new ReplicationLog(writerConn, this.nodeId, this.hlc, CHANGES_TABLE, this.tracker)
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

  /**
   * Connects the transport, pulls a full copy when this node needs one, and starts replicating.
   */
  start(): Promise<void> {
    return startEngine(this)
  }

  /**
   * Stops replicating, abandons any sync in flight, and disconnects the transport.
   */
  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false
    stopCoordinatorTimers(this)

    this.syncJoiner.stopTimers()
    this.syncServer.abortAll()

    if (this.syncState.phase === 'syncing') {
      try {
        await setForeignKeysEnabled(this.writerConn, true)
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.emitError({ error: wrappedErr, operation: 'engine-stop-pragma-restore', recoverable: false })
      }
    }

    this.senderLoop.stop()
    if (this.tracker) {
      this.tracker.clearPruneBoundary()
    }
    await stopCoordinatorMode(this)
    await this.config.transport.disconnect()
  }

  /**
   * Reports where this node stands.
   *
   * @returns The node's role, its peers, its progress, its health, and its group state.
   */
  status(): ReplicationStatus {
    return {
      nodeId: this.nodeId,
      role: effectiveTopologyRole(this),
      peers: this.peerTracker.allPeerStates(),
      localSeq: this.lastSentSeq,
      replicating: this.running,
      health: computeNodeHealth(this),
      syncState: { ...this.syncState },
      coordinator: getCoordinatorRuntimeStatus(this),
    }
  }

  /**
   * Returns the highest change-log position this node has recorded locally.
   *
   * @returns That position, which a caller waits for a replica to reach.
   */
  getCurrentSeq(): bigint {
    return this.lastLocalSeq
  }

  /**
   * Returns how far this node has applied one peer's changes.
   *
   * @param peerId - Identifier of the peer.
   * @returns The highest position from that peer this node has applied.
   */
  getAppliedSeq(peerId: string): bigint {
    return this.appliedSeqByPeer.get(peerId) ?? 0n
  }

  /**
   * Runs a read, refusing it when the node cannot meet the read concern.
   *
   * @param sql - The statement to run.
   * @param params - Values bound to the statement, named or positional.
   * @param options - Read concern for this statement.
   * @returns The rows the statement produced.
   */
  query<T>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    return query<T>(this, sql, params, options)
  }

  /**
   * Runs one write, forwarding it to the primary when this node accepts no writes and forwarding is on.
   *
   * @param sql - The statement to run.
   * @param params - Values bound to the statement, named or positional.
   * @param options - Write concern for this statement.
   * @returns How many rows changed, and the last inserted row id.
   */
  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    return execute(this, sql, params, options)
  }

  /**
   * Runs one statement over many parameter sets in a single transaction.
   *
   * @param sql - The statement to run for each parameter set.
   * @param paramsBatch - One parameter set per run.
   * @param options - Write concern for the transaction.
   * @returns One result per parameter set, in order.
   */
  executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    return executeBatch(this, sql, paramsBatch, options)
  }

  /**
   * Runs a function inside one transaction on this node.
   *
   * @param fn - Receives the transaction and runs statements on it.
   * @param options - Write concern for the transaction.
   * @returns Whatever the function returned.
   */
  transaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T> {
    return transaction<T>(this, fn, options)
  }

  /**
   * Sends a write to the primary and waits for its result.
   *
   * @param statements - The statements to run, in order, each with its own parameters.
   * @param options - Write concern the primary applies.
   * @returns What the primary reported for each statement.
   */
  forwardStatements(
    statements: Array<{ sql: string; params?: Params }>,
    options?: QueryOptions,
  ): Promise<ForwardedTransactionResult> {
    return forwardStatements(this, statements, options)
  }

  /** @internal */
  emitError(event: ReplicationErrorEvent): void {
    if (this.listenerCount('replication-error') > 0) {
      try {
        this.emit('replication-error', event)
      } catch {
        /* Listener failures must not disrupt engine operation */
      }
    }
  }

  /** @internal */
  isCoordinatorMode(): boolean {
    return this.config.coordinator !== undefined
  }

  /** @internal */
  verifyPrimaryAuthority(): Promise<ReplicationGroupState> {
    return verifyPrimaryAuthority(this)
  }

  /** @internal */
  markCoordinatorSyncReady(): Promise<void> {
    return markCoordinatorSyncReady(this)
  }

  /** @internal */
  getCurrentPrimaryPeerId(): string | null {
    return getForwardingPrimaryPeerId(this)
  }

  /**
   * Stamps an outgoing replication message with this node's group and primary term.
   *
   * @internal
   */
  decorate<T extends CoordinatorStampedMessage>(message: T): T {
    return { ...message, ...getCoordinatorMessageFields(this) }
  }
}
