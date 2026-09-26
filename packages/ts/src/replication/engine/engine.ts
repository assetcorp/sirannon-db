import { EventEmitter } from 'node:events'
import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import { LWWResolver } from '../../core/sync/conflict/lww.js'
import { HLC } from '../../core/sync/hlc.js'
import type { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions } from '../../core/types.js'
import type { CoordinatorLease, CoordinatorWatchDisposer, ReplicationGroupState } from '../coordinator/types.js'
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
import { markCoordinatorSyncReady } from './coordinator-membership.js'
import { execute, executeBatch, forwardStatements, query, transaction } from './data-api.js'
import { initialSyncState } from './internal-types.js'
import { LocalExecutor } from './local-executor.js'
import { computeNodeHealth } from './node-health.js'
import { SenderLoop } from './sender-loop.js'
import { startEngine, stopEngine } from './startup.js'
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
 * The engine exposes its state and dependencies as properties, so that the
 * helper modules that implement it can share one mutable instance.
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
   * Identifies this node, and the engine stamps it on every change that this node writes.
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
  nodeSessionWatchDisposer: CoordinatorWatchDisposer | null = null
  /** @internal */
  controllerLeaseWatchDisposer: CoordinatorWatchDisposer | null = null
  /** @internal */
  observedControllerLease: CoordinatorLease | null = null
  /** @internal */
  controllerBidding = false
  /** @internal */
  liveNodeIds: string[] | null = null
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
   * Connects the transport and starts replicating. When this node is a replica without a complete, current copy, it
   * also requests a full copy from a source peer, and the returned promise resolves before that copy finishes.
   */
  async start(): Promise<void> {
    try {
      await startEngine(this)
    } catch (err) {
      await this.stop().catch(() => {})
      throw err
    }
  }

  /**
   * Stops replicating, aborts every first sync that this node serves, and disconnects the transport.
   */
  async stop(): Promise<void> {
    await stopEngine(this)
  }

  /**
   * Returns this node's replication status.
   *
   * @returns The node's role, peers, sequence position, sync state, health, and coordinator group state.
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
   * Returns the highest position in this node's local change log.
   *
   * @returns That position, which a caller can wait for a replica to reach.
   */
  getCurrentSeq(): bigint {
    return this.lastLocalSeq
  }

  /**
   * Returns the highest applied position from one peer's change log.
   *
   * @param peerId - The peer's node ID.
   * @returns That position, or `0n` before this node applies any change from that peer.
   */
  getAppliedSeq(peerId: string): bigint {
    return this.appliedSeqByPeer.get(peerId) ?? 0n
  }

  /**
   * Executes a read, and throws when this node is still syncing or cannot meet the read concern.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The read concern for this statement.
   * @returns The rows that the statement returns.
   */
  query<T>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
    return query<T>(this, sql, params, options)
  }

  /**
   * Executes one write on this node, or forwards it to the primary when this node cannot accept writes and
   * `writeForwarding` is on.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @param options - The write concern for this statement, which the primary also applies to a forwarded write before it answers.
   * @returns The number of rows that changed and the ID of the last inserted row.
   */
  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    return execute(this, sql, params, options)
  }

  /**
   * Executes one statement once for each parameter set and commits every set in one transaction, on this node or, when
   * this node forwards the batch, on the primary.
   *
   * @param sql - The statement to execute for each parameter set.
   * @param paramsBatch - One parameter set per execution.
   * @param options - The write concern for the batch, which the primary also applies to a forwarded batch before it answers.
   * @returns One result per parameter set, in order.
   */
  executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    return executeBatch(this, sql, paramsBatch, options)
  }

  /**
   * Calls `fn` inside one transaction on this node. On a node that cannot accept writes, it throws a `TopologyError`,
   * since the engine forwards only `execute` and `executeBatch` writes to the primary.
   *
   * @param fn - The function that receives the transaction and executes statements on it.
   * @param options - The write concern to wait for after the transaction commits.
   * @returns The value that `fn` resolves to.
   */
  transaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T> {
    return transaction<T>(this, fn, options)
  }

  /**
   * Sends the statements to the primary, which executes them in one transaction, and returns the primary's result.
   * When this node is the primary, it executes them here in the same way.
   *
   * @param statements - The statements to execute, in order, each with its own parameters.
   * @param options - The write concern that the primary applies to the statements before it answers.
   * @returns The result of each statement, in order, and the request ID.
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
      } catch {}
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
   * Returns a copy of an outgoing replication message with this node's group ID and primary term added.
   *
   * @internal
   */
  decorate<T extends CoordinatorStampedMessage>(message: T): T {
    return { ...message, ...getCoordinatorMessageFields(this) }
  }
}
