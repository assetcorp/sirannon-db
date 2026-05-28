import { randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import type { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions } from '../../core/types.js'
import { LWWResolver } from '../conflict/lww.js'
import type { CoordinatorWatchDisposer, ReplicationGroupState } from '../coordinator/types.js'
import {
  AuthorityError,
  CoordinatorError,
  NodeDrainingError,
  NodeNotInSyncError,
  ReadConcernError,
  StalePrimaryError,
  SyncError,
  TopologyError,
} from '../errors.js'
import { HLC } from '../hlc.js'
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
import { LocalExecutor } from './local-executor.js'
import { SenderLoop } from './sender-loop.js'
import { startEngine } from './startup.js'
import { SyncJoiner } from './sync-joiner.js'
import { SyncServer } from './sync-server.js'
import { installTestHooks } from './test-hooks.js'

const DEFAULT_COORDINATOR_SESSION_TTL_MS = 10_000
const DEFAULT_CONTROLLER_LEASE_TTL_MS = 10_000
const DEFAULT_CONTROLLER_TICK_INTERVAL_MS = 1_000

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
  coordinatorState: ReplicationGroupState | null = null
  coordinatorAuthority = false
  controllerState: 'disabled' | 'standby' | 'active' | 'lost' = 'disabled'
  private nodeSessionLeaseId: string | null = null
  private controllerLeaseId: string | null = null
  private coordinatorWatchDisposer: CoordinatorWatchDisposer | null = null
  private coordinatorLeaseTimer: ReturnType<typeof setInterval> | null = null
  private controllerTimer: ReturnType<typeof setInterval> | null = null
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
    if (config.coordinator && !config.nodeId) {
      throw new AuthorityError('Coordinator mode requires a stable persisted nodeId')
    }
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
    this.stopCoordinatorTimers()

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
    await this.stopCoordinatorMode()
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
      coordinator: this.getCoordinatorRuntimeStatus(),
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
    await this.assertReadConcern(options?.readConcern?.level)
    return this.database.query<T>(sql, params, options)
  }

  async execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    if (this.syncState.phase !== 'ready') {
      throw new SyncError(`Node is in '${this.syncState.phase}' phase and cannot accept operations`)
    }

    if (!(await this.canAcceptLocalWrite())) {
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
      this.throwNotCurrentPrimary()
    }

    return this.localExecutor.executeLocally(sql, params, options)
  }

  async executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    if (this.syncState.phase !== 'ready') {
      throw new SyncError(`Node is in '${this.syncState.phase}' phase and cannot accept operations`)
    }

    if (!(await this.canAcceptLocalWrite())) {
      if (this.config.writeForwarding) {
        const statements = paramsBatch.map(p => ({ sql, params: p }))
        const result = await this.forwardStatements(statements, options)
        return result.results.map(r => ({
          changes: r.changes,
          lastInsertRowId:
            typeof r.lastInsertRowId === 'string' ? BigInt(r.lastInsertRowId) : Number(r.lastInsertRowId),
        }))
      }
      this.throwNotCurrentPrimary()
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
    if (!(await this.canAcceptLocalWrite())) {
      throw new TopologyError('This node cannot accept writes in transaction mode')
    }
    return this.localExecutor.executeTransactionLocally(fn, options)
  }

  async forwardStatements(
    statements: Array<{ sql: string; params?: Params }>,
    _options?: QueryOptions,
  ): Promise<ForwardedTransactionResult> {
    if (await this.canAcceptLocalWrite()) {
      return this.localExecutor.executeForwardedLocally(statements)
    }

    const primaryPeerId = this.getForwardingPrimaryPeerId()

    if (primaryPeerId === null) {
      throw new TopologyError('No primary node available for write forwarding')
    }

    return this.config.transport.forward(primaryPeerId, {
      statements,
      requestId: randomUUID(),
      ...this.getCoordinatorMessageFields(),
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

    if (this.isCoordinatorMode()) {
      const state = this.coordinatorState ?? (await this.refreshCoordinatorState())
      if (!state) {
        throw new CoordinatorError('Cannot satisfy write concern without replication-group state')
      }
      if (wc.level === 'majority') {
        await this.peerTracker.waitForConfiguredMajority(seq, this.nodeId, state.votingDataBearingNodeIds, timeout)
        await this.updateCoordinatorInSyncAfterWrite(seq, state)
      } else if (wc.level === 'all') {
        await this.peerTracker.waitForConfiguredAll(seq, this.nodeId, state.votingDataBearingNodeIds, timeout)
      }
      return
    }

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

  isCoordinatorMode(): boolean {
    return this.config.coordinator !== undefined
  }

  async startCoordinatorMode(): Promise<void> {
    const config = this.config.coordinator
    if (!config) return

    const coordinator = config.coordinator
    let state = await coordinator.getReplicationGroupState(config.clusterId, config.groupId)
    if (!state && config.votingDataBearingNodeIds) {
      state = await coordinator.setReplicationGroupState({
        clusterId: config.clusterId,
        groupId: config.groupId,
        votingDataBearingNodeIds: config.votingDataBearingNodeIds,
        currentPrimary: this.config.topology.role === 'primary' ? this.localCoordinatorPrimary() : null,
        primaryTerm: 1n,
        inSyncNodeIds: [this.nodeId],
        compatibility: config.compatibility,
      })
    }
    if (!state) {
      throw new CoordinatorError(`Replication group '${config.groupId}' is not registered`)
    }
    this.coordinatorState = state
    this.coordinatorAuthority = this.hasCurrentPrimaryAuthorityFor(state)

    const session = await coordinator.registerNodeSession({
      clusterId: config.clusterId,
      nodeId: this.nodeId,
      ttlMs: config.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS,
      endpoint: config.endpoint,
      groupIds: [config.groupId],
      dataBearing: true,
      voting: state.votingDataBearingNodeIds.includes(this.nodeId),
      compatibility: config.compatibility,
    })
    this.nodeSessionLeaseId = session.lease.id

    this.coordinatorWatchDisposer = await coordinator.watchReplicationGroup(config.clusterId, config.groupId, next => {
      this.coordinatorState = next
      this.coordinatorAuthority = this.hasCurrentPrimaryAuthorityFor(next)
    })

    this.startCoordinatorLeaseRenewal()
    this.startControllerLoop()
  }

  async verifyPrimaryAuthority(): Promise<ReplicationGroupState> {
    const state = await this.refreshCoordinatorState()
    if (!state) {
      throw new CoordinatorError('Cannot prove primary authority without replication-group state')
    }
    if (!this.hasCurrentPrimaryAuthorityFor(state)) {
      throw new StalePrimaryError(
        'Node is not the current primary for this replication group',
        this.errorDetails(state),
      )
    }
    if (state.drainingNodeIds.includes(this.nodeId)) {
      throw new NodeDrainingError('Node is draining and cannot accept writes', this.errorDetails(state))
    }
    if (state.repairingNodeIds.includes(this.nodeId) || state.faultedNodeIds.includes(this.nodeId)) {
      throw new AuthorityError(
        'Node is not eligible to accept writes while repair or fault state is active',
        'AUTHORITY_LOST',
        this.errorDetails(state),
      )
    }
    return state
  }

  async assertInboundCoordinatorMessage(
    message: { groupId?: string; primaryTerm?: bigint },
    fromPeerId: string,
    direction: 'batch' | 'ack' | 'forward' | 'sync-request' | 'sync-data',
  ): Promise<void> {
    const config = this.config.coordinator
    if (!config) return
    const state = this.coordinatorState ?? (await this.refreshCoordinatorState())
    if (!state) {
      throw new CoordinatorError('Cannot validate coordinator message without replication-group state')
    }
    if (message.groupId !== config.groupId || message.primaryTerm !== state.primaryTerm) {
      throw new StalePrimaryError(
        `Rejected ${direction} message for stale or wrong replication group term`,
        this.errorDetails(state),
      )
    }
    if ((direction === 'batch' || direction === 'sync-data') && fromPeerId !== state.currentPrimary?.nodeId) {
      throw new StalePrimaryError(
        `Rejected ${direction} message from non-current primary '${fromPeerId}'`,
        this.errorDetails(state),
      )
    }
    if (
      (direction === 'ack' || direction === 'forward' || direction === 'sync-request') &&
      !this.hasCurrentPrimaryAuthorityFor(state)
    ) {
      throw new StalePrimaryError(
        `Rejected ${direction} message because this node is not current primary`,
        this.errorDetails(state),
      )
    }
  }

  decorateBatch(batch: ReplicationBatch): ReplicationBatch {
    return { ...batch, ...this.getCoordinatorMessageFields() }
  }

  decorateAck(ack: ReplicationAck): ReplicationAck {
    return { ...ack, ...this.getCoordinatorMessageFields() }
  }

  decorateForwardResult(result: ForwardedTransactionResult): ForwardedTransactionResult {
    return { ...result, ...this.getCoordinatorMessageFields() }
  }

  decorateSyncRequest(request: SyncRequest): SyncRequest {
    return { ...request, ...this.getCoordinatorMessageFields() }
  }

  decorateSyncBatch(batch: SyncBatch): SyncBatch {
    return { ...batch, ...this.getCoordinatorMessageFields() }
  }

  decorateSyncComplete(complete: SyncComplete): SyncComplete {
    return { ...complete, ...this.getCoordinatorMessageFields() }
  }

  decorateSyncAck(ack: SyncAck): SyncAck {
    return { ...ack, ...this.getCoordinatorMessageFields() }
  }

  resolveWriteConcern(
    wc: { level: string; timeoutMs?: number } | undefined,
  ): { level: string; timeoutMs?: number } | undefined {
    if (wc) return wc
    if (this.isCoordinatorMode()) {
      return { level: 'majority' }
    }
    return undefined
  }

  getCurrentPrimaryPeerId(): string | null {
    return this.getForwardingPrimaryPeerId()
  }

  private async canAcceptLocalWrite(): Promise<boolean> {
    if (!this.isCoordinatorMode()) {
      return this.config.topology.canWrite()
    }
    const state = await this.refreshCoordinatorState()
    if (!state) {
      throw new CoordinatorError('Cannot prove write authority without replication-group state')
    }
    if (!this.hasCurrentPrimaryAuthorityFor(state)) {
      return false
    }
    if (state.drainingNodeIds.includes(this.nodeId)) {
      throw new NodeDrainingError('Node is draining and cannot accept writes', this.errorDetails(state))
    }
    if (state.repairingNodeIds.includes(this.nodeId) || state.faultedNodeIds.includes(this.nodeId)) {
      throw new AuthorityError(
        'Node is not eligible to accept writes while repair or fault state is active',
        'AUTHORITY_LOST',
        this.errorDetails(state),
      )
    }
    return true
  }

  private throwNotCurrentPrimary(): never {
    if (this.isCoordinatorMode()) {
      throw new StalePrimaryError('This node is not the current primary', this.errorDetails(this.coordinatorState))
    }
    throw new TopologyError('This node cannot accept writes')
  }

  private async assertReadConcern(level: string | undefined): Promise<void> {
    if (!this.isCoordinatorMode()) return
    const readConcern = level ?? 'majority'
    if (readConcern === 'local') return
    const state = await this.refreshCoordinatorState()
    if (!state) {
      throw new CoordinatorError('Cannot satisfy read concern without replication-group state')
    }
    if (readConcern === 'linearizable') {
      await this.verifyPrimaryAuthority()
      return
    }
    if (readConcern === 'majority') {
      if (!state.inSyncNodeIds.includes(this.nodeId)) {
        throw new NodeNotInSyncError('Node is not in the in-sync set for majority reads', this.errorDetails(state))
      }
      if (state.drainingNodeIds.includes(this.nodeId) || state.repairingNodeIds.includes(this.nodeId)) {
        throw new ReadConcernError(
          'Node cannot serve majority reads while draining or repairing',
          this.errorDetails(state),
        )
      }
      return
    }
    throw new ReadConcernError(`Unsupported read concern '${readConcern}'`, this.errorDetails(state))
  }

  private async refreshCoordinatorState(): Promise<ReplicationGroupState | null> {
    const config = this.config.coordinator
    if (!config) return null
    const state = await config.coordinator.getReplicationGroupState(config.clusterId, config.groupId)
    this.coordinatorState = state
    this.coordinatorAuthority = state ? this.hasCurrentPrimaryAuthorityFor(state) : false
    return state
  }

  private hasCurrentPrimaryAuthorityFor(state: ReplicationGroupState): boolean {
    return state.currentPrimary?.nodeId === this.nodeId
  }

  private getCoordinatorMessageFields(): { groupId?: string; primaryTerm?: bigint } {
    const config = this.config.coordinator
    if (!config) return {}
    return {
      groupId: config.groupId,
      primaryTerm: this.coordinatorState?.primaryTerm ?? this.config.transportConfig?.primaryTerm,
    }
  }

  private getForwardingPrimaryPeerId(): string | null {
    if (this.isCoordinatorMode()) {
      const primaryId = this.coordinatorState?.currentPrimary?.nodeId
      if (primaryId && this.config.transport.peers().has(primaryId)) {
        return primaryId
      }
      return null
    }

    for (const [peerId, info] of this.config.transport.peers()) {
      if (info.role === 'primary') {
        return peerId
      }
    }
    return null
  }

  private async updateCoordinatorInSyncAfterWrite(seq: bigint, state: ReplicationGroupState): Promise<void> {
    const config = this.config.coordinator
    if (!config) return
    const ackedNodeIds = this.peerTracker.ackedConfiguredNodeIds(seq, this.nodeId, state.votingDataBearingNodeIds)
    const nextInSync = state.inSyncNodeIds.filter(nodeId => ackedNodeIds.includes(nodeId))
    if (arraysEqual(nextInSync, state.inSyncNodeIds)) {
      return
    }
    const updated = await config.coordinator.updateInSyncSet({
      clusterId: config.clusterId,
      groupId: config.groupId,
      inSyncNodeIds: nextInSync,
    })
    if (!updated) {
      throw new CoordinatorError('Failed to update in-sync set before acknowledging majority write')
    }
    this.coordinatorState = updated
  }

  private startCoordinatorLeaseRenewal(): void {
    const config = this.config.coordinator
    const leaseId = this.nodeSessionLeaseId
    if (!config || !leaseId) return
    const ttlMs = config.sessionTtlMs ?? DEFAULT_COORDINATOR_SESSION_TTL_MS
    const timer = setInterval(
      () => {
        config.coordinator
          .renewLease(leaseId, ttlMs)
          .then(renewed => {
            if (!renewed) {
              this.coordinatorAuthority = false
            }
          })
          .catch((err: unknown) => {
            this.coordinatorAuthority = false
            const wrappedErr = err instanceof Error ? err : new Error(String(err))
            this.emitError({ error: wrappedErr, operation: 'coordinator-session-renew', recoverable: false })
          })
      },
      Math.max(1_000, Math.floor(ttlMs / 3)),
    )
    this.unrefTimer(timer)
    this.coordinatorLeaseTimer = timer
  }

  private startControllerLoop(): void {
    const config = this.config.coordinator
    if (!config) return
    const controllerConfig = typeof config.controller === 'object' ? config.controller : {}
    const enabled = typeof config.controller === 'boolean' ? config.controller : (controllerConfig.enabled ?? true)
    if (!enabled) {
      this.controllerState = 'disabled'
      return
    }

    this.controllerState = 'standby'
    const ttlMs = controllerConfig.leaseTtlMs ?? DEFAULT_CONTROLLER_LEASE_TTL_MS
    const holderId = controllerConfig.holderId ?? this.nodeId
    const tickMs = controllerConfig.tickIntervalMs ?? DEFAULT_CONTROLLER_TICK_INTERVAL_MS
    const timer = setInterval(() => {
      this.controllerTick(holderId, ttlMs).catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.controllerState = 'lost'
        this.emitError({ error: wrappedErr, operation: 'coordinator-controller', recoverable: true })
      })
    }, tickMs)
    this.unrefTimer(timer)
    this.controllerTimer = timer
  }

  private async controllerTick(holderId: string, ttlMs: number): Promise<void> {
    const config = this.config.coordinator
    if (!config) return
    if (this.controllerLeaseId) {
      const renewed = await config.coordinator.renewLease(this.controllerLeaseId, ttlMs)
      if (!renewed) {
        this.controllerLeaseId = null
        this.controllerState = 'lost'
        return
      }
      this.controllerState = 'active'
      await this.runControllerPromotionCheck()
      return
    }

    const acquired = await config.coordinator.tryAcquireControllerLease({
      clusterId: config.clusterId,
      holderId,
      ttlMs,
    })
    if (acquired.acquired) {
      this.controllerLeaseId = acquired.lease.id
      this.controllerState = 'active'
      await this.runControllerPromotionCheck()
    } else {
      this.controllerState = 'standby'
    }
  }

  private async runControllerPromotionCheck(): Promise<void> {
    const config = this.config.coordinator
    if (!config) return
    const state = await this.refreshCoordinatorState()
    if (!state) return
    const primaryNodeId = state.currentPrimary?.nodeId
    const primaryLive = primaryNodeId
      ? await config.coordinator.getLiveNodeSession(config.clusterId, primaryNodeId)
      : null
    if (primaryNodeId && primaryLive) {
      return
    }
    try {
      const promoted = await config.coordinator.promoteEligibleReplica({
        clusterId: config.clusterId,
        groupId: config.groupId,
        excludeNodeIds: primaryNodeId ? [primaryNodeId] : [],
      })
      this.coordinatorState = promoted
      this.coordinatorAuthority = this.hasCurrentPrimaryAuthorityFor(promoted)
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      this.emitError({ error: wrappedErr, operation: 'coordinator-promotion', recoverable: true })
    }
  }

  private stopCoordinatorTimers(): void {
    if (this.coordinatorLeaseTimer) {
      clearInterval(this.coordinatorLeaseTimer)
      this.coordinatorLeaseTimer = null
    }
    if (this.controllerTimer) {
      clearInterval(this.controllerTimer)
      this.controllerTimer = null
    }
  }

  private async stopCoordinatorMode(): Promise<void> {
    const config = this.config.coordinator
    if (!config) return
    if (this.coordinatorWatchDisposer) {
      await this.coordinatorWatchDisposer()
      this.coordinatorWatchDisposer = null
    }
    if (this.controllerLeaseId) {
      const leaseId = this.controllerLeaseId
      this.controllerLeaseId = null
      await config.coordinator.releaseLease(leaseId).catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        this.emitError({ error: wrappedErr, operation: 'coordinator-controller-release', recoverable: true })
      })
    }
    await config.coordinator.deregisterNodeSession(config.clusterId, this.nodeId).catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      this.emitError({ error: wrappedErr, operation: 'coordinator-session-deregister', recoverable: true })
    })
  }

  private localCoordinatorPrimary(): { nodeId: string; endpoint?: string } {
    const endpoint = this.config.coordinator?.endpoint
    return endpoint ? { nodeId: this.nodeId, endpoint } : { nodeId: this.nodeId }
  }

  private unrefTimer(timer: ReturnType<typeof setInterval>): void {
    const unref = (timer as { unref?: () => void }).unref
    unref?.call(timer)
  }

  private getCoordinatorRuntimeStatus(): ReplicationStatus['coordinator'] {
    const config = this.config.coordinator
    const state = this.coordinatorState
    if (!config || !state) return undefined
    return {
      clusterId: config.clusterId,
      groupId: config.groupId,
      currentPrimary: state.currentPrimary ? { ...state.currentPrimary } : null,
      primaryTerm: state.primaryTerm,
      inSyncNodeIds: [...state.inSyncNodeIds],
      drainingNodeIds: [...state.drainingNodeIds],
      repairingNodeIds: [...state.repairingNodeIds],
      faultedNodeIds: [...state.faultedNodeIds],
      votingDataBearingNodeIds: [...state.votingDataBearingNodeIds],
      authority: this.coordinatorAuthority,
      controllerState: this.controllerState,
    }
  }

  private errorDetails(state: ReplicationGroupState | null): Record<string, unknown> {
    return {
      currentPrimary: state?.currentPrimary ?? null,
      primaryTerm: state?.primaryTerm.toString(),
      replicationGroupId: this.config.coordinator?.groupId,
    }
  }
}

function arraysEqual(left: string[], right: string[]): boolean {
  if (left.length !== right.length) return false
  for (let i = 0; i < left.length; i++) {
    if (left[i] !== right[i]) return false
  }
  return true
}
