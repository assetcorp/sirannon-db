import { SyncError } from '../errors.js'
import type { ReplicationEngine } from './engine.js'
import { wireTransportHandlers } from './transport-wiring.js'

/**
 * Advances the engine's in-memory HLC past every timestamp persisted in this
 * database. Idempotent: callable on a fresh database (no-op) or on a
 * recovered one (advances to the max observed value).
 *
 * Reusable for future promotion paths: a node that transitions to primary
 * must also start by absorbing every HLC stamped under its previous role.
 */
export async function recoverHlcFromDurableState(engine: ReplicationEngine): Promise<void> {
  const maxObserved = await engine.log.recoverMaxObservedHlc()
  if (maxObserved === null) return
  engine.hlc.receive(maxObserved)
}

export async function startEngine(engine: ReplicationEngine): Promise<void> {
  if (engine.running) return
  engine.running = true

  await engine.log.ensureReplicationTables()
  await recoverHlcFromDurableState(engine)
  engine.lastSentSeq = await engine.log.getLocalSeq()
  engine.lastLocalSeq = engine.lastSentSeq
  await engine.loadAppliedSeqs()
  await engine.startCoordinatorMode()
  await engine.prepareCoordinatorRejoinIfNeeded()

  wireTransportHandlers(engine)
  const transportConfig = {
    ...engine.config.transportConfig,
    localRole: engine.config.topology.role,
    groupId: engine.config.coordinator?.groupId ?? engine.config.transportConfig?.groupId,
    primaryTerm: engine.coordinatorState?.primaryTerm ?? engine.config.transportConfig?.primaryTerm,
    protocolVersion:
      engine.config.coordinator?.compatibility?.protocolVersion ?? engine.config.transportConfig?.protocolVersion,
  }
  await engine.config.transport.connect(engine.nodeId, transportConfig)

  const isPrimary = engine.isCoordinatorMode()
    ? engine.hasCoordinatorWriteAuthority()
    : engine.config.topology.role === 'primary'
  const syncCompleted = await engine.log.isSyncCompleted()
  const requiresCoordinatorRejoinSync = engine.requiresCoordinatorRejoinSync()

  if (engine.initialSync && !isPrimary && (!syncCompleted || requiresCoordinatorRejoinSync)) {
    const savedState = await engine.log.getSyncState()
    if (savedState.phase === 'syncing') {
      if (!engine.tracker) {
        throw new SyncError('Initial sync requires a ChangeTracker in ReplicationConfig')
      }
      await engine.log.wipeTables(
        engine.writerConn,
        await engine.log.getTablesInFkOrder(engine.writerConn),
        engine.tracker,
      )
    }
    engine.syncState = {
      phase: 'pending',
      sourcePeerId: null,
      snapshotSeq: null,
      completedTables: [],
      totalTables: 0,
      startedAt: null,
      error: null,
    }
    await engine.log.setSyncMeta('pending')
    await engine.syncJoiner.initiateSync()
    return
  }

  if (engine.initialSync && !isPrimary && syncCompleted) {
    const savedState = await engine.log.getSyncState()
    if (savedState.phase === 'catching-up') {
      engine.syncState = {
        phase: 'catching-up',
        sourcePeerId: savedState.sourcePeerId,
        snapshotSeq: savedState.snapshotSeq,
        completedTables: [],
        totalTables: 0,
        startedAt: null,
        error: null,
      }
      engine.startSenderLoop()
      engine.syncJoiner.startCatchUpCheck()
      return
    }
    engine.syncState.phase = 'ready'
  }

  if (!engine.initialSync && engine.resumeFromSeq !== undefined) {
    engine.lastSentSeq = engine.resumeFromSeq
    await engine.log.setSyncMeta('ready', engine.resumeFromSeq)
    engine.syncState.phase = 'ready'
  } else if (!engine.initialSync && !syncCompleted) {
    const localSeq = await engine.log.getLocalSeq()
    if (localSeq > 0n) {
      await engine.log.setSyncMeta('ready', localSeq)
    }
    engine.syncState.phase = 'ready'
  } else {
    engine.syncState.phase = 'ready'
  }

  engine.startSenderLoop()
}
