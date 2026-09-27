import { loadPersistedHlc } from '../../core/sync/hlc-store.js'
import { selectMaxAppliedSourceSeqByNode, setForeignKeysEnabled } from '../../core/system-catalog/index.js'
import { SyncError } from '../errors.js'
import { startCoordinatorMode, stopCoordinatorMode, stopCoordinatorTimers } from './coordinator-lifecycle.js'
import { prepareCoordinatorRejoinIfNeeded, requiresCoordinatorRejoinSync } from './coordinator-membership.js'
import type { ReplicationEngine } from './engine.js'
import { wireTransportHandlers } from './transport-wiring.js'

async function loadAppliedSeqs(engine: ReplicationEngine): Promise<void> {
  for (const [nodeId, seq] of await selectMaxAppliedSourceSeqByNode(engine.writerConn)) {
    engine.appliedSeqByPeer.set(nodeId, seq)
  }
}

/**
 * Advances the engine's in-memory HLC past the highest timestamp that this
 * database stores, so that the clock stamps the next local write later than
 * every stored one. On a new database, which stores no timestamp, the function
 * changes nothing, and you can call it more than once because each call only
 * moves the clock forward.
 *
 * Code that promotes a node to primary should call it before the node's first
 * write as primary, so that every timestamp that the node issues is later than
 * the ones that it stamped in its previous role.
 */
export async function recoverHlcFromDurableState(engine: ReplicationEngine): Promise<void> {
  const persisted = await loadPersistedHlc(engine.writerConn)
  if (persisted !== null) {
    engine.hlc.receive(persisted)
  }
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
  await loadAppliedSeqs(engine)
  await startCoordinatorMode(engine)
  await prepareCoordinatorRejoinIfNeeded(engine)

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

  const isPrimary = engine.isCoordinatorMode() ? engine.coordinatorAuthority : engine.config.topology.role === 'primary'
  const syncCompleted = await engine.log.isSyncCompleted()
  const rejoinSyncRequired = requiresCoordinatorRejoinSync(engine, engine.coordinatorState)

  if (engine.initialSync && !isPrimary && (!syncCompleted || rejoinSyncRequired)) {
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
      engine.senderLoop.start()
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

  engine.senderLoop.start()
}

export async function stopEngine(engine: ReplicationEngine): Promise<void> {
  if (!engine.running) return
  engine.running = false
  stopCoordinatorTimers(engine)

  engine.syncJoiner.stopTimers()
  engine.syncServer.abortAll()

  if (engine.syncState.phase === 'syncing') {
    try {
      await setForeignKeysEnabled(engine.writerConn, true)
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'engine-stop-pragma-restore', recoverable: false })
    }
  }

  engine.senderLoop.stop()
  if (engine.tracker) {
    engine.tracker.clearPruneBoundary('replication')
  }
  await stopCoordinatorMode(engine)
  await engine.config.transport.disconnect()
}
