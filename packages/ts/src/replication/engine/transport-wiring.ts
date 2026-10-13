import { HLC } from '../../core/sync/hlc.js'
import { setForeignKeysEnabled } from '../../core/system-catalog/index.js'
import { BatchValidationError, ReplicationError } from '../errors.js'
import type { NodeInfo, ReplicationBatch, SyncPhase } from '../types.js'
import { assertInboundCoordinatorMessage, resolverFor } from './coordinator-authority.js'
import { handleCoordinatorAckProgress } from './coordinator-membership.js'
import type { ReplicationEngine } from './engine.js'
import {
  acceptsBatchTerm,
  applyInAnchoredTerm,
  handleHistoryRefusal,
  isAnchoredToCurrentTerm,
} from './history-anchor.js'
import { refreshTriggersAfterDdl } from './trigger-refresh.js'

function clockLeadMs(remoteHlc: string): number {
  return HLC.decode(remoteHlc).wallMs - Date.now()
}

function replicatesFrom(engine: ReplicationEngine, peer: NodeInfo): boolean {
  if (engine.isCoordinatorMode()) {
    return engine.coordinatorState?.currentPrimary?.nodeId === peer.id
  }
  return engine.config.topology.shouldAcceptFrom(peer.id, peer.role)
}

function servesItsData(phase: SyncPhase): boolean {
  return phase === 'ready' || phase === 'catching-up'
}

async function reportAppliedPosition(engine: ReplicationEngine, peer: NodeInfo, appliedSeq: bigint): Promise<void> {
  if (appliedSeq === 0n || !replicatesFrom(engine, peer)) return
  if (!servesItsData(engine.syncState.phase) || !isAnchoredToCurrentTerm(engine)) return
  if (!servesItsData((await engine.log.getSyncState()).phase)) return
  await engine.config.transport.sendAck(
    peer.id,
    engine.decorate({ batchId: '', ackedSeq: appliedSeq, nodeId: engine.nodeId }),
  )
}

function persistAckProgress(engine: ReplicationEngine, nodeId: string, ackedSeq: bigint): void {
  engine.log
    .setLastAppliedSeq(nodeId, ackedSeq)
    .then(() => handleCoordinatorAckProgress(engine, nodeId, ackedSeq))
    .catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'ack-state-persist', peerId: nodeId, recoverable: true })
    })
}

export function wireTransportHandlers(engine: ReplicationEngine): void {
  const receiveBatch = async (batch: ReplicationBatch, fromPeerId: string): Promise<void> => {
    if (!engine.running) return
    if (engine.syncState.phase !== 'ready' && engine.syncState.phase !== 'catching-up') return

    try {
      if (batch.sourceNodeId !== fromPeerId) {
        throw new BatchValidationError(
          `Batch sourceNodeId '${batch.sourceNodeId}' does not match sender '${fromPeerId}'`,
        )
      }

      const knownPeers = engine.config.transport.peers()
      if (!knownPeers.has(fromPeerId)) {
        throw new BatchValidationError(`Batch from unknown peer: ${fromPeerId}`)
      }

      const peerInfo = knownPeers.get(fromPeerId)
      if (!peerInfo || !engine.config.topology.shouldAcceptFrom(fromPeerId, peerInfo.role)) {
        if (!engine.isCoordinatorMode()) {
          throw new ReplicationError(`Rejected batch from unauthorized peer: ${fromPeerId}`)
        }
      }

      await assertInboundCoordinatorMessage(engine, batch, fromPeerId, 'batch')
      if (!acceptsBatchTerm(engine, batch)) return

      if (batch.changes.length > engine.maxBatchChanges) {
        throw new BatchValidationError(
          `Batch too large: ${batch.changes.length} changes exceeds max ${engine.maxBatchChanges}`,
        )
      }

      const lead = clockLeadMs(batch.hlcRange.max)
      if (lead > engine.maxClockDriftMs) {
        throw new BatchValidationError(
          `Clock drift too high: the batch is stamped ${lead}ms ahead of the local clock, which exceeds max ${engine.maxClockDriftMs}ms`,
        )
      }

      const appliedSeq = await engine.log.getLastAppliedSeq(fromPeerId)
      if (batch.afterSeq !== undefined && batch.afterSeq > appliedSeq) {
        await engine.config.transport.sendAck(
          fromPeerId,
          engine.decorate({ batchId: '', ackedSeq: appliedSeq, nodeId: engine.nodeId }),
        )
        return
      }

      const applyResult = await applyInAnchoredTerm(engine, batch, () =>
        engine.log.applyBatch(batch, table => resolverFor(engine, table)),
      )
      if (applyResult === null) return

      const batchContainedDdl = batch.changes.some(c => c.operation === 'ddl')
      if (batchContainedDdl) {
        if (applyResult.droppedTables.length > 0 && engine.tracker) {
          await engine.tracker.pruneDroppedTables(engine.writerConn, applyResult.droppedTables)
        }
        await refreshTriggersAfterDdl(engine)
      }

      await engine.log.setLastAppliedSeq(fromPeerId, batch.toSeq)
      engine.peerTracker.onBatchApplied(fromPeerId, batch.hlcRange.max)

      const previousApplied = engine.appliedSeqByPeer.get(fromPeerId) ?? 0n
      if (batch.toSeq > previousApplied) {
        engine.appliedSeqByPeer.set(fromPeerId, batch.toSeq)
      }

      if (batch.toSeq > engine.highestSourceSeqSeen) {
        engine.highestSourceSeqSeen = batch.toSeq
      }

      await engine.config.transport.sendAck(
        fromPeerId,
        engine.decorate({
          batchId: batch.batchId,
          ackedSeq: batch.toSeq,
          nodeId: engine.nodeId,
        }),
      )
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'batch-received', peerId: fromPeerId, recoverable: true })
    }
  }

  const batchQueues = new Map<string, Promise<void>>()
  engine.config.transport.onBatchReceived((batch, fromPeerId) => {
    const received = (batchQueues.get(fromPeerId) ?? Promise.resolve()).then(() => receiveBatch(batch, fromPeerId))
    batchQueues.set(
      fromPeerId,
      received.catch(() => undefined),
    )
    return received
  })

  engine.config.transport.onAckReceived((ack, fromPeerId) => {
    if (!engine.running) return
    if (engine.syncState.phase !== 'ready' && engine.syncState.phase !== 'catching-up') return
    if (!engine.isCoordinatorMode()) {
      if (ack.nodeId !== fromPeerId) {
        engine.emitError({
          error: new ReplicationError(
            `Ack nodeId '${ack.nodeId}' does not match sender '${fromPeerId}'`,
            'ACK_NODE_ID_MISMATCH',
          ),
          operation: 'ack-identity-mismatch',
          peerId: fromPeerId,
          recoverable: true,
        })
        return
      }
      if (ack.batchId === '') engine.peerTracker.resumeFrom(ack.nodeId, ack.ackedSeq)
      engine.peerTracker.onAckReceived(ack.nodeId, ack.ackedSeq)
      persistAckProgress(engine, ack.nodeId, ack.ackedSeq)
      return
    }
    assertInboundCoordinatorMessage(engine, ack, fromPeerId, 'ack')
      .then(() => {
        if (ack.batchId === '') engine.peerTracker.resumeFrom(ack.nodeId, ack.ackedSeq)
        engine.peerTracker.onAckReceived(ack.nodeId, ack.ackedSeq)
        persistAckProgress(engine, ack.nodeId, ack.ackedSeq)
      })
      .catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'ack-received', peerId: fromPeerId, recoverable: true })
      })
  })

  if (engine.config.topology.canWrite() || engine.isCoordinatorMode()) {
    engine.config.transport.onForwardReceived(async (request, fromPeerId) => {
      if (engine.syncState.phase !== 'ready' && engine.syncState.phase !== 'catching-up') {
        throw new ReplicationError('Node is not ready to handle forwarded requests')
      }
      await assertInboundCoordinatorMessage(engine, request, fromPeerId, 'forward')
      const knownPeers = engine.config.transport.peers()
      if (!knownPeers.has(fromPeerId)) {
        throw new ReplicationError(`Rejected forward from unknown peer: ${fromPeerId}`)
      }
      try {
        const result = await engine.localExecutor.executeForwardedLocally(request.statements, request.writeConcern)
        return engine.decorate(result)
      } catch (err: unknown) {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'forward-execution', peerId: fromPeerId, recoverable: true })
        throw wrappedErr
      }
    })
  }

  engine.config.transport.onPeerConnected(peer => {
    engine.peerTracker.addPeer(peer.id)
    engine.log
      .getPeerAckedSeq(peer.id)
      .then(async ackedSeq => {
        if (!engine.running) return
        engine.peerTracker.onAckReceived(peer.id, ackedSeq)
        await engine.log.setLastAppliedSeq(peer.id, ackedSeq)
        await handleCoordinatorAckProgress(engine, peer.id, ackedSeq)
        await reportAppliedPosition(engine, peer, ackedSeq)
      })
      .catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'peer-state-initialise', peerId: peer.id, recoverable: true })
      })
    if (engine.syncState.phase === 'pending') {
      engine.syncJoiner.initiateSync().catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'sync-initiation', peerId: peer.id, recoverable: true })
      })
    }
  })

  engine.config.transport.onPeerDisconnected(peerId => {
    engine.peerTracker.removePeer(peerId)

    engine.syncServer.abortSessionsForPeer(peerId)

    if (engine.syncState.phase === 'syncing' && engine.syncState.sourcePeerId === peerId) {
      engine.syncState.phase = 'pending'
      engine.syncState.sourcePeerId = null
      setForeignKeysEnabled(engine.writerConn, true).catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'peer-disconnect-pragma-restore', peerId, recoverable: false })
      })
      engine.expectedBatchIndex.clear()
      engine.syncTableDigests.clear()
      engine.log.setSyncMeta('pending').catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'sync-meta-write', peerId, recoverable: false })
      })
    }
  })

  engine.config.transport.onSyncRequested(async (request, fromPeerId) => {
    if (!engine.running) return
    await assertInboundCoordinatorMessage(engine, request, fromPeerId, 'sync-request')
    await engine.syncServer.handleSyncRequest(request, fromPeerId)
  })

  engine.config.transport.onSyncBatchReceived(async (batch, fromPeerId) => {
    if (!engine.running) return
    await assertInboundCoordinatorMessage(engine, batch, fromPeerId, 'sync-data')
    await engine.syncJoiner.handleSyncBatchReceived(batch, fromPeerId)
  })

  engine.config.transport.onSyncCompleteReceived(async (complete, fromPeerId) => {
    if (!engine.running) return
    await assertInboundCoordinatorMessage(engine, complete, fromPeerId, 'sync-data')
    await engine.syncJoiner.handleSyncCompleteReceived(complete, fromPeerId)
  })

  engine.config.transport.onSyncAckReceived((ack, fromPeerId) => {
    if (!engine.running) return
    if (engine.syncJoiner.isSourceRejection(ack, fromPeerId)) {
      engine.syncJoiner.handleSourceRejection(ack, fromPeerId)
      return
    }
    if (handleHistoryRefusal(engine, ack, fromPeerId)) return
    if (!engine.isCoordinatorMode()) {
      if (ack.joinerNodeId !== fromPeerId) {
        engine.emitError({
          error: new ReplicationError(
            `SyncAck joinerNodeId '${ack.joinerNodeId}' does not match sender '${fromPeerId}'`,
            'SYNC_ACK_NODE_ID_MISMATCH',
          ),
          operation: 'sync-ack-identity-mismatch',
          peerId: fromPeerId,
          recoverable: true,
        })
        return
      }
      engine.syncServer.handleSyncAckReceived(ack)
      return
    }
    assertInboundCoordinatorMessage(engine, ack, fromPeerId, 'ack')
      .then(() => {
        engine.syncServer.handleSyncAckReceived(ack)
      })
      .catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'sync-ack-received', peerId: fromPeerId, recoverable: true })
      })
  })
}
