import { setForeignKeysEnabled } from '../../core/system-catalog/index.js'
import { BatchValidationError, ReplicationError } from '../errors.js'
import type { ReplicationEngine } from './engine.js'
import { delayAckIfConfigured } from './test-hooks.js'

function persistAckProgress(engine: ReplicationEngine, nodeId: string, ackedSeq: bigint): void {
  engine.log
    .setLastAppliedSeq(nodeId, ackedSeq)
    .then(() => engine.handleCoordinatorAckProgress(nodeId, ackedSeq))
    .catch((err: unknown) => {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'ack-state-persist', peerId: nodeId, recoverable: true })
    })
}

export function wireTransportHandlers(engine: ReplicationEngine): void {
  engine.config.transport.onBatchReceived(async (batch, fromPeerId) => {
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

      await engine.assertInboundCoordinatorMessage(batch, fromPeerId, 'batch')

      if (batch.changes.length > engine.maxBatchChanges) {
        throw new BatchValidationError(
          `Batch too large: ${batch.changes.length} changes exceeds max ${engine.maxBatchChanges}`,
        )
      }

      const drift = engine.checkClockDrift(batch.hlcRange.max)
      if (drift > engine.maxClockDriftMs) {
        throw new BatchValidationError(`Clock drift too high: ${drift}ms exceeds max ${engine.maxClockDriftMs}ms`)
      }

      const applyResult = await engine.log.applyBatch(batch, table => engine.getResolver(table))

      const batchContainedDdl = batch.changes.some(c => c.operation === 'ddl')
      if (batchContainedDdl) {
        if (applyResult.droppedTables.length > 0 && engine.tracker) {
          await engine.tracker.pruneDroppedTables(engine.writerConn, applyResult.droppedTables)
        }
        await engine.refreshTriggersAfterDdl()
      }

      await engine.log.setLastAppliedSeq(fromPeerId, batch.toSeq)

      const previousApplied = engine.appliedSeqByPeer.get(fromPeerId) ?? 0n
      if (batch.toSeq > previousApplied) {
        engine.appliedSeqByPeer.set(fromPeerId, batch.toSeq)
      }

      if (batch.toSeq > engine.highestSourceSeqSeen) {
        engine.highestSourceSeqSeen = batch.toSeq
      }

      await delayAckIfConfigured(engine)
      await engine.config.transport.sendAck(
        fromPeerId,
        engine.decorateAck({
          batchId: batch.batchId,
          ackedSeq: batch.toSeq,
          nodeId: engine.nodeId,
        }),
      )
    } catch (err: unknown) {
      const wrappedErr = err instanceof Error ? err : new Error(String(err))
      engine.emitError({ error: wrappedErr, operation: 'batch-received', peerId: fromPeerId, recoverable: true })
    }
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
      engine.peerTracker.onAckReceived(ack.nodeId, ack.ackedSeq)
      persistAckProgress(engine, ack.nodeId, ack.ackedSeq)
      return
    }
    engine
      .assertInboundCoordinatorMessage(ack, fromPeerId, 'ack')
      .then(() => {
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
      await engine.assertInboundCoordinatorMessage(request, fromPeerId, 'forward')
      const knownPeers = engine.config.transport.peers()
      if (!knownPeers.has(fromPeerId)) {
        throw new ReplicationError(`Rejected forward from unknown peer: ${fromPeerId}`)
      }
      try {
        const result = await engine.localExecutor.executeForwardedLocally(request.statements)
        return engine.decorateForwardResult(result)
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
        await engine.handleCoordinatorAckProgress(peer.id, ackedSeq)
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
    await engine.assertInboundCoordinatorMessage(request, fromPeerId, 'sync-request')
    await engine.syncServer.handleSyncRequest(request, fromPeerId)
  })

  engine.config.transport.onSyncBatchReceived(async (batch, fromPeerId) => {
    if (!engine.running) return
    await engine.assertInboundCoordinatorMessage(batch, fromPeerId, 'sync-data')
    await engine.syncJoiner.handleSyncBatchReceived(batch, fromPeerId)
  })

  engine.config.transport.onSyncCompleteReceived(async (complete, fromPeerId) => {
    if (!engine.running) return
    await engine.assertInboundCoordinatorMessage(complete, fromPeerId, 'sync-data')
    await engine.syncJoiner.handleSyncCompleteReceived(complete, fromPeerId)
  })

  engine.config.transport.onSyncAckReceived((ack, fromPeerId) => {
    if (!engine.running) return
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
    engine
      .assertInboundCoordinatorMessage(ack, fromPeerId, 'ack')
      .then(() => {
        engine.syncServer.handleSyncAckReceived(ack)
      })
      .catch((err: unknown) => {
        const wrappedErr = err instanceof Error ? err : new Error(String(err))
        engine.emitError({ error: wrappedErr, operation: 'sync-ack-received', peerId: fromPeerId, recoverable: true })
      })
  })
}
