import type { ReplicationEngine } from '../../../replication/engine.js'

const DEFAULT_POLL_INTERVAL_MS = 20

export interface WaitForReplicaOptions {
  pollIntervalMs?: number
}

export async function waitForReplica(
  replica: ReplicationEngine,
  primaryNodeId: string,
  targetSeq: bigint,
  timeoutMs: number,
  options: WaitForReplicaOptions = {},
): Promise<void> {
  const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
  const deadline = Date.now() + timeoutMs

  while (true) {
    const applied = replica.getAppliedSeq(primaryNodeId)
    if (applied >= targetSeq) {
      return
    }

    if (Date.now() >= deadline) {
      const lag = targetSeq - applied
      throw new Error(
        `waitForReplica timed out after ${timeoutMs}ms: expected ${targetSeq}, got ${applied}, lag ${lag} for peer ${primaryNodeId}`,
      )
    }

    await sleep(pollIntervalMs)
  }
}

export async function waitForReady(replica: ReplicationEngine, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs

  while (true) {
    const status = replica.status()
    if (status.syncState?.phase === 'ready') {
      return
    }

    if (Date.now() >= deadline) {
      throw new Error(
        `waitForReady timed out after ${timeoutMs}ms: phase=${status.syncState?.phase ?? 'unknown'}, peers=${status.peers.length}`,
      )
    }

    await sleep(DEFAULT_POLL_INTERVAL_MS)
  }
}

export async function waitForPeerConnected(
  engine: ReplicationEngine,
  peerNodeId: string,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs

  while (true) {
    const status = engine.status()
    if (status.peers.some(p => p.nodeId === peerNodeId)) {
      return
    }

    if (Date.now() >= deadline) {
      throw new Error(`waitForPeerConnected timed out after ${timeoutMs}ms waiting for peer ${peerNodeId}`)
    }

    await sleep(DEFAULT_POLL_INTERVAL_MS)
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    const t = setTimeout(resolve, ms)
    t.unref()
  })
}
