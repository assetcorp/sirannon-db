import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import type { ReplicationEngine } from '../../replication/engine.js'
import type { ReplicationConfig, ReplicationErrorEvent, SyncPhase } from '../../replication/types.js'
import {
  attachDiagnostics,
  compareDatabases,
  createMtlsCerts,
  createPrimary,
  createReplica,
  type DiagnosticsHandle,
  type ManagedNode,
  type MtlsCerts,
  type NodeStorage,
  stopNode,
  waitForPeerConnected,
  waitForReady,
  waitForReplica,
} from './lib/index.js'

const PRIMARY_ID = 'primary-chaos-aaaa'
const REPLICA_ID = 'replica-chaos-bbbb'

const CHAOS_SCHEMA = `
  CREATE TABLE IF NOT EXISTS chaos_items (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    note TEXT,
    payload BLOB
  );

  CREATE TABLE IF NOT EXISTS chaos_events (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    FOREIGN KEY (item_id) REFERENCES chaos_items(id)
  )
`

type DebugAckEngine = ReplicationEngine & {
  __setAckDelayMs(ms: number): void
}

describe('E2E chaos: gRPC replication over mTLS', () => {
  let certs: MtlsCerts
  let primary: ManagedNode | null = null
  let replica: ManagedNode | null = null
  let diagnostics: DiagnosticsHandle | null = null

  beforeAll(async () => {
    certs = await createMtlsCerts([PRIMARY_ID, REPLICA_ID])
  })

  afterAll(() => {
    certs?.cleanup()
  })

  afterEach(async ctx => {
    diagnostics?.dumpIfFailed(ctx)
    if (replica) await stopNode(replica)
    if (primary) await stopNode(primary)
    diagnostics?.cleanup()
    primary = null
    replica = null
    diagnostics = null
  })

  it('recovers when the replication stream drops before an ACK completes', async () => {
    const pair = await startPair({
      primaryConfig: {
        batchIntervalMs: 10,
        batchSize: 1,
        maxPendingBatches: 1,
        ackTimeoutMs: 150,
      },
      replicaConfig: {
        batchIntervalMs: 10,
      },
    })
    primary = pair.primary
    replica = pair.replica

    setAckDelay(replica.engine, 500)
    await insertItem(primary, 1)

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)

    await replica.transport.disconnect()
    await sleep(600)

    setAckDelay(replica.engine, 0)
    await reconnectReplica(replica, primary)
    await waitForPeerAck(primary.engine, replica.nodeId, targetSeq, 10_000)
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)
    expectNoFatalErrors(primary, replica)
  })

  it('recovers a replica that restarts during initial sync', async () => {
    primary = await startPrimary()
    for (let i = 1; i <= 80; i++) {
      await insertItem(primary, i)
    }

    replica = await startReplica(primary.port, undefined, {
      syncBatchSize: 1,
      catchUpDeadlineMs: 15_000,
    })
    diagnostics = attachDiagnostics(primary, replica)

    setAckDelay(replica.engine, 100)
    await waitForSyncPhase(replica.engine, 'syncing', 10_000)

    const replicaStorage = storageFor(replica)
    await stopNode(replica)
    replica = null

    replica = await startReplica(primary.port, replicaStorage, {
      syncBatchSize: 5,
      catchUpDeadlineMs: 15_000,
    })
    diagnostics = attachDiagnostics(primary, replica)

    await waitForReady(replica.engine, 20_000)
    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)
    expectNoFatalErrors(primary, replica)
  })

  it('recovers when the primary stops and restarts from durable state', async () => {
    const pair = await startPair()
    primary = pair.primary
    replica = pair.replica

    await insertItem(primary, 1)
    await insertItem(primary, 2)

    let targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)

    const primaryStorage = storageFor(primary)
    const primaryPort = primary.port

    await stopNode(primary)
    primary = null
    await replica.transport.disconnect()

    primary = await startPrimary(primaryStorage, primaryPort)
    diagnostics = attachDiagnostics(primary, replica)
    await reconnectReplica(replica, primary)

    await insertItem(primary, 3)
    await primary.engine.execute('UPDATE chaos_items SET version = version + 1, note = ? WHERE id = ?', [
      'after-primary-restart',
      1,
    ])

    targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)
    expectNoFatalErrors(primary, replica)
  })

  it('keeps CDC pruning behind slow replica ACKs', async () => {
    const pair = await startPair({
      primaryConfig: {
        batchIntervalMs: 10,
        batchSize: 1,
        maxPendingBatches: 1,
        ackTimeoutMs: 5_000,
      },
      replicaConfig: {
        batchIntervalMs: 10,
      },
    })
    primary = pair.primary
    replica = pair.replica

    setAckDelay(replica.engine, 500)

    await insertItem(primary, 1)
    await insertItem(primary, 2)
    await waitForReplica(replica.engine, primary.nodeId, 1n, 10_000)

    await markChangesStale(primary.conn)

    const preAckDeleted = await primary.tracker.cleanup(primary.conn)
    expect(preAckDeleted).toBe(0)
    expect(await changeCount(primary.conn)).toBe(2)

    await waitForPeerAck(primary.engine, replica.nodeId, 1n, 10_000)
    await waitForCleanupDeletion(primary.tracker, primary.conn, 1, 10_000)
    expect(await changeCount(primary.conn)).toBe(1)

    setAckDelay(replica.engine, 0)

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await waitForPeerAck(primary.engine, replica.nodeId, targetSeq, 10_000)
    await waitForCleanupDeletion(primary.tracker, primary.conn, 1, 10_000)
    expect(await changeCount(primary.conn)).toBe(0)
    await compareDatabases(primary.conn, replica.conn)
    expectNoFatalErrors(primary, replica)
  })

  async function startPair(
    args: { primaryConfig?: Partial<ReplicationConfig>; replicaConfig?: Partial<ReplicationConfig> } = {},
  ): Promise<{ primary: ManagedNode; replica: ManagedNode }> {
    const primaryNode = await startPrimary(undefined, undefined, args.primaryConfig)
    const replicaNode = await startReplica(primaryNode.port, undefined, args.replicaConfig)
    diagnostics = attachDiagnostics(primaryNode, replicaNode)

    await waitForReady(replicaNode.engine, 20_000)
    await waitForPeerState(primaryNode.conn, replicaNode.nodeId, 0n, 10_000)

    return { primary: primaryNode, replica: replicaNode }
  }

  async function startPrimary(
    storage?: NodeStorage,
    listenPort?: number,
    configOverrides?: Partial<ReplicationConfig>,
  ): Promise<ManagedNode> {
    return createPrimary({
      nodeId: PRIMARY_ID,
      certs,
      storage,
      listenPort,
      initialize: initializePrimary,
      configOverrides,
    })
  }

  async function startReplica(
    primaryPort: number,
    storage?: NodeStorage,
    configOverrides?: Partial<ReplicationConfig>,
  ): Promise<ManagedNode> {
    return createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: 'localhost',
      primaryPort,
      storage,
      configOverrides,
    })
  }
})

async function initializePrimary(conn: SQLiteConnection, tracker: ChangeTracker): Promise<void> {
  await conn.exec(CHAOS_SCHEMA)
  await tracker.watch(conn, 'chaos_items')
  await tracker.watch(conn, 'chaos_events')
}

async function insertItem(node: ManagedNode, id: number): Promise<void> {
  await node.engine.execute('INSERT INTO chaos_items (id, name, version, note, payload) VALUES (?, ?, ?, ?, ?)', [
    id,
    `item-${id}`,
    1,
    `note-${id}`,
    Buffer.from(`payload-${id}`),
  ])
}

async function reconnectReplica(node: ManagedNode, primaryNode: ManagedNode): Promise<void> {
  await node.transport.connect(node.nodeId, {
    localRole: 'replica',
    endpoints: [`localhost:${primaryNode.port}`],
  })
  await waitForPeerConnected(node.engine, primaryNode.nodeId, 10_000)
  await waitForPeerConnected(primaryNode.engine, node.nodeId, 10_000)
  await waitForPeerState(primaryNode.conn, node.nodeId, 0n, 10_000)
}

async function waitForPeerAck(
  engine: ReplicationEngine,
  peerNodeId: string,
  targetSeq: bigint,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => {
    const peer = engine.status().peers.find(p => p.nodeId === peerNodeId)
    return peer !== undefined && peer.lastAckedSeq >= targetSeq
  }, timeoutMs)
}

async function waitForPeerState(
  conn: SQLiteConnection,
  peerNodeId: string,
  targetSeq: bigint,
  timeoutMs: number,
): Promise<void> {
  const stmt = await conn.prepare('SELECT last_acked_seq FROM _sirannon_peer_state WHERE peer_node_id = ?')
  await waitForCondition(async () => {
    const row = (await stmt.get(peerNodeId)) as { last_acked_seq: number | string | null } | undefined
    if (!row || row.last_acked_seq === null) {
      return false
    }
    return BigInt(row.last_acked_seq) >= targetSeq
  }, timeoutMs)
}

async function waitForSyncPhase(engine: ReplicationEngine, phase: SyncPhase, timeoutMs: number): Promise<void> {
  await waitForCondition(async () => engine.status().syncState?.phase === phase, timeoutMs)
}

async function waitForCleanupDeletion(
  tracker: ChangeTracker,
  conn: SQLiteConnection,
  expectedDeleted: number,
  timeoutMs: number,
): Promise<void> {
  await waitForCondition(async () => {
    const deleted = await tracker.cleanup(conn)
    if (deleted > 0 && deleted !== expectedDeleted) {
      throw new Error(`Expected cleanup to delete ${expectedDeleted} row(s), deleted ${deleted}`)
    }
    return deleted === expectedDeleted
  }, timeoutMs)
}

async function waitForCondition(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastError: Error | null = null

  while (Date.now() < deadline) {
    try {
      if (await predicate()) {
        return
      }
    } catch (err: unknown) {
      lastError = err instanceof Error ? err : new Error(String(err))
    }
    await sleep(20)
  }

  if (lastError) {
    throw lastError
  }
  throw new Error(`Condition not met within ${timeoutMs}ms`)
}

async function markChangesStale(conn: SQLiteConnection): Promise<void> {
  const staleTimestamp = Date.now() / 1000 - 7_200
  const stmt = await conn.prepare('UPDATE _sirannon_changes SET changed_at = ?')
  await stmt.run(staleTimestamp)
}

async function changeCount(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('SELECT COUNT(*) AS count FROM _sirannon_changes')
  const row = (await stmt.get()) as { count: number }
  return row.count
}

function setAckDelay(engine: ReplicationEngine, ms: number): void {
  const debugEngine = engine as DebugAckEngine
  debugEngine.__setAckDelayMs(ms)
}

function storageFor(node: ManagedNode): NodeStorage {
  return {
    tempDir: node.tempDir,
    dbPath: node.dbPath,
  }
}

function expectNoFatalErrors(...nodes: ManagedNode[]): void {
  const fatalErrors: string[] = []

  for (const node of nodes) {
    for (const event of node.recentErrors) {
      if (!event.recoverable) {
        fatalErrors.push(formatError(node, event))
      }
    }
  }

  expect(fatalErrors).toEqual([])
}

function formatError(node: ManagedNode, event: ReplicationErrorEvent): string {
  const peer = event.peerId ? ` peer=${event.peerId}` : ''
  return `${node.nodeId} op=${event.operation}${peer} message=${event.error.message}`
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timer.unref?.()
  })
}
