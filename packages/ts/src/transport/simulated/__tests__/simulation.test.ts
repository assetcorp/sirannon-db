import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { ReplicationEngine } from '../../../replication/engine.js'
import { RaftNode } from '../../../replication/raft/raft-node.js'
import { MultiPrimaryTopology } from '../../../replication/topology/multi-primary.js'
import type { ReplicationConfig } from '../../../replication/types.js'
import { ConvergenceOracle } from '../convergence.js'
import { FaultPolicy } from '../fault-policy.js'
import { SimulatedNetwork } from '../index.js'
import { SeededPRNG } from '../prng.js'
import { DeterministicScheduler } from '../scheduler.js'

const T0 = 1_000_000_000
const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'
const NODE_C = 'cccc0000cccc0000cccc0000cccc0000'

interface SimNodeContext {
  db: Database
  conn: SQLiteConnection
  engine: ReplicationEngine
  nodeId: string
}

describe('Simulation Scenarios', () => {
  let tempDir: string
  let scheduler: DeterministicScheduler
  let network: SimulatedNetwork
  let prng: SeededPRNG
  let oracle: ConvergenceOracle

  const openDbs: Database[] = []
  const openConns: SQLiteConnection[] = []
  const runningEngines: ReplicationEngine[] = []

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0)
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sim-'))
    prng = new SeededPRNG(42)
    scheduler = new DeterministicScheduler(T0)
    network = new SimulatedNetwork(scheduler, new FaultPolicy({}, prng))
    oracle = new ConvergenceOracle()
  })

  afterEach(async () => {
    for (const engine of runningEngines) {
      try {
        await engine.stop()
      } catch {
        /* best-effort */
      }
    }
    runningEngines.length = 0

    scheduler.dispose()

    for (const db of openDbs) {
      try {
        if (!db.closed) await db.close()
      } catch {
        /* best-effort */
      }
    }
    openDbs.length = 0

    for (const conn of openConns) {
      try {
        await conn.close()
      } catch {
        /* best-effort */
      }
    }
    openConns.length = 0

    vi.useRealTimers()
    rmSync(tempDir, { recursive: true, force: true })
  })

  async function createSimNode(nodeId: string, overrides: Partial<ReplicationConfig> = {}): Promise<SimNodeContext> {
    const dbPath = join(tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    openConns.push(conn)

    const tracker = new ChangeTracker({ replication: true })
    await conn.exec(`
      CREATE TABLE items (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL DEFAULT 0
      )
    `)
    await tracker.watch(conn, 'items')

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    openDbs.push(db)

    const transport = network.createTransport()
    const config: ReplicationConfig = {
      nodeId,
      topology: new MultiPrimaryTopology(),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      initialSync: false,
      ...overrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    runningEngines.push(engine)

    return { db, conn, engine, nodeId }
  }

  async function queryItems(conn: SQLiteConnection): Promise<Array<{ id: number; name: string; value: number }>> {
    const stmt = await conn.prepare('SELECT id, name, value FROM items ORDER BY id')
    return stmt.all() as Promise<Array<{ id: number; name: string; value: number }>>
  }

  it('converges concurrent multi-primary writes via LWW', async () => {
    const nodeA = await createSimNode(NODE_A)
    const nodeB = await createSimNode(NODE_B)

    await nodeA.engine.start()
    await nodeB.engine.start()

    await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'from-A', 100)")
    await scheduler.advanceBy(50)
    await nodeB.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'from-B', 200)")

    await scheduler.runUntilQuiet()

    await oracle.assertConverged(
      [
        { nodeId: NODE_A, conn: nodeA.conn },
        { nodeId: NODE_B, conn: nodeB.conn },
      ],
      ['items'],
    )

    const itemsA = await queryItems(nodeA.conn)
    const itemsB = await queryItems(nodeB.conn)
    expect(itemsA).toEqual(itemsB)
    expect(itemsA).toHaveLength(1)
    expect(itemsA[0].name).toBe('from-B')
    expect(itemsA[0].value).toBe(200)
  })

  it('recovers convergence after a fault-policy network partition heals', async () => {
    const nodeA = await createSimNode(NODE_A, { ackTimeoutMs: 200 })
    const nodeB = await createSimNode(NODE_B, { ackTimeoutMs: 200 })

    await nodeA.engine.start()
    await nodeB.engine.start()

    await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'initial', 10)")
    await scheduler.runUntilQuiet()
    await oracle.assertConverged(
      [
        { nodeId: NODE_A, conn: nodeA.conn },
        { nodeId: NODE_B, conn: nodeB.conn },
      ],
      ['items'],
    )

    network.policy.addPartition(NODE_A, NODE_B)

    await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (2, 'from-A-during-partition', 20)")
    await nodeB.engine.execute("INSERT INTO items (id, name, value) VALUES (3, 'from-B-during-partition', 30)")
    await scheduler.runUntilQuiet()

    const duringPartitionA = await queryItems(nodeA.conn)
    const duringPartitionB = await queryItems(nodeB.conn)
    expect(duringPartitionA).toHaveLength(2)
    expect(duringPartitionB).toHaveLength(2)
    expect(duringPartitionA.map(r => r.id)).toEqual([1, 2])
    expect(duringPartitionB.map(r => r.id)).toEqual([1, 3])

    network.policy.removePartition(NODE_A, NODE_B)
    await scheduler.runUntilQuiet(10000, 5, 30)

    await oracle.assertConverged(
      [
        { nodeId: NODE_A, conn: nodeA.conn },
        { nodeId: NODE_B, conn: nodeB.conn },
      ],
      ['items'],
    )

    const finalA = await queryItems(nodeA.conn)
    expect(finalA).toHaveLength(3)
    expect(finalA.map(r => r.id)).toEqual([1, 2, 3])
  })

  it('retries delivery after silent batch drop', async () => {
    const nodeA = await createSimNode(NODE_A, { ackTimeoutMs: 150 })
    const nodeB = await createSimNode(NODE_B)

    await nodeA.engine.start()
    await nodeB.engine.start()

    await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'should-arrive', 42)")

    network.policy.setDropRate(1.0)
    await scheduler.runUntilQuiet(2000, 3, 30)

    const itemsBDuring = await queryItems(nodeB.conn)
    expect(itemsBDuring).toHaveLength(0)

    network.policy.setDropRate(0)
    await scheduler.runUntilQuiet(10000, 5, 30)

    await oracle.assertConverged(
      [
        { nodeId: NODE_A, conn: nodeA.conn },
        { nodeId: NODE_B, conn: nodeB.conn },
      ],
      ['items'],
    )

    const itemsBAfter = await queryItems(nodeB.conn)
    expect(itemsBAfter).toHaveLength(1)
    expect(itemsBAfter[0].name).toBe('should-arrive')
  })

  it('converges after ack loss using ack timeout retry', async () => {
    const nodeA = await createSimNode(NODE_A, { ackTimeoutMs: 200 })
    const nodeB = await createSimNode(NODE_B, { ackTimeoutMs: 200 })

    await nodeA.engine.start()
    await nodeB.engine.start()

    for (let i = 1; i <= 10; i++) {
      await nodeA.engine.execute(`INSERT INTO items (id, name, value) VALUES (${i}, 'item-${i}', ${i * 10})`)
    }

    await scheduler.advanceBy(31)
    network.policy.setDropRate(0.3)
    await scheduler.runUntilQuiet(10000, 5, 30)

    network.policy.setDropRate(0)
    await scheduler.runUntilQuiet(10000, 5, 30)

    await oracle.assertConverged(
      [
        { nodeId: NODE_A, conn: nodeA.conn },
        { nodeId: NODE_B, conn: nodeB.conn },
      ],
      ['items'],
    )

    const itemsB = await queryItems(nodeB.conn)
    expect(itemsB).toHaveLength(10)
  })

  it('elects a deterministic Raft leader with seeded randomness', async () => {
    async function runElection(seed: number): Promise<string | null> {
      vi.setSystemTime(T0)

      const electionPrng = new SeededPRNG(seed)
      const electionScheduler = new DeterministicScheduler(T0)
      const electionPolicy = new FaultPolicy({ latencyMin: 1, latencyMax: 3 }, electionPrng)
      const electionNetwork = new SimulatedNetwork(electionScheduler, electionPolicy)

      const raftRandomFn = () => electionPrng.next()

      const tA = electionNetwork.createTransport()
      const tB = electionNetwork.createTransport()
      const tC = electionNetwork.createTransport()

      await tA.connect(NODE_A, {})
      await tB.connect(NODE_B, {})
      await tC.connect(NODE_C, {})

      const raftConfig = {
        electionTimeoutMin: 100,
        electionTimeoutMax: 200,
        heartbeatInterval: 40,
        randomFn: raftRandomFn,
      }

      const raftA = new RaftNode(NODE_A, tA, raftConfig)
      const raftB = new RaftNode(NODE_B, tB, raftConfig)
      const raftC = new RaftNode(NODE_C, tC, raftConfig)

      raftA.start()
      raftB.start()
      raftC.start()

      await electionScheduler.advanceBy(250)
      await electionScheduler.runUntilQuiet(2000, 3, 40)

      const states = [
        { node: NODE_A, state: raftA.state, leader: raftA.leaderId },
        { node: NODE_B, state: raftB.state, leader: raftB.leaderId },
        { node: NODE_C, state: raftC.state, leader: raftC.leaderId },
      ]

      const leaders = states.filter(s => s.state === 'leader')

      raftA.stop()
      raftB.stop()
      raftC.stop()
      electionScheduler.dispose()

      expect(leaders.length).toBe(1)

      const followers = states.filter(s => s.state === 'follower')
      expect(followers.length).toBe(2)

      return leaders[0].node
    }

    const leader1 = await runElection(99)
    const leader2 = await runElection(99)
    expect(leader1).toBe(leader2)

    const leader3 = await runElection(7777)
    expect(leader3).not.toBeNull()
  })
})
