import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  PrimaryReplicaTopology,
  type ReplicationConfig,
  ReplicationEngine,
  type ReplicationErrorEvent,
  WriteConcernError,
} from '../../../replication/index.js'
import {
  DeterministicScheduler,
  type EventKind,
  FaultPolicy,
  SeededPRNG,
  SimulatedNetwork,
} from '../../../transport/simulated/index.js'

const T0 = Date.UTC(2026, 0, 15, 9)
const PRIMARY = 'primary-ledger-east'
const REPLICA = 'replica-ledger-west'
const SECOND_REPLICA = 'replica-ledger-north'
const ACK_TIMEOUT_MS = 300
const FORWARD_TIMEOUT_MS = 2_000
const IDLE_STEP_MS = 10
const DEFAULT_LIMIT_MS = 30_000

const LEDGER_SCHEMA = 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, account TEXT NOT NULL, cents INTEGER NOT NULL)'
const INSERT_ENTRY = 'INSERT INTO ledger (id, account, cents) VALUES (?, ?, ?)'

interface Entry {
  id: number
  account: string
  cents: number
}

interface SimNode {
  nodeId: string
  dbPath: string
  engine: ReplicationEngine
  errors: ReplicationErrorEvent[]
}

interface OpenNode {
  engine: ReplicationEngine
  conn: SQLiteConnection
  sirannon: Sirannon
}

class LinkFaults extends FaultPolicy {
  private readonly counts = new Map<string, number>()
  private readonly lostKinds = new Set<EventKind>()
  private readonly lostOrdinals = new Map<string, Set<number>>()

  shouldDrop(from: string, to: string, kind: EventKind): boolean {
    const key = linkKey(from, to, kind)
    const ordinal = (this.counts.get(key) ?? 0) + 1
    this.counts.set(key, ordinal)
    if (this.lostKinds.has(kind)) return true
    if (this.lostOrdinals.get(key)?.has(ordinal)) return true
    return super.shouldDrop(from, to, kind)
  }

  sent(from: string, to: string, kind: EventKind): number {
    return this.counts.get(linkKey(from, to, kind)) ?? 0
  }

  lose(kind: EventKind): void {
    this.lostKinds.add(kind)
  }

  restore(kind: EventKind): void {
    this.lostKinds.delete(kind)
  }

  loseNext(from: string, to: string, kind: EventKind): void {
    const key = linkKey(from, to, kind)
    const ordinals = this.lostOrdinals.get(key) ?? new Set<number>()
    ordinals.add(this.sent(from, to, kind) + 1)
    this.lostOrdinals.set(key, ordinals)
  }
}

function linkKey(from: string, to: string, kind: EventKind): string {
  return `${from}>${to}:${kind}`
}

function entry(id: number, account: string, cents: number): Entry {
  return { id, account, cents }
}

function ledgerRows(path: string): Entry[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    const table = raw.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ledger'").get()
    if (table === undefined) return []
    return raw.prepare('SELECT id, account, cents FROM ledger ORDER BY id').all() as Entry[]
  } finally {
    raw.close()
  }
}

function sortedEntries(expected: Map<number, Entry>): Entry[] {
  return [...expected.values()].sort((left, right) => left.id - right.id)
}

describe('replication over a simulated network that loses, delays, and partitions messages', () => {
  let directory: string
  let scheduler: DeterministicScheduler
  let faults: LinkFaults
  let network: SimulatedNetwork
  const open: OpenNode[] = []

  beforeEach(() => {
    vi.useFakeTimers({ now: T0 })
    directory = mkdtempSync(join(tmpdir(), 'sirannon-simulated-'))
    scheduler = new DeterministicScheduler(T0)
    faults = new LinkFaults({}, new SeededPRNG(7))
    network = new SimulatedNetwork(scheduler, faults, FORWARD_TIMEOUT_MS)
  })

  afterEach(async () => {
    for (const node of open) await node.engine.stop().catch(() => {})
    scheduler.dispose()
    for (const node of open) {
      await node.sirannon.shutdown().catch(() => {})
      await node.conn.close().catch(() => {})
    }
    open.length = 0
    vi.useRealTimers()
    rmSync(directory, { recursive: true, force: true })
  })

  async function until(condition: () => boolean, label: string, limitMs = DEFAULT_LIMIT_MS): Promise<void> {
    const deadline = Date.now() + limitMs
    while (!condition()) {
      if (Date.now() >= deadline) throw new Error(`Timed out after ${limitMs} simulated ms waiting for ${label}`)
      if (!(await scheduler.step())) await scheduler.advanceBy(IDLE_STEP_MS)
    }
  }

  async function pass(ms: number): Promise<void> {
    const deadline = Date.now() + ms
    await until(() => Date.now() >= deadline, `${ms} simulated ms to pass`, ms + IDLE_STEP_MS)
  }

  async function settle<T>(work: Promise<T>): Promise<T | Error> {
    let outcome: { value: T } | { error: Error } | undefined
    work.then(
      value => {
        outcome = { value }
      },
      (error: unknown) => {
        outcome = { error: error instanceof Error ? error : new Error(String(error)) }
      },
    )
    await until(() => outcome !== undefined, 'the operation to settle')
    if (outcome === undefined) throw new Error('The operation did not settle')
    return 'error' in outcome ? outcome.error : outcome.value
  }

  async function startNode(
    nodeId: string,
    role: 'primary' | 'replica',
    overrides: Partial<ReplicationConfig> = {},
  ): Promise<SimNode> {
    const dbPath = join(directory, `${nodeId}.db`)
    const driver = betterSqlite3()
    const conn = await driver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    const tracker = new ChangeTracker()
    if (role === 'primary') {
      await conn.exec(LEDGER_SCHEMA)
      await tracker.watch(conn, 'ledger')
    }
    const sirannon = new Sirannon({ driver })
    const db = await sirannon.open(nodeId, dbPath)
    const engine = new ReplicationEngine(db, conn, {
      nodeId,
      topology: new PrimaryReplicaTopology(role),
      transport: network.createTransport(),
      batchIntervalMs: 30,
      batchSize: 100,
      ackTimeoutMs: ACK_TIMEOUT_MS,
      initialSync: role === 'replica',
      changeTracker: tracker,
      snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }),
      syncAckTimeoutMs: 2_000,
      catchUpDeadlineMs: 2_000,
      writeForwarding: role === 'replica',
      ...overrides,
    })
    const errors: ReplicationErrorEvent[] = []
    engine.on('replication-error', (event: ReplicationErrorEvent) => errors.push(event))
    open.push({ engine, conn, sirannon })
    await engine.start()
    return { nodeId, dbPath, engine, errors }
  }

  async function cluster(
    replicaIds: readonly string[],
    overrides: Partial<ReplicationConfig> = {},
  ): Promise<{ primary: SimNode; replicas: SimNode[] }> {
    const primary = await startNode(PRIMARY, 'primary', overrides)
    const replicas: SimNode[] = []
    for (const nodeId of replicaIds) replicas.push(await startNode(nodeId, 'replica', overrides))
    for (const replica of replicas) {
      await until(() => replica.engine.status().syncState?.phase === 'ready', `${replica.nodeId} to finish its copy`)
    }
    return { primary, replicas }
  }

  async function write(primary: SimNode, expected: Map<number, Entry>, row: Entry): Promise<void> {
    await settle(primary.engine.execute(INSERT_ENTRY, [row.id, row.account, row.cents]))
    expected.set(row.id, row)
  }

  async function holds(node: SimNode, expected: Map<number, Entry>, limitMs = DEFAULT_LIMIT_MS): Promise<void> {
    const want = JSON.stringify(sortedEntries(expected))
    await until(() => JSON.stringify(ledgerRows(node.dbPath)) === want, `${node.nodeId} to hold the ledger`, limitMs)
    expect(ledgerRows(node.dbPath)).toEqual(sortedEntries(expected))
  }

  function unrecoverable(node: SimNode): ReplicationErrorEvent[] {
    return node.errors.filter(event => !event.recoverable)
  }

  it('holds a partitioned replica at its last rows while the other follows, then brings it level once the partition heals', async () => {
    const { primary, replicas } = await cluster([REPLICA, SECOND_REPLICA])
    const [cutOff, connected] = replicas
    const expected = new Map<number, Entry>()
    for (let id = 1; id <= 5; id++) await write(primary, expected, entry(id, `acct-${id}`, id * 100))
    await holds(cutOff, expected)
    const beforePartition = ledgerRows(cutOff.dbPath)

    faults.addPartition(PRIMARY, REPLICA)
    for (let id = 6; id <= 10; id++) await write(primary, expected, entry(id, `acct-${id}`, id * 100))
    await settle(primary.engine.execute('UPDATE ledger SET cents = ? WHERE id = ?', [9_999, 2]))
    expected.set(2, entry(2, 'acct-2', 9_999))
    await settle(primary.engine.execute('DELETE FROM ledger WHERE id = ?', [3]))
    expected.delete(3)
    await holds(connected, expected)
    await pass(ACK_TIMEOUT_MS * 4)
    expect(ledgerRows(cutOff.dbPath)).toEqual(beforePartition)

    faults.removePartition(PRIMARY, REPLICA)
    await holds(cutOff, expected)
    for (const node of [primary, cutOff, connected]) expect(unrecoverable(node)).toEqual([])
  })

  it('keeps the replica equal to the primary while every acknowledgement is lost and the primary resends each batch', async () => {
    const { primary, replicas } = await cluster([REPLICA])
    const [replica] = replicas
    const expected = new Map<number, Entry>()
    faults.lose('ack')
    const sentBefore = faults.sent(PRIMARY, REPLICA, 'batch')

    for (let id = 1; id <= 6; id++) await write(primary, expected, entry(id, `acct-${id}`, id))
    await holds(replica, expected)
    const sentOnceHeld = faults.sent(PRIMARY, REPLICA, 'batch')
    await pass(ACK_TIMEOUT_MS * 3)

    expect(sentOnceHeld).toBeGreaterThan(sentBefore)
    expect(faults.sent(PRIMARY, REPLICA, 'batch')).toBeGreaterThan(sentOnceHeld)
    expect(ledgerRows(replica.dbPath)).toEqual(sortedEntries(expected))
    expect(primary.engine.status().peers.find(peer => peer.nodeId === REPLICA)?.lastAckedSeq).toBe(0n)
  })

  it('fails a majority write while acknowledgements are lost, though the replica holds the row, and meets the next one once they return', async () => {
    const { primary, replicas } = await cluster([REPLICA])
    const [replica] = replicas
    faults.lose('ack')

    const unconfirmed = await settle(
      primary.engine.execute(INSERT_ENTRY, [1, 'acct-1', 10], { writeConcern: { level: 'majority', timeoutMs: 500 } }),
    )
    expect(unconfirmed).toBeInstanceOf(WriteConcernError)
    await holds(replica, new Map([[1, entry(1, 'acct-1', 10)]]))

    faults.restore('ack')
    const confirmed = await settle(
      primary.engine.execute(INSERT_ENTRY, [2, 'acct-2', 20], {
        writeConcern: { level: 'majority', timeoutMs: 5_000 },
      }),
    )
    expect(confirmed).not.toBeInstanceOf(Error)
    expect(ledgerRows(replica.dbPath)).toEqual([entry(1, 'acct-1', 10), entry(2, 'acct-2', 20)])
  })

  it('sends exactly maxPendingBatches unacknowledged batches, sends no more until one expires, then delivers the rest', async () => {
    const maxPendingBatches = 3
    const ackTimeoutMs = 5_000
    const { primary, replicas } = await cluster([REPLICA], { batchSize: 1, maxPendingBatches, ackTimeoutMs })
    const [replica] = replicas
    const expected = new Map<number, Entry>()
    faults.lose('ack')
    const sentBefore = faults.sent(PRIMARY, REPLICA, 'batch')

    for (let id = 1; id <= 6; id++) await write(primary, expected, entry(id, `acct-${id}`, id))
    await pass(2_000)

    expect(faults.sent(PRIMARY, REPLICA, 'batch') - sentBefore).toBe(maxPendingBatches)
    expect(ledgerRows(replica.dbPath)).toEqual([1, 2, 3].map(id => entry(id, `acct-${id}`, id)))

    faults.restore('ack')
    await holds(replica, expected, ackTimeoutMs * 3)
  })

  it('converges every replica through seeded loss of three messages in ten when one batch is in flight at a time', async () => {
    const { primary, replicas } = await cluster([REPLICA, SECOND_REPLICA], { maxPendingBatches: 1 })
    const expected = new Map<number, Entry>()
    faults.setDropRate(0.3)

    for (let id = 1; id <= 30; id++) {
      await write(primary, expected, entry(id, `acct-${id}`, id * 7))
      if (id % 4 === 0) {
        await settle(primary.engine.execute('UPDATE ledger SET cents = cents + 1 WHERE id = ?', [id - 2]))
        const updated = expected.get(id - 2)
        if (updated !== undefined) expected.set(id - 2, { ...updated, cents: updated.cents + 1 })
      }
      if (id % 5 === 0) {
        await settle(primary.engine.execute('DELETE FROM ledger WHERE id = ?', [id - 4]))
        expected.delete(id - 4)
      }
      await pass(15)
    }

    for (const replica of replicas) await holds(replica, expected, 120_000)
    for (const node of [primary, ...replicas]) expect(unrecoverable(node)).toEqual([])
  })

  it('reports a forwarded write as failed when its request is lost, and the primary holds no row', async () => {
    const { primary, replicas } = await cluster([REPLICA])
    const [replica] = replicas
    faults.loseNext(REPLICA, PRIMARY, 'forward_request')

    const outcome = await settle(replica.engine.execute(INSERT_ENTRY, [1, 'acct-1', 10]))

    expect(outcome).toBeInstanceOf(Error)
    await pass(ACK_TIMEOUT_MS * 3)
    expect(ledgerRows(primary.dbPath)).toEqual([])
    expect(ledgerRows(replica.dbPath)).toEqual([])
  })

  it('reports a forwarded write as failed without resending it when its response is lost, while the committed row reaches the replica', async () => {
    const { primary, replicas } = await cluster([REPLICA])
    const [replica] = replicas
    faults.loseNext(PRIMARY, REPLICA, 'forward_response')

    const outcome = await settle(replica.engine.execute(INSERT_ENTRY, [1, 'acct-1', 10]))

    expect(outcome).toBeInstanceOf(Error)
    expect(faults.sent(REPLICA, PRIMARY, 'forward_request')).toBe(1)
    expect(ledgerRows(primary.dbPath)).toEqual([entry(1, 'acct-1', 10)])
    await holds(replica, new Map([[1, entry(1, 'acct-1', 10)]]))
  })

  it('resends a dropped batch once the replica refuses the later batch that follows the gap', async () => {
    const { primary, replicas } = await cluster([REPLICA])
    const [replica] = replicas
    const expected = new Map<number, Entry>()
    const sentBefore = faults.sent(PRIMARY, REPLICA, 'batch')
    faults.loseNext(PRIMARY, REPLICA, 'batch')

    await write(primary, expected, entry(1, 'acct-1', 10))
    await until(() => faults.sent(PRIMARY, REPLICA, 'batch') > sentBefore, 'the first batch to leave the primary')
    await write(primary, expected, entry(2, 'acct-2', 20))

    await holds(replica, expected, ACK_TIMEOUT_MS * 20)
  })
})
