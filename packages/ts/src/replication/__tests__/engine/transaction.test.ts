import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { InMemoryTransport, MemoryBus } from '../../../transport/memory/index.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationConfig } from '../../types.js'

const NODE_PRIMARY = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const NODE_REPLICA = 'bbbb0000bbbb0000bbbb0000bbbb0000'

const SCHEMA = `
  CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    holder TEXT NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE ledger (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    delta INTEGER NOT NULL,
    memo TEXT
  );
`

interface NodeContext {
  db: Database
  conn: SQLiteConnection
  engine: ReplicationEngine
  transport: InMemoryTransport
}

interface ChangesRow {
  seq: number
  table_name: string
  operation: string
  row_id: string
  node_id: string
  tx_id: string
  hlc: string
}

async function waitUntil(predicate: () => Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise(resolve => setTimeout(resolve, 20))
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for predicate`)
}

async function readChanges(conn: SQLiteConnection): Promise<ChangesRow[]> {
  const stmt = await conn.prepare(
    'SELECT seq, table_name, operation, row_id, node_id, tx_id, hlc FROM _sirannon_changes ORDER BY seq ASC',
  )
  return (await stmt.all()) as ChangesRow[]
}

describe('ReplicationEngine.transaction', () => {
  let tempDir: string
  let bus: MemoryBus
  const openDbs: Database[] = []
  const openConns: SQLiteConnection[] = []
  const runningEngines: ReplicationEngine[] = []

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-engine-tx-'))
    bus = new MemoryBus()
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
    rmSync(tempDir, { recursive: true, force: true })
  })

  async function createNode(
    nodeId: string,
    role: 'primary' | 'replica',
    overrides: Partial<ReplicationConfig> = {},
  ): Promise<NodeContext> {
    const dbPath = join(tempDir, `${nodeId.slice(0, 8)}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    openConns.push(conn)

    const tracker = new ChangeTracker({ replication: true })
    await conn.exec(SCHEMA)
    await tracker.watch(conn, 'accounts')
    await tracker.watch(conn, 'ledger')

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    openDbs.push(db)

    const transport = new InMemoryTransport(bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology(role),
      transport,
      batchIntervalMs: 25,
      batchSize: 200,
      initialSync: false,
      changeTracker: tracker,
      ...overrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    runningEngines.push(engine)

    return { db, conn, engine, transport }
  }

  describe('change-log stamping', () => {
    it('stamps every CDC row with the engine node_id, a shared tx_id, and a non-empty HLC', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      await primary.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute("INSERT INTO accounts (id, holder, balance) VALUES (1, 'alice', 1000)")
        await tx.execute("INSERT INTO accounts (id, holder, balance) VALUES (2, 'bob', 500)")
        await tx.execute("INSERT INTO ledger (id, account_id, delta, memo) VALUES (10, 1, -250, 'transfer-out')")
        await tx.execute("INSERT INTO ledger (id, account_id, delta, memo) VALUES (11, 2,  250, 'transfer-in')")
      })

      const rows = await readChanges(primary.conn)
      expect(rows).toHaveLength(4)
      const tableSequence = rows.map(r => r.table_name)
      expect(tableSequence).toEqual(['accounts', 'accounts', 'ledger', 'ledger'])

      const txIds = new Set(rows.map(r => r.tx_id))
      expect(txIds.size).toBe(1)
      const sharedTxId = rows[0].tx_id
      expect(sharedTxId.length).toBeGreaterThan(0)
      expect(sharedTxId).not.toBe('')

      for (const row of rows) {
        expect(row.node_id).toBe(NODE_PRIMARY)
        expect(row.hlc.length).toBeGreaterThan(0)
        expect(row.hlc).not.toBe('')
      }
    })
  })

  describe('replication propagation', () => {
    it('delivers mixed INSERT/UPDATE/DELETE across tables to a connected replica', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      const replica = await createNode(NODE_REPLICA, 'replica')

      await primary.engine.start()
      await replica.engine.start()

      await primary.engine.execute("INSERT INTO accounts (id, holder, balance) VALUES (1, 'alice', 1000)")
      await primary.engine.execute("INSERT INTO accounts (id, holder, balance) VALUES (2, 'bob', 500)")
      await primary.engine.execute('INSERT INTO ledger (id, account_id, delta) VALUES (99, 1, 0)')

      await primary.engine.transaction(async tx => {
        await tx.execute('UPDATE accounts SET balance = balance - 200 WHERE id = 1')
        await tx.execute('UPDATE accounts SET balance = balance + 200 WHERE id = 2')
        await tx.execute("INSERT INTO ledger (id, account_id, delta, memo) VALUES (100, 1, -200, 'pay-bob')")
        await tx.execute('DELETE FROM ledger WHERE id = 99')
      })

      const target = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= target, 5000)

      const replicaAccountsStmt = await replica.conn.prepare('SELECT id, holder, balance FROM accounts ORDER BY id')
      const replicaAccounts = (await replicaAccountsStmt.all()) as Array<{
        id: number
        holder: string
        balance: number
      }>
      expect(replicaAccounts).toEqual([
        { id: 1, holder: 'alice', balance: 800 },
        { id: 2, holder: 'bob', balance: 700 },
      ])

      const replicaLedgerStmt = await replica.conn.prepare('SELECT id, account_id, delta, memo FROM ledger ORDER BY id')
      const replicaLedger = (await replicaLedgerStmt.all()) as Array<{
        id: number
        account_id: number
        delta: number
        memo: string | null
      }>
      expect(replicaLedger).toEqual([{ id: 100, account_id: 1, delta: -200, memo: 'pay-bob' }])
    })
  })

  describe('rollback semantics', () => {
    it('rolls back every write and leaves no orphan CDC rows when the callback throws', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      await primary.engine.start()

      const before = await readChanges(primary.conn)
      const seqBefore = primary.engine.getCurrentSeq()

      const failure = new Error('intentional rollback')
      await expect(
        primary.engine.transaction(async tx => {
          await tx.execute("INSERT INTO accounts (id, holder, balance) VALUES (5, 'charlie', 9999)")
          await tx.execute('INSERT INTO ledger (id, account_id, delta) VALUES (50, 5, 100)')
          throw failure
        }),
      ).rejects.toBe(failure)

      const after = await readChanges(primary.conn)
      expect(after).toEqual(before)

      const accountsStmt = await primary.conn.prepare('SELECT COUNT(*) AS n FROM accounts')
      const accounts = (await accountsStmt.get()) as { n: number }
      expect(accounts.n).toBe(0)

      const ledgerStmt = await primary.conn.prepare('SELECT COUNT(*) AS n FROM ledger')
      const ledger = (await ledgerStmt.get()) as { n: number }
      expect(ledger.n).toBe(0)

      expect(primary.engine.getCurrentSeq()).toBe(seqBefore)
    })
  })

  describe('no-op transaction', () => {
    it('commits cleanly and inserts no CDC rows when the callback performs no writes', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      await primary.engine.start()

      const seqBefore = primary.engine.getCurrentSeq()
      const before = await readChanges(primary.conn)

      const observed = await primary.engine.transaction(async _tx => {
        return 'fn-return-value'
      })

      expect(observed).toBe('fn-return-value')
      const after = await readChanges(primary.conn)
      expect(after).toEqual(before)
      expect(primary.engine.getCurrentSeq()).toBe(seqBefore)
    })
  })

  describe('concurrency', () => {
    it('serialises two parallel transactions and propagates every change to the replica', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      const replica = await createNode(NODE_REPLICA, 'replica')

      await primary.engine.start()
      await replica.engine.start()

      const [result1, result2] = await Promise.all([
        primary.engine.transaction(async tx => {
          await tx.execute("INSERT INTO accounts (id, holder, balance) VALUES (10, 'first', 100)")
          await tx.execute('INSERT INTO ledger (id, account_id, delta) VALUES (1010, 10, 100)')
          return 'first'
        }),
        primary.engine.transaction(async tx => {
          await tx.execute("INSERT INTO accounts (id, holder, balance) VALUES (20, 'second', 200)")
          await tx.execute('INSERT INTO ledger (id, account_id, delta) VALUES (1020, 20, 200)')
          return 'second'
        }),
      ])

      expect(result1).toBe('first')
      expect(result2).toBe('second')

      const rows = await readChanges(primary.conn)
      const accountsRows = rows.filter(r => r.table_name === 'accounts')
      expect(accountsRows).toHaveLength(2)
      const distinctTxIds = new Set(accountsRows.map(r => r.tx_id))
      expect(distinctTxIds.size).toBe(2)
      for (const row of rows) {
        expect(row.node_id).toBe(NODE_PRIMARY)
        expect(row.hlc).not.toBe('')
      }

      const target = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= target, 5000)

      const replicaStmt = await replica.conn.prepare('SELECT id, holder, balance FROM accounts ORDER BY id')
      const replicaAccounts = (await replicaStmt.all()) as Array<{ id: number; holder: string; balance: number }>
      expect(replicaAccounts).toEqual([
        { id: 10, holder: 'first', balance: 100 },
        { id: 20, holder: 'second', balance: 200 },
      ])
    })
  })

  describe('DDL handling', () => {
    it('records a __ddl__ marker row and refreshes triggers after CREATE INDEX inside a transaction', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      await primary.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute('CREATE INDEX idx_accounts_holder ON accounts (holder)')
      })

      const rows = await readChanges(primary.conn)
      const ddlRows = rows.filter(r => r.operation === 'DDL')
      expect(ddlRows).toHaveLength(1)
      expect(ddlRows[0].table_name).toBe('__ddl__')
      expect(ddlRows[0].node_id).toBe(NODE_PRIMARY)
      expect(ddlRows[0].tx_id.length).toBeGreaterThan(0)

      await primary.engine.execute("INSERT INTO accounts (id, holder, balance) VALUES (7, 'dave', 7)")
      const after = await readChanges(primary.conn)
      const dataRows = after.filter(r => r.operation !== 'DDL')
      expect(dataRows.length).toBeGreaterThan(0)
      for (const row of dataRows) {
        expect(row.node_id).toBe(NODE_PRIMARY)
      }
    })

    it('refreshes triggers mid-transaction after ALTER TABLE ADD COLUMN', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      await primary.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute("ALTER TABLE accounts ADD COLUMN status TEXT DEFAULT 'active'")
        await tx.execute("INSERT INTO accounts (id, holder, balance, status) VALUES (8, 'eve', 8, 'pending')")
      })

      const stmt = await primary.conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE operation = 'INSERT' AND table_name = 'accounts' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as {
        id: number
        holder: string
        balance: number
        status: string
      }
      expect(parsed.id).toBe(8)
      expect(parsed.holder).toBe('eve')
      expect(parsed.balance).toBe(8)
      expect(parsed.status).toBe('pending')
    })
  })

  describe('error envelope', () => {
    it('preserves a thrown ReplicationError when the callback emits DDL with embedded semicolons', async () => {
      const primary = await createNode(NODE_PRIMARY, 'primary')
      await primary.engine.start()

      await expect(
        primary.engine.transaction(async tx => {
          await tx.execute('CREATE TABLE leaks (id INTEGER); DROP TABLE accounts')
        }),
      ).rejects.toThrow('DDL statements containing semicolons')

      const rows = await readChanges(primary.conn)
      expect(rows.filter(r => r.table_name === 'leaks' || r.operation === 'DDL')).toEqual([])
    })
  })
})
