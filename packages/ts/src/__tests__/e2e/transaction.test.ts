import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import {
  attachDiagnostics,
  compareDatabases,
  createMtlsCerts,
  createPrimary,
  createReplica,
  type DiagnosticsHandle,
  type ManagedNode,
  type MtlsCerts,
  stopNode,
  waitForReady,
  waitForReplica,
} from './lib/index.js'

const PRIMARY_ID = 'primary-txn-aaaa'
const REPLICA_ID = 'replica-txn-bbbb'

const PRIMARY_SCHEMA = `
  CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    holder TEXT NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0
  );
`

const LEDGER_SCHEMA = `
  CREATE TABLE ledger (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    delta INTEGER NOT NULL
  );
`

describe('E2E: engine.transaction over real gRPC + mTLS', () => {
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

  beforeEach(async () => {
    primary = await createPrimary({
      nodeId: PRIMARY_ID,
      certs,
      initialize: async (conn, tracker) => {
        await conn.exec(PRIMARY_SCHEMA)
        await conn.exec(LEDGER_SCHEMA)
        await tracker.watch(conn, 'accounts')
        await tracker.watch(conn, 'ledger')
      },
    })

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: '127.0.0.1',
      primaryPort: primary.port,
    })

    diagnostics = attachDiagnostics(primary, replica)

    await waitForReady(replica.engine, 20_000)
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

  it('replicates an atomic multi-table transaction through the live gRPC loop', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    await primary.engine.execute("INSERT INTO accounts (id, holder, balance) VALUES (1, 'alice', 1000)")
    await primary.engine.execute("INSERT INTO accounts (id, holder, balance) VALUES (2, 'bob', 500)")

    const observed = await primary.engine.transaction(async tx => {
      await tx.execute('UPDATE accounts SET balance = balance - 300 WHERE id = 1')
      await tx.execute('UPDATE accounts SET balance = balance + 300 WHERE id = 2')
      await tx.execute('INSERT INTO ledger (id, account_id, delta) VALUES (1, 1, -300)')
      await tx.execute('INSERT INTO ledger (id, account_id, delta) VALUES (2, 2,  300)')
      return 'ok'
    })

    expect(observed).toBe('ok')

    const targetSeq = primary.engine.getCurrentSeq()
    expect(targetSeq).toBeGreaterThan(0n)

    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)

    const accountsStmt = await replica.conn.prepare('SELECT id, holder, balance FROM accounts ORDER BY id')
    const accounts = (await accountsStmt.all()) as Array<{ id: number; holder: string; balance: number }>
    expect(accounts).toEqual([
      { id: 1, holder: 'alice', balance: 700 },
      { id: 2, holder: 'bob', balance: 800 },
    ])

    const ledgerStmt = await replica.conn.prepare('SELECT id, account_id, delta FROM ledger ORDER BY id')
    const ledger = (await ledgerStmt.all()) as Array<{ id: number; account_id: number; delta: number }>
    expect(ledger).toEqual([
      { id: 1, account_id: 1, delta: -300 },
      { id: 2, account_id: 2, delta: 300 },
    ])
  })
})
