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

const PRIMARY_ID = 'primary-ddl-aaaa'
const REPLICA_ID = 'replica-ddl-bbbb'

const INITIAL_SCHEMA = `
  CREATE TABLE inventory (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL
  )
`

describe('E2E: live DDL replicated to replica after initial sync', () => {
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
        await conn.exec(INITIAL_SCHEMA)
        await tracker.watch(conn, 'inventory')
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

  it('replicates CREATE INDEX and ALTER TABLE ADD COLUMN to the replica', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'sku-001')")
    await primary.engine.execute('CREATE INDEX idx_inventory_sku ON inventory(sku)')
    await primary.engine.execute('ALTER TABLE inventory ADD COLUMN quantity INTEGER')
    await primary.engine.execute('UPDATE inventory SET quantity = 7 WHERE id = 1')
    await primary.engine.execute("INSERT INTO inventory (id, sku, quantity) VALUES (2, 'sku-002', 99)")

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)

    const idxStmt = await replica.conn.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_inventory_sku'",
    )
    const idxRow = await idxStmt.get()
    expect(idxRow).toBeDefined()

    const pragmaStmt = await replica.conn.prepare('PRAGMA table_info(inventory)')
    const cols = (await pragmaStmt.all()) as Array<{ name: string }>
    const colNames = cols.map(c => c.name)
    expect(colNames).toContain('quantity')

    const dataStmt = await replica.conn.prepare('SELECT id, sku, quantity FROM inventory ORDER BY id')
    const rows = (await dataStmt.all()) as Array<{ id: number; sku: string; quantity: number | null }>
    expect(rows).toEqual([
      { id: 1, sku: 'sku-001', quantity: 7 },
      { id: 2, sku: 'sku-002', quantity: 99 },
    ])
  })
})
