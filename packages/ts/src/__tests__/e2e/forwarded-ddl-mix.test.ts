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

const PRIMARY_ID = 'primary-fwddl-aaaa'
const REPLICA_ID = 'replica-fwddl-bbbb'

const SCHEMA = `
  CREATE TABLE inventory (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL
  )
`

describe('E2E: forwardStatements batch with DDL followed by DML over gRPC + mTLS', () => {
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
        await conn.exec(SCHEMA)
        await tracker.watch(conn, 'inventory')
      },
    })

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: '127.0.0.1',
      primaryPort: primary.port,
      configOverrides: { writeForwarding: true },
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

  it('propagates ALTER TABLE ADD COLUMN + INSERT in one forwarded batch to the replica with the new column populated', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    await replica.engine.forwardStatements([
      { sql: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER' },
      { sql: "INSERT INTO inventory (id, sku, quantity) VALUES (1, 'sku-001', 42)" },
    ])

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)

    const pragmaStmt = await replica.conn.prepare('PRAGMA table_info(inventory)')
    const cols = (await pragmaStmt.all()) as Array<{ name: string }>
    expect(cols.map(c => c.name)).toContain('quantity')

    const dataStmt = await replica.conn.prepare('SELECT id, sku, quantity FROM inventory WHERE id = 1')
    const row = (await dataStmt.get()) as { id: number; sku: string; quantity: number } | undefined
    expect(row).toEqual({ id: 1, sku: 'sku-001', quantity: 42 })
  })
})
