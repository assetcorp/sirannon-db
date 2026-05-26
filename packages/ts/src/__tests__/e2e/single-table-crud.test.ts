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

const PRIMARY_ID = 'primary-crud-aaaa'
const REPLICA_ID = 'replica-crud-bbbb'

const PRIMARY_SCHEMA = `
  CREATE TABLE widgets (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0
  )
`

describe('E2E: single-table CRUD over real gRPC + mTLS', () => {
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
        await tracker.watch(conn, 'widgets')
      },
    })

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: 'localhost',
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

  it('replicates inserts, updates and deletes through the live gRPC loop', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    await primary.engine.execute("INSERT INTO widgets (id, name, quantity) VALUES (1, 'sprocket', 12)")
    await primary.engine.execute("INSERT INTO widgets (id, name, quantity) VALUES (2, 'gadget', 4)")
    await primary.engine.execute('UPDATE widgets SET quantity = 17 WHERE id = 1')
    await primary.engine.execute('DELETE FROM widgets WHERE id = 2')

    const targetSeq = primary.engine.getCurrentSeq()
    expect(targetSeq).toBeGreaterThan(0n)

    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)

    const rows = (await (
      await replica.conn.prepare('SELECT id, name, quantity FROM widgets ORDER BY id')
    ).all()) as Array<{
      id: number
      name: string
      quantity: number
    }>
    expect(rows).toEqual([{ id: 1, name: 'sprocket', quantity: 17 }])
  })
})
