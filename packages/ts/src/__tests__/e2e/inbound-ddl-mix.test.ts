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

const PRIMARY_ID = 'primary-inbound-aaaa'
const REPLICA_ID = 'replica-inbound-bbbb'

const SCHEMA = `
  CREATE TABLE inventory (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL
  )
`

describe('E2E: peer-replicated DDL+DML batch refreshes replica CDC triggers', () => {
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

  it('captures the new column in the replica CDC log when a peer batch carries ALTER + INSERT against the same table', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    await primary.engine.transaction(async tx => {
      await tx.execute('ALTER TABLE inventory ADD COLUMN bar TEXT')
      await tx.execute("INSERT INTO inventory (id, sku, bar) VALUES (1, 'sku-001', 'value-bar')")
    })

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)

    const replicaCdcStmt = await replica.conn.prepare(
      "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
    )
    const cdcRow = (await replicaCdcStmt.get()) as { new_data: string } | undefined
    expect(cdcRow).toBeDefined()
    const parsed = JSON.parse(cdcRow?.new_data ?? '{}') as { id: number; sku: string; bar: string }
    expect(parsed).toEqual({ id: 1, sku: 'sku-001', bar: 'value-bar' })

    const dataStmt = await replica.conn.prepare('SELECT id, sku, bar FROM inventory WHERE id = 1')
    const dataRow = (await dataStmt.get()) as { id: number; sku: string; bar: string } | undefined
    expect(dataRow).toEqual({ id: 1, sku: 'sku-001', bar: 'value-bar' })
  })
})
