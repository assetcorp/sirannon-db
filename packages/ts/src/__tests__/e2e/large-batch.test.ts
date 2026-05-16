import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import {
  attachDiagnostics,
  compareDatabases,
  createMtlsCerts,
  createPrimary,
  createReplica,
  type ManagedNode,
  type MtlsCerts,
  stopNode,
  waitForReady,
  waitForReplica,
} from './lib/index.js'

const PRIMARY_ID = 'primary-bulk-aaaa'
const REPLICA_ID = 'replica-bulk-bbbb'

const SCHEMA = `
  CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL,
    value INTEGER NOT NULL
  )
`

const ROW_COUNT = 1500

describe('E2E: large batch of writes in a single transaction', () => {
  let certs: MtlsCerts
  let primary: ManagedNode | null = null
  let replica: ManagedNode | null = null
  let diagnostics: { cleanup(): void } | null = null

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
        await tracker.watch(conn, 'events')
      },
      configOverrides: {
        batchSize: 500,
        maxBatchChanges: 5000,
      },
    })

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: '127.0.0.1',
      primaryPort: primary.port,
      configOverrides: {
        maxBatchChanges: 5000,
      },
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

  it(`replicates ${ROW_COUNT} rows inserted in a single multi-row INSERT`, async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    const rows: string[] = []
    for (let i = 1; i <= ROW_COUNT; i++) {
      rows.push(`(${i}, 'event-${i}', ${i * 3})`)
    }
    const sql = `INSERT INTO events (id, label, value) VALUES ${rows.join(', ')}`
    await primary.engine.execute(sql)

    const targetSeq = primary.engine.getCurrentSeq()
    expect(targetSeq).toBeGreaterThanOrEqual(BigInt(ROW_COUNT))

    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 20_000)
    await compareDatabases(primary.conn, replica.conn)

    const countRow = (await (await replica.conn.prepare('SELECT COUNT(*) AS cnt FROM events')).get()) as {
      cnt: number
    }
    expect(countRow.cnt).toBe(ROW_COUNT)
  })
})
