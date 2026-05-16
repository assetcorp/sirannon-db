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

const PRIMARY_ID = 'primary-fwd-aaaa'
const REPLICA_ID = 'replica-fwd-bbbb'

const SCHEMA = `
  CREATE TABLE tasks (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL,
    status TEXT NOT NULL
  )
`

describe('E2E: write forwarding from replica to primary via Forward RPC', () => {
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
        await tracker.watch(conn, 'tasks')
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

  it('forwards single writes from the replica to the primary, then replicates back to the replica', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    const result = await replica.engine.execute(
      "INSERT INTO tasks (id, label, status) VALUES (1, 'compile', 'pending')",
    )
    expect(result.changes).toBe(1)

    const primaryRow = (await (await primary.conn.prepare('SELECT label, status FROM tasks WHERE id = 1')).get()) as
      | { label: string; status: string }
      | undefined
    expect(primaryRow?.label).toBe('compile')

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)
  })

  it('forwards multi-statement transactions atomically via forwardStatements', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    const result = await replica.engine.forwardStatements([
      { sql: "INSERT INTO tasks (id, label, status) VALUES (10, 'alpha', 'pending')" },
      { sql: "INSERT INTO tasks (id, label, status) VALUES (11, 'beta', 'pending')" },
      { sql: "INSERT INTO tasks (id, label, status) VALUES (12, 'gamma', 'pending')" },
    ])

    expect(result.results).toHaveLength(3)
    for (const r of result.results) {
      expect(r.changes).toBe(1)
    }

    const primaryCount = (await (await primary.conn.prepare('SELECT COUNT(*) AS cnt FROM tasks')).get()) as {
      cnt: number
    }
    expect(primaryCount.cnt).toBe(3)

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)
  })
})
