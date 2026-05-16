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

const PRIMARY_ID = 'primary-sync-aaaa'
const REPLICA_ID = 'replica-sync-bbbb'

const SCHEMA = `
  CREATE TABLE articles (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT,
    score INTEGER NOT NULL DEFAULT 0
  )
`

const INDEX = 'CREATE INDEX idx_articles_score ON articles(score)'

describe('E2E: initial sync of schema and data', () => {
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
        await conn.exec(INDEX)
        await tracker.watch(conn, 'articles')
      },
    })

    const insertStmt = await primary.conn.prepare('INSERT INTO articles (id, title, body, score) VALUES (?, ?, ?, ?)')
    for (let i = 1; i <= 25; i++) {
      await insertStmt.run(i, `title-${i}`, `body of article ${i}`, i % 5)
    }
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

  it('syncs the schema and all pre-existing rows to a joining replica', async () => {
    if (!primary) throw new Error('primary not initialised')

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: '127.0.0.1',
      primaryPort: primary.port,
    })
    diagnostics = attachDiagnostics(primary, replica)

    await waitForReady(replica.engine, 25_000)
    await compareDatabases(primary.conn, replica.conn)

    const rowCountStmt = await replica.conn.prepare('SELECT COUNT(*) AS cnt FROM articles')
    const row = (await rowCountStmt.get()) as { cnt: number }
    expect(row.cnt).toBe(25)

    const indexStmt = await replica.conn.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_articles_score'",
    )
    const indexRow = await indexStmt.get()
    expect(indexRow).toBeDefined()
  })

  it('streams subsequent writes to the replica after initial sync completes', async () => {
    if (!primary) throw new Error('primary not initialised')

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: '127.0.0.1',
      primaryPort: primary.port,
    })
    diagnostics = attachDiagnostics(primary, replica)

    await waitForReady(replica.engine, 30_000)
    await compareDatabases(primary.conn, replica.conn)

    await primary.engine.execute("INSERT INTO articles (id, title, body, score) VALUES (100, 'post-sync', 'x', 9)")
    await primary.engine.execute('UPDATE articles SET score = 42 WHERE id = 1')

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)
  })
})
