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

const PRIMARY_ID = 'primary-fk-aaaa'
const REPLICA_ID = 'replica-fk-bbbb'

const AUTHORS_TABLE = `
  CREATE TABLE authors (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
  )
`

const POSTS_TABLE = `
  CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    author_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    FOREIGN KEY (author_id) REFERENCES authors(id)
  )
`

const COMMENTS_TABLE = `
  CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    FOREIGN KEY (post_id) REFERENCES posts(id)
  )
`

describe('E2E: multi-table foreign-key ordering', () => {
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
        await conn.exec(AUTHORS_TABLE)
        await conn.exec(POSTS_TABLE)
        await conn.exec(COMMENTS_TABLE)
        await tracker.watch(conn, 'authors')
        await tracker.watch(conn, 'posts')
        await tracker.watch(conn, 'comments')
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

  it('preserves parent-before-child ordering when replicating across foreign-key chains', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    await primary.engine.execute("INSERT INTO authors (id, name) VALUES (1, 'ada')")
    await primary.engine.execute("INSERT INTO authors (id, name) VALUES (2, 'lin')")
    await primary.engine.execute("INSERT INTO posts (id, author_id, title) VALUES (10, 1, 'first')")
    await primary.engine.execute("INSERT INTO posts (id, author_id, title) VALUES (11, 1, 'second')")
    await primary.engine.execute("INSERT INTO posts (id, author_id, title) VALUES (12, 2, 'third')")
    await primary.engine.execute("INSERT INTO comments (id, post_id, body) VALUES (100, 10, 'hi')")
    await primary.engine.execute("INSERT INTO comments (id, post_id, body) VALUES (101, 10, 'again')")
    await primary.engine.execute("INSERT INTO comments (id, post_id, body) VALUES (102, 12, 'on third')")

    const targetSeq = primary.engine.getCurrentSeq()
    await waitForReplica(replica.engine, primary.nodeId, targetSeq, 10_000)
    await compareDatabases(primary.conn, replica.conn)

    const authors = (await (await replica.conn.prepare('SELECT COUNT(*) AS cnt FROM authors')).get()) as { cnt: number }
    const posts = (await (await replica.conn.prepare('SELECT COUNT(*) AS cnt FROM posts')).get()) as { cnt: number }
    const comments = (await (await replica.conn.prepare('SELECT COUNT(*) AS cnt FROM comments')).get()) as {
      cnt: number
    }
    expect(authors.cnt).toBe(2)
    expect(posts.cnt).toBe(3)
    expect(comments.cnt).toBe(3)
  })
})
