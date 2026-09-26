import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { QueryError } from '../../../core/errors.js'
import { ReplicationEngine } from '../../engine.js'
import { createDbAndConn, createHarness, type EngineTestHarness, makeConfig, teardownHarness } from './helpers.js'

const CREATE_USERS = 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)'
const INSERT_USER = 'INSERT INTO users (id, name) VALUES (?, ?)'

describe('statement errors on a writable node', () => {
  let harness: EngineTestHarness
  let engine: ReplicationEngine

  beforeEach(async () => {
    harness = createHarness()
    const { db, conn } = await createDbAndConn(harness, CREATE_USERS)
    engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engine.start()
    await engine.execute(INSERT_USER, [1, 'Ada'])
  })

  afterEach(async () => {
    await engine.stop()
    await teardownHarness(harness)
  })

  it('raises QUERY_ERROR with the statement when execute breaks a constraint', async () => {
    const failure = await engine.execute(INSERT_USER, [1, 'Duplicate']).catch((err: unknown) => err)

    expect(failure).toBeInstanceOf(QueryError)
    expect(failure).toMatchObject({ code: 'QUERY_ERROR', sql: INSERT_USER })
  })

  it('raises QUERY_ERROR when a batch breaks a constraint', async () => {
    const failure = await engine
      .executeBatch(INSERT_USER, [
        [2, 'Grace'],
        [1, 'Duplicate'],
      ])
      .catch((err: unknown) => err)

    expect(failure).toBeInstanceOf(QueryError)
    expect(failure).toMatchObject({ code: 'QUERY_ERROR' })
  })

  it('raises QUERY_ERROR when forwarded statements break a constraint on the primary', async () => {
    const failure = await engine
      .forwardStatements([{ sql: INSERT_USER, params: [1, 'Duplicate'] }])
      .catch((err: unknown) => err)

    expect(failure).toBeInstanceOf(QueryError)
    expect(failure).toMatchObject({ code: 'QUERY_ERROR' })
  })

  it('refuses a write to a reserved table with FORBIDDEN_SQL', async () => {
    await expect(engine.execute('DELETE FROM _sirannon_changes')).rejects.toMatchObject({ code: 'FORBIDDEN_SQL' })
    await expect(engine.executeBatch('DELETE FROM _sirannon_changes', [[]])).rejects.toMatchObject({
      code: 'FORBIDDEN_SQL',
    })
    await expect(engine.forwardStatements([{ sql: 'DELETE FROM _sirannon_changes' }])).rejects.toMatchObject({
      code: 'FORBIDDEN_SQL',
    })
  })
})
