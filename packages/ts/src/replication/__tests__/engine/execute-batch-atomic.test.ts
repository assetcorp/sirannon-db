import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ReplicationEngine } from '../../engine.js'
import { createDbAndConn, createHarness, type EngineTestHarness, makeConfig, teardownHarness } from './helpers.js'

describe('executeBatch on a writable node', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('commits no parameter set when a later one fails', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engine.start()

    await expect(
      engine.executeBatch('INSERT INTO users (id, name) VALUES (?, ?)', [
        [1, 'Ada'],
        [2, 'Grace'],
        [1, 'Duplicate'],
      ]),
    ).rejects.toThrow()

    const rows = await db.query<{ count: number }>('SELECT count(*) AS count FROM users')
    expect(rows[0]?.count).toBe(0)
    await engine.stop()
  })

  it('stamps every parameter set of one batch with the same transaction id', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engine.start()

    const results = await engine.executeBatch('INSERT INTO users (id, name) VALUES (?, ?)', [
      [1, 'Ada'],
      [2, 'Grace'],
    ])

    expect(results.map(result => result.changes)).toEqual([1, 1])
    const stamped = await (await conn.prepare('SELECT DISTINCT tx_id FROM _sirannon_changes')).all()
    expect(stamped).toHaveLength(1)
    await engine.stop()
  })
})
