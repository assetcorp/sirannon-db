import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { describe, expect, it } from 'vitest'
import { FieldMergeResolver } from '../../../replication/index.js'
import type { NodeStorage } from '../lib/factory.js'
import { caughtUp, errorsOf, eventually, readRows, tableRows } from '../lib/replication-scene.js'
import { PRIMARY, REPLICA, ready, sceneForEachTest } from './harness.js'

const handle = sceneForEachTest()

const LEDGER_SCHEMA = 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER, payload BLOB)'
const INSERT_LEDGER = 'INSERT INTO ledger (id, label, amount, payload) VALUES (?, ?, ?, ?)'
const FIRST_AMOUNT = 9007199254740993n
const SECOND_AMOUNT = 9007199254740995n
const FIRST_PAYLOAD = Buffer.from([0, 1, 2, 254, 255])
const SECOND_PAYLOAD = Buffer.from([9, 8, 7])

function replicaStorage(): NodeStorage {
  const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-conflicts-'))
  return { tempDir, dbPath: join(tempDir, `${REPLICA}.db`) }
}

function writeOutsideReplication(path: string, sql: string, params: unknown[]): void {
  const raw = new BetterSqlite3(path)
  try {
    raw.prepare(sql).run(...params)
  } finally {
    raw.close()
  }
}

function columnVersionsOf(path: string) {
  return async (table: string, rowId: string) => {
    const rows = readRows(
      path,
      'SELECT column_name, hlc, node_id FROM _sirannon_column_versions WHERE table_name = ? AND row_id = ?',
      [table, rowId],
    )
    return new Map(rows.map(row => [String(row.column_name), { hlc: String(row.hlc), nodeId: String(row.node_id) }]))
  }
}

describe('a replica row that differs from the primary when a primary update arrives', () => {
  it('merges column by column with a field-merge resolver, keeping large integers and blobs intact', async () => {
    const { scene } = handle
    const storage = replicaStorage()
    const primary = await scene.primary(PRIMARY, { schema: [LEDGER_SCHEMA], watch: ['ledger'] })
    const replica = await scene.replica(REPLICA, {
      port: primary.port,
      storage,
      config: { defaultConflictResolver: new FieldMergeResolver(columnVersionsOf(storage.dbPath)) },
    })
    await ready(replica)
    await primary.engine.execute(INSERT_LEDGER, [1, 'opened on the primary', FIRST_AMOUNT, FIRST_PAYLOAD])
    await caughtUp(replica, primary)

    writeOutsideReplication(storage.dbPath, 'UPDATE ledger SET label = ? WHERE id = 1', ['relabelled on the replica'])
    await primary.engine.execute('UPDATE ledger SET amount = ?, payload = ? WHERE id = 1', [
      SECOND_AMOUNT,
      SECOND_PAYLOAD,
    ])
    await caughtUp(replica, primary)

    expect(tableRows(primary.dbPath, 'ledger')).toEqual([
      { id: 1, label: 'opened on the primary', amount: SECOND_AMOUNT, payload: SECOND_PAYLOAD },
    ])
    expect(tableRows(replica.dbPath, 'ledger')).toEqual([
      { id: 1, label: 'relabelled on the replica', amount: SECOND_AMOUNT, payload: SECOND_PAYLOAD },
    ])
    expect(errorsOf(replica)).toEqual([])
  })

  it('takes the later primary update over the whole row with the default resolver', async () => {
    const { scene } = handle
    const storage = replicaStorage()
    const primary = await scene.primary(PRIMARY, { schema: [LEDGER_SCHEMA], watch: ['ledger'] })
    const replica = await scene.replica(REPLICA, { port: primary.port, storage })
    await ready(replica)
    await primary.engine.execute(INSERT_LEDGER, [1, 'opened on the primary', FIRST_AMOUNT, FIRST_PAYLOAD])
    await caughtUp(replica, primary)

    writeOutsideReplication(storage.dbPath, 'UPDATE ledger SET label = ? WHERE id = 1', ['relabelled on the replica'])
    await primary.engine.execute('UPDATE ledger SET amount = ? WHERE id = 1', [SECOND_AMOUNT])
    await caughtUp(replica, primary)

    expect(tableRows(replica.dbPath, 'ledger')).toEqual(tableRows(primary.dbPath, 'ledger'))
    expect(tableRows(replica.dbPath, 'ledger')).toEqual([
      { id: 1, label: 'opened on the primary', amount: SECOND_AMOUNT, payload: FIRST_PAYLOAD },
    ])
  })

  it('reports a resolver that throws as CONFLICT_ERROR naming the table and row, and leaves the row as it was', async () => {
    const { scene } = handle
    const resolverFailure = new Error('pricing rules unavailable')
    const primary = await scene.primary(PRIMARY, { schema: [LEDGER_SCHEMA], watch: ['ledger'] })
    const replica = await scene.replica(REPLICA, {
      port: primary.port,
      config: {
        defaultConflictResolver: {
          resolve: () => {
            throw resolverFailure
          },
        },
      },
    })
    await ready(replica)
    await primary.engine.execute(INSERT_LEDGER, [7, 'opened on the primary', FIRST_AMOUNT, FIRST_PAYLOAD])
    await caughtUp(replica, primary)

    await primary.engine.execute("UPDATE ledger SET label = 'changed on the primary' WHERE id = 7")
    await eventually(() => errorsOf(replica, 'batch-received').length > 0, 'the replica to report the conflict')

    const reported = errorsOf(replica, 'batch-received')[0]?.error
    expect(reported).toMatchObject({ code: 'CONFLICT_ERROR', table: 'ledger', rowId: '7' })
    expect(reported?.cause).toBe(resolverFailure)
    expect(tableRows(replica.dbPath, 'ledger')).toEqual([
      { id: 7, label: 'opened on the primary', amount: FIRST_AMOUNT, payload: FIRST_PAYLOAD },
    ])
  })
})
