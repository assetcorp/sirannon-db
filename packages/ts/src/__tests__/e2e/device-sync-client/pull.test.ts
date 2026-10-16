import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import type { SyncStatus } from '../../../client/index.js'
import type { ChangeEvent } from '../../../core/index.js'
import { readSqlite, scratchDirectory } from '../lib/served-registry.js'
import {
  countOf,
  eventually,
  metaValue,
  NOTES_TABLE,
  openDevice,
  releaseDeviceSync,
  sameRows,
  sampleCounts,
  sleep,
  startSyncServer,
  TAGS_TABLE,
  writeSqlite,
} from './harness.js'

const PAIRED_ROWS = 50
const DEVICE_NODE_IDS = ['0'.repeat(32), 'f'.repeat(32), `7${'f'.repeat(31)}`, `8${'0'.repeat(31)}`]

afterEach(releaseDeviceSync)

function deviceFileWithNodeId(nodeId: string): string {
  const path = join(scratchDirectory('device-sync-device'), 'device.db')
  writeSqlite(path, [
    ['CREATE TABLE _sirannon_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)'],
    ["INSERT INTO _sirannon_meta (key, value) VALUES ('node_id', ?)", [nodeId]],
  ])
  return path
}

function serverMaxSeq(path: string): string {
  const [row] = readSqlite<{ top: bigint }>(path, 'SELECT max(seq) AS top FROM _sirannon_changes')
  return String(row?.top ?? 0n)
}

function ackedSeq(serverPath: string, deviceId: string | null): string | null {
  const [row] = readSqlite<{ acked: bigint }>(
    serverPath,
    'SELECT acked_seq AS acked FROM _sirannon_device_cursors WHERE device_id = ?',
    [deviceId],
  )
  return row === undefined ? null : String(row.acked)
}

describe('a device pulling server writes', () => {
  it('applies each server transaction whole across tables, commits before reporting, and receives none of its own writes', async () => {
    const server = await startSyncServer({ schema: [NOTES_TABLE, TAGS_TABLE], watch: ['notes', 'tags'] })
    const device = await openDevice(server.url, { schema: [NOTES_TABLE, TAGS_TABLE], watch: ['notes', 'tags'] })
    const received: Array<{ event: ChangeEvent; committed: boolean }> = []
    const controller = device.sync({
      tables: ['notes', 'tags'],
      onChange: event => {
        const committed =
          readSqlite(device.path, `SELECT 1 FROM "${event.table}" WHERE id = ?`, [event.row.id]).length === 1
        received.push({ event, committed })
      },
    })
    await controller.start()
    await device.db.execute("INSERT INTO notes (id, body) VALUES (10, 'written on the device')")
    await eventually(() => countOf(server.path, 'notes') === 1, 'the device write reaching the server')

    const sampler = sampleCounts(
      device.path,
      'SELECT (SELECT count(*) FROM notes WHERE id >= 100) + (SELECT count(*) FROM tags WHERE id >= 100) AS n',
    )
    await server.db.transaction(async tx => {
      for (let id = 100; id < 100 + PAIRED_ROWS; id++) {
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `note ${id}`])
        await tx.execute('INSERT INTO tags (id, label) VALUES (?, ?)', [id, `tag ${id}`])
      }
    })
    await server.db.execute("INSERT INTO notes (id, body) VALUES (11, 'written on the server')")
    await eventually(() => countOf(device.path, 'notes') === PAIRED_ROWS + 2, 'the pulled rows')
    await sleep(250)
    const samples = sampler.stop()

    expect(new Set(samples.filter(count => count !== 0 && count !== PAIRED_ROWS * 2))).toEqual(new Set())
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    expect(sameRows(server.path, device.path, 'tags')).toBe(true)
    expect(received.filter(({ event }) => event.table === 'notes' && Number(event.row.id) === 10)).toEqual([])
    expect(received.filter(({ committed }) => !committed)).toEqual([])
    expect(received).toHaveLength(PAIRED_ROWS * 2 + 1)

    const transactionIds = readSqlite<{ tx: string }>(
      device.path,
      "SELECT DISTINCT tx_id AS tx FROM _sirannon_changes WHERE table_name IN ('notes', 'tags') AND CAST(row_id AS INTEGER) >= 100",
    )
    expect(transactionIds).toHaveLength(1)
    expect(transactionIds[0]?.tx).toMatch(/^[0-9a-f]+$/)

    const deviceId = (await controller.status()).deviceId
    const lastServerSeq = serverMaxSeq(server.path)
    await eventually(() => metaValue(device.path, 'device_sync_pull_seq') === lastServerSeq, 'the durable cursor')
    await eventually(() => ackedSeq(server.path, deviceId) === lastServerSeq, 'the acknowledged cursor')
    expect(String((await controller.status()).lastPulledSeq)).toBe(lastServerSeq)
  })

  it('receives every kind of server write, each under the transaction that the server committed it in', async () => {
    const pairings = [false, true].flatMap(writerWorker => DEVICE_NODE_IDS.map(nodeId => ({ writerWorker, nodeId })))
    for (const { writerWorker, nodeId } of pairings) {
      const server = await startSyncServer({ database: { writerWorker } })
      const device = await openDevice(server.url, { path: deviceFileWithNodeId(nodeId) })
      await device.sync().start()

      const db = server.db
      await db.execute("INSERT INTO notes (body) VALUES ('execute')")
      await Promise.all(
        Array.from({ length: 6 }, (_, at) => db.execute('INSERT INTO notes (body) VALUES (?)', [`concurrent ${at}`])),
      )
      await db.executeTransaction([
        { sql: 'INSERT INTO notes (body) VALUES (?)', params: ['statements a'] },
        { sql: "UPDATE notes SET body = 'statements a updated' WHERE body = 'statements a'" },
        { sql: 'INSERT INTO notes (body) VALUES (?)', params: ['statements b'] },
      ])
      writeSqlite(server.path, [["INSERT INTO notes (body) VALUES ('from another process')"]])
      await db.executeBatch('INSERT INTO notes (body) VALUES (?)', [['batch a'], ['batch b'], ['batch c']])
      await db.transaction(async tx => {
        await tx.execute("INSERT INTO notes (body) VALUES ('callback')")
        await tx.execute("UPDATE notes SET body = 'callback updated' WHERE body = 'callback'")
        await tx.execute("UPDATE notes SET body = 'callback updated twice' WHERE body = 'callback updated'")
        await tx.execute("INSERT INTO notes (body) VALUES ('short-lived')")
        await tx.execute("DELETE FROM notes WHERE body = 'short-lived'")
        await db.execute("INSERT INTO notes (body) VALUES ('nested execute')")
        await db.executeBatch('INSERT INTO notes (body) VALUES (?)', [['nested batch']])
      })
      await db.bulkLoad(
        'INSERT INTO notes (body) VALUES (?)',
        Array.from({ length: 4 }, (_, at) => [`bulk ${at}`]),
      )
      await db.execute("WITH v(body) AS (VALUES ('with a'), ('with b')) INSERT INTO notes (body) SELECT body FROM v")
      await db.execute("/* imported */ INSERT INTO notes (body) VALUES ('block comment')")
      await db.execute("-- imported\nINSERT INTO notes (body) VALUES ('line comment')")
      await db.execute("UPDATE notes SET body = body || ' seen' WHERE body LIKE 'batch%'")
      await db.execute("DELETE FROM notes WHERE body LIKE 'concurrent%' AND id % 2 = 0")

      const label = `writerWorker ${writerWorker}, device ${nodeId}, server ${metaValue(server.path, 'node_id')}`
      await eventually(() => sameRows(server.path, device.path, 'notes'), `the device holding every row, ${label}`)
      expect(metaValue(device.path, 'node_id'), label).toBe(nodeId)
      const serverNode = metaValue(server.path, 'node_id')
      const transactionsOf = (path: string) =>
        readSqlite<{ tx: string }>(
          path,
          "SELECT DISTINCT tx_id AS tx FROM _sirannon_changes WHERE table_name = 'notes' AND node_id = ? ORDER BY tx",
          [serverNode],
        ).map(row => row.tx)
      const unstamped = readSqlite(server.path, "SELECT 1 FROM _sirannon_changes WHERE node_id = '' OR tx_id = ''")

      expect(unstamped, label).toEqual([])
      expect(countOf(server.path, 'notes'), label).toBe(21)
      expect(transactionsOf(server.path), label).toHaveLength(17)
      expect(transactionsOf(device.path), label).toEqual(transactionsOf(server.path))
      await releaseDeviceSync()
    }
  })

  it('converges when the device and the server write the same rows while it pulls', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    const controller = device.sync()
    await controller.start()

    const deviceWrites = (async () => {
      for (let id = 1; id <= 100; id++) {
        await device.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `device ${id}`])
        if (id % 10 === 0) await device.db.execute('UPDATE notes SET body = ? WHERE id <= ?', [`device pass ${id}`, id])
      }
    })()
    const serverWrites = (async () => {
      for (let id = 1_001; id <= 1_100; id++) {
        await server.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `server ${id}`])
        if (id % 10 === 0) await server.db.execute('UPDATE notes SET body = ? WHERE id <= ?', [`server pass ${id}`, 50])
      }
    })()
    await Promise.all([deviceWrites, serverWrites])

    await eventually(
      () => countOf(server.path, 'notes') === 200 && sameRows(server.path, device.path, 'notes'),
      'the device and the server holding the same rows',
    )
  })

  it('resumes after a stop and a reopen from the last change that it applied, with no change missed or repeated', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    const seen: string[] = []
    const record = (event: ChangeEvent) => seen.push(`${event.type}:${String(event.row.id ?? event.rowId)}`)

    const first = device.sync({ onChange: record })
    await first.start()
    for (let id = 1; id <= 20; id++) await server.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'one'])
    await eventually(() => countOf(device.path, 'notes') === 20, 'the first session')
    await first.stop()

    for (let id = 21; id <= 40; id++) await server.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'two'])
    await server.db.execute('DELETE FROM notes WHERE id <= 5')
    await device.reopen()

    const second = device.sync({ onChange: record })
    await second.start()
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the second session')
    await sleep(200)

    expect(seen.filter(entry => entry.startsWith('insert:'))).toHaveLength(40)
    expect(new Set(seen).size).toBe(seen.length)
    expect(countOf(device.path, 'notes')).toBe(35)
  })
})

describe('a device reporting its progress', () => {
  it('reports every state, push, pull, error, and snapshot to the status listener in the order that they happen', async () => {
    const server = await startSyncServer()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'already on the server')")
    const device = await openDevice(server.url)
    const reports: SyncStatus[] = []
    const controller = device.sync({ pushIntervalMs: 60_000, onStatusChange: status => reports.push(status) })

    await controller.start()
    await eventually(() => reports.some(status => status.state === 'running'), 'the running report')
    expect(reports[0]?.state).toBe('starting')

    await device.db.execute("INSERT INTO notes (id, body) VALUES (2, 'typed on the device')")
    await server.db.execute("INSERT INTO notes (id, body) VALUES (3, 'written on the server')")
    await eventually(() => reports.some(status => status.pendingPushCount === 1), 'the pending count report')
    controller.triggerPush()
    await eventually(() => reports.some(status => status.lastPushedSeq > 0n && status.pushCaughtUp), 'the push report')
    await eventually(() => reports.some(status => status.lastPulledSeq !== null), 'the pull report')

    await server.stop()
    await device.db.execute("INSERT INTO notes (id, body) VALUES (4, 'typed while the server is down')")
    controller.triggerPush()
    await eventually(() => reports.some(status => status.lastError?.code === 'CONNECTION_ERROR'), 'the error report')
    await server.start()
    await eventually(() => {
      controller.triggerPush()
      return reports.at(-1)?.lastError === null && countOf(server.path, 'notes') === 4
    }, 'the recovery')

    await controller.downloadSnapshot()
    const snapshotting = reports.findIndex(status => status.state === 'snapshotting')
    expect(snapshotting).toBeGreaterThan(0)
    expect(reports.slice(snapshotting).some(status => status.state === 'running')).toBe(true)

    await controller.stop()
    expect(reports.at(-1)?.state).toBe('stopped')
  })

  it('keeps syncing when every application callback throws', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    const fail = (): never => {
      throw new Error('the application callback failed')
    }
    const controller = device.sync({
      onChange: fail,
      onStatusChange: fail,
      onResyncRequired: fail,
      onSnapshotProgress: fail,
      onSnapshotComplete: fail,
    })
    await controller.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'first')")
    await eventually(() => countOf(device.path, 'notes') === 1, 'the first pulled row')

    await controller.downloadSnapshot()
    expect((await controller.status()).state).toBe('running')
    await server.db.execute("INSERT INTO notes (id, body) VALUES (2, 'after the snapshot')")
    await device.db.execute("INSERT INTO notes (id, body) VALUES (3, 'from the device')")
    await eventually(() => countOf(device.path, 'notes') === 3 && sameRows(server.path, device.path, 'notes'), 'sync')
  })
})
