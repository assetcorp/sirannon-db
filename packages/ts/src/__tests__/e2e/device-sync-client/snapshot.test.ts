import { afterEach, describe, expect, it } from 'vitest'
import type { SnapshotOutcome, SnapshotProgress } from '../../../client/index.js'
import { readSqlite } from '../lib/served-registry.js'
import {
  countOf,
  eventually,
  metaValue,
  openDevice,
  releaseDeviceSync,
  sameRows,
  sleep,
  startSyncServer,
} from './harness.js'
import { forwardEverything, type HostileAnswer, startHostileServer } from './hostile-server.js'

const RICH_NOTES = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, weight INTEGER, art BLOB)'
const SEEDED = 12
const USABLE: SnapshotOutcome = { ok: true, error: null, databaseUsable: true, retrying: false }
const CUT_RESPONSE_CODES = ['CONNECTION_ERROR', 'INVALID_RESPONSE']

afterEach(releaseDeviceSync)

async function seededServer(rows = SEEDED, options: Parameters<typeof startSyncServer>[0] = {}) {
  const server = await startSyncServer({ schema: [RICH_NOTES], ...options })
  await server.db.transaction(async tx => {
    for (let id = 1; id <= rows; id++) {
      await tx.execute('INSERT INTO notes (id, body, weight, art) VALUES (?, ?, ?, ?)', [
        id,
        `server note ${id}`,
        9_007_199_254_740_991n + BigInt(id),
        Buffer.from([id % 256, 0, 255]),
      ])
    }
  })
  return server
}

function refusalCode(promise: Promise<unknown>): Promise<string> {
  return promise.then(
    () => 'resolved',
    (err: unknown) => String((err as { code?: unknown }).code),
  )
}

function cutPagesAfter(allowed: number, isCutting: () => boolean) {
  let pages = 0
  return (request: { path: string }): HostileAnswer => {
    if (!request.path.endsWith('/snapshot/page') || !isCutting()) return forwardEverything()
    pages += 1
    return pages > allowed
      ? { kind: 'cut', status: 200, text: '{"rows":[],"checksum":"","done":true}' }
      : forwardEverything()
  }
}

describe('a device downloading a snapshot', () => {
  it('leaves the first copy to the application, then replaces its tables with an exact server copy and syncs on from it', async () => {
    const server = await seededServer()
    const device = await openDevice(server.url, { schema: [RICH_NOTES] })
    await device.db.execute("INSERT INTO notes (id, body) VALUES (99, 'written before the copy')")
    const fromController: number[] = []
    const outcomes: SnapshotOutcome[] = []
    const controller = device.sync({
      onSnapshotProgress: p => fromController.push(p.loadedRows),
      onSnapshotComplete: outcome => outcomes.push(outcome),
    })
    await controller.start()
    await sleep(200)
    expect([(await controller.status()).resyncRequired, outcomes]).toEqual([false, []])
    expect(readSqlite(device.path, 'SELECT id FROM notes')).toEqual([{ id: 99n }])

    const progress: SnapshotProgress[] = []
    await controller.downloadSnapshot({ pageSize: 5, onProgress: event => progress.push(event) })

    expect(outcomes).toEqual([USABLE])
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    expect(countOf(device.path, 'notes')).toBe(SEEDED + 1)
    expect(fromController).toEqual([])
    expect(progress.map(event => event.loadedRows)).toEqual([5, 10, 13])
    expect(progress.at(-1)).toMatchObject({ table: 'notes', loadedRows: 13, totalRows: 13, tableTotalRows: 13 })
    expect(metaValue(device.path, 'device_sync_snapshot_state')).toBeNull()
    expect(metaValue(device.path, 'device_sync_pull_epoch')).toMatch(/^[0-9a-f]+$/)
    const settled = await controller.status()
    expect([settled.state, settled.resyncRequired, settled.lastError]).toEqual(['running', false, null])

    await controller.downloadSnapshot()
    expect(fromController.at(-1)).toBe(SEEDED + 1)

    await device.db.execute("INSERT INTO notes (id, body) VALUES (200, 'after the copy')")
    await server.db.execute("INSERT INTO notes (id, body) VALUES (201, 'from the server after the copy')")
    await eventually(
      () => countOf(server.path, 'notes') === SEEDED + 3 && sameRows(server.path, device.path, 'notes'),
      'sync resuming from the copy',
    )
  })

  it('follows pages that the server trims to its byte cap when it asks for the most rows that a page can hold', async () => {
    const server = await startSyncServer()
    const wide = 'w'.repeat(300_000)
    await server.db.transaction(async tx => {
      for (let id = 1; id <= 40; id++)
        await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `${id}${wide}`])
    })
    const device = await openDevice(server.url)
    const progress: number[] = []
    const controller = device.sync({ autoResync: false })
    await controller.start()

    await controller.downloadSnapshot({ pageSize: 1_000, onProgress: event => progress.push(event.loadedRows) })

    expect(progress.length).toBeGreaterThan(1)
    expect(progress.at(-1)).toBe(40)
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
  })

  it('pushes the writes that it holds before it wipes its tables, and refuses a download before the controller starts', async () => {
    const server = await seededServer()
    const device = await openDevice(server.url, { schema: [RICH_NOTES] })
    const controller = device.sync({ autoResync: false })
    await expect(controller.downloadSnapshot()).rejects.toThrow(/started sync controller/)

    await controller.start()
    controller.pause()
    await device.db.execute("INSERT INTO notes (id, body) VALUES (300, 'must survive the wipe')")
    await controller.downloadSnapshot({ pageSize: 50 })

    expect(readSqlite(server.path, 'SELECT body FROM notes WHERE id = 300')).toEqual([
      { body: 'must survive the wipe' },
    ])
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
  })

  it('refuses device reads and writes during the copy and reconciles the server writes made meanwhile', async () => {
    const server = await seededServer(300)
    const device = await openDevice(server.url, { schema: [RICH_NOTES] })
    const refusals: Array<Promise<string>> = []
    let serverWrites = 0
    const controller = device.sync({
      autoResync: false,
      onSnapshotProgress: () => {
        refusals.push(refusalCode(device.db.query('SELECT count(*) FROM notes')))
        refusals.push(refusalCode(device.db.execute("INSERT INTO notes (id, body) VALUES (5000, 'mid-copy')")))
        serverWrites += 1
        void server.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [1_000 + serverWrites, 'during the copy'])
      },
    })
    await controller.start()

    await controller.downloadSnapshot({ pageSize: 7 })
    const codes = await Promise.all(refusals)

    expect(codes.length).toBeGreaterThan(10)
    expect(new Set(codes)).toEqual(new Set(['SNAPSHOT_IN_PROGRESS']))
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the writes made during the copy arriving')
    expect(countOf(device.path, 'notes')).toBe(300 + serverWrites)
  })
})

describe('a device whose snapshot download fails', () => {
  it('reports a failure before the wipe as leaving its data usable', async () => {
    const server = await seededServer()
    const device = await openDevice(server.url, { schema: [RICH_NOTES] })
    await device.db.execute("INSERT INTO notes (id, body) VALUES (77, 'kept')")
    const outcomes: SnapshotOutcome[] = []
    const controller = device.sync({ autoResync: false, onSnapshotComplete: outcome => outcomes.push(outcome) })
    await controller.start()
    await eventually(() => countOf(server.path, 'notes') === SEEDED + 1, 'the device write reaching the server')

    await server.stop()
    await expect(controller.downloadSnapshot()).rejects.toMatchObject({ code: 'CONNECTION_ERROR' })

    expect(outcomes).toEqual([
      {
        ok: false,
        error: { code: 'CONNECTION_ERROR', message: expect.any(String) },
        databaseUsable: true,
        retrying: false,
      },
    ])
    expect((await controller.status()).resyncRequired).toBe(true)
    expect(readSqlite(device.path, 'SELECT id FROM notes')).toEqual([{ id: 77n }])
    expect(await device.db.query('SELECT id FROM notes')).toEqual([{ id: 77 }])
  })

  it('stays blocked through a reopen after a copy that died mid-way, and copies again on its own at the next start', async () => {
    const server = await seededServer(30)
    let cutting = true
    const hostile = await startHostileServer(
      () => server.directPort,
      cutPagesAfter(2, () => cutting),
    )
    const device = await openDevice(hostile.url, { schema: [RICH_NOTES] })
    const outcomes: SnapshotOutcome[] = []
    const first = device.sync({ autoResync: false, onSnapshotComplete: outcome => outcomes.push(outcome) })
    await first.start()

    const code = await refusalCode(first.downloadSnapshot({ pageSize: 5 }))
    expect(CUT_RESPONSE_CODES).toContain(code)
    expect(outcomes).toEqual([
      { ok: false, error: { code, message: expect.any(String) }, databaseUsable: false, retrying: false },
    ])
    expect(await refusalCode(device.db.query('SELECT id FROM notes'))).toBe('SNAPSHOT_IN_PROGRESS')
    expect(metaValue(device.path, 'device_sync_snapshot_state')).toBe('loading')

    await device.reopen()
    expect(await refusalCode(device.db.query('SELECT id FROM notes'))).toBe('SNAPSHOT_IN_PROGRESS')
    expect(await refusalCode(device.db.execute('INSERT INTO notes (id) VALUES (1)'))).toBe('SNAPSHOT_IN_PROGRESS')

    const announced: string[] = []
    const resumed: SnapshotOutcome[] = []
    cutting = false
    const second = device.sync({
      snapshotPageSize: 5,
      onResyncRequired: () => announced.push('resync'),
      onSnapshotComplete: outcome => resumed.push(outcome),
    })
    await second.start()
    expect(announced).toEqual(['resync'])

    await eventually(() => resumed.length === 1, 'the copy that the start schedules on its own')
    expect(resumed).toEqual([USABLE])
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    expect((await second.status()).resyncRequired).toBe(false)
    expect(await device.db.query('SELECT count(*) AS n FROM notes')).toEqual([{ n: 30 }])
  })

  it('retries a copy that died past the wipe on its own and reports the database usable once a retry finishes', async () => {
    const server = await seededServer(30)
    let cutting = true
    const hostile = await startHostileServer(
      () => server.directPort,
      cutPagesAfter(1, () => cutting),
    )
    const device = await openDevice(hostile.url, { schema: [RICH_NOTES] })
    const outcomes: SnapshotOutcome[] = []
    let editorEnabled = true
    const controller = device.sync({
      snapshotRetryDelayMs: 300,
      snapshotPageSize: 5,
      onResyncRequired: () => {
        editorEnabled = false
      },
      onSnapshotComplete: outcome => {
        editorEnabled = outcome.databaseUsable
        outcomes.push(outcome)
      },
    })
    await controller.start()

    expect(CUT_RESPONSE_CODES).toContain(await refusalCode(controller.downloadSnapshot({ pageSize: 5 })))
    expect(outcomes[0]).toMatchObject({ ok: false, databaseUsable: false, retrying: true })
    expect(editorEnabled).toBe(false)

    cutting = false
    await eventually(() => outcomes.length === 2, 'the scheduled retry finishing')
    expect(outcomes[1]).toEqual(USABLE)
    expect(editorEnabled).toBe(true)
    expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    await sleep(100)
    expect(outcomes).toHaveLength(2)
  })
})

describe('a device whose server database is replaced', () => {
  it('marks a resync that it keeps through a reopen and takes the new schema and rows from the next copy', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    const announced: string[] = []
    const controller = device.sync({ autoResync: false, onResyncRequired: () => announced.push('resync') })
    await controller.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'from the first database')")
    await eventually(() => countOf(device.path, 'notes') === 1, 'the first row')

    const replacement = await startSyncServer({
      schema: ['CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, pinned INTEGER NOT NULL DEFAULT 0)'],
    })
    await replacement.db.execute("INSERT INTO notes (id, body, pinned) VALUES (7, 'from the replacement', 1)")
    await server.stop()
    server.proxy.pointAt(replacement.directPort)

    await eventually(async () => (await controller.status()).resyncRequired, 'the resync signal')
    expect(announced).toEqual(['resync'])
    expect(metaValue(device.path, 'device_sync_resync_required')).toBe('1')

    await device.reopen()
    const after = device.sync({ autoResync: false })
    await after.start()
    expect((await after.status()).resyncRequired).toBe(true)

    await after.downloadSnapshot()
    expect(sameRows(replacement.path, device.path, 'notes')).toBe(true)
    expect(readSqlite(device.path, "SELECT name FROM pragma_table_info('notes') ORDER BY cid")).toEqual(
      readSqlite(replacement.path, "SELECT name FROM pragma_table_info('notes') ORDER BY cid"),
    )
    await replacement.db.execute("INSERT INTO notes (id, body) VALUES (8, 'after the copy')")
    await eventually(() => sameRows(replacement.path, device.path, 'notes'), 'sync with the replacement')
  })
})
