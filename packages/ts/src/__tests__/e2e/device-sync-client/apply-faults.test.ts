import { afterEach, describe, expect, it } from 'vitest'
import { statementFaultDriver } from '../../helpers/fault-drivers/statement-fault-driver.js'
import { readSqlite } from '../lib/served-registry.js'
import {
  countOf,
  eventually,
  metaValue,
  NOTES_TABLE,
  openDevice,
  releaseDeviceSync,
  sameRows,
  sleep,
  stagedCount,
  startSyncServer,
  TAGS_TABLE,
} from './harness.js'

const APPLIED_NOTE_INSERT = 'INSERT INTO "notes"'

afterEach(releaseDeviceSync)

function serverSeqOf(path: string, table: string, rowId: number): string {
  const [row] = readSqlite<{ seq: bigint }>(
    path,
    'SELECT max(seq) AS seq FROM _sirannon_changes WHERE table_name = ? AND row_id = ?',
    [table, String(rowId)],
  )
  return String(row?.seq)
}

describe('a device whose local write fails while it applies a pulled transaction', () => {
  it('commits none of the transaction, holds its cursor, retries after its own retries fail, and converges', async () => {
    const { driver, fault } = statementFaultDriver()
    const server = await startSyncServer()
    const device = await openDevice(server.url, { driver })
    const controller = device.sync()
    await controller.start()

    for (const [round, failingCall] of [1, 2, 3].entries()) {
      await eventually(async () => (await controller.status()).lastError === null, `a clean start to round ${round}`)
      const cursorBefore = metaValue(device.path, 'device_sync_pull_seq')
      const rowsBefore = countOf(device.path, 'notes')
      const failuresBefore = fault.failures()
      fault.failCallWithinEachTransaction(APPLIED_NOTE_INSERT, failingCall)
      const first = 100 * (round + 1)
      await server.db.transaction(async tx => {
        for (let id = first; id < first + 3; id++)
          await tx.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, 'x'])
      })

      await eventually(() => fault.failures() - failuresBefore >= 2, `two failed attempts in round ${round}`)
      expect((await controller.status()).lastError).not.toBeNull()
      expect(countOf(device.path, 'notes')).toBe(rowsBefore)
      expect(metaValue(device.path, 'device_sync_pull_seq')).toBe(cursorBefore)

      fault.disarm()
      await eventually(() => sameRows(server.path, device.path, 'notes'), `the transaction applying in round ${round}`)
      await eventually(
        async () =>
          metaValue(device.path, 'device_sync_pull_seq') === String((await controller.status()).lastPulledSeq),
        `the cursor following the apply in round ${round}`,
      )
    }
    await eventually(async () => (await controller.status()).lastError === null, 'the failure clearing')
  }, 60_000)

  it('holds its cursor while its own conflict resolver throws, and applies the change once the resolver decides', async () => {
    const server = await startSyncServer()
    const device = await openDevice(server.url)
    let refusing = true
    let calls = 0
    const controller = device.sync({
      resolver: {
        resolve: () => {
          calls += 1
          if (refusing) throw new Error('the application resolver failed')
          return { action: 'accept_remote' }
        },
      },
    })
    await controller.start()
    await device.db.execute("INSERT INTO notes (id, body) VALUES (41, 'device version')")
    await eventually(() => countOf(server.path, 'notes') === 1, 'the device row reaching the server')
    const cursorBefore = metaValue(device.path, 'device_sync_pull_seq')

    await server.db.execute("UPDATE notes SET body = 'server version' WHERE id = 41")
    await eventually(() => calls >= 2, 'the resolver refusing twice')
    expect((await controller.status()).lastError?.code).toBe('CONFLICT_ERROR')
    expect(readSqlite(device.path, 'SELECT body FROM notes')).toEqual([{ body: 'device version' }])
    expect(metaValue(device.path, 'device_sync_pull_seq')).toBe(cursorBefore)

    refusing = false
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the resolved change applying')
  }, 30_000)

  it('waits longer between attempts at a change that keeps failing', async () => {
    const { driver, fault } = statementFaultDriver()
    const server = await startSyncServer()
    const device = await openDevice(server.url, { driver })
    const controller = device.sync({ pushIntervalMs: 50, maxPushRetryDelayMs: 30_000 })
    await controller.start()

    fault.failEveryCall(APPLIED_NOTE_INSERT)
    await server.db.execute("INSERT INTO notes (id, body) VALUES (20, 'never applies')")
    await eventually(async () => (await controller.status()).lastError !== null, 'the first failure')
    const atFirstFailure = fault.failures()
    await sleep(1_500)
    const retries = fault.failures() - atFirstFailure

    expect(retries).toBeGreaterThanOrEqual(2)
    expect(retries).toBeLessThan(8)
    expect(countOf(device.path, 'notes')).toBe(0)
  }, 30_000)

  it('applies staged work that failed in an earlier session once a later start can write it', async () => {
    const { driver, fault } = statementFaultDriver()
    const server = await startSyncServer()
    const device = await openDevice(server.url, { driver })
    const first = device.sync()
    await first.start()

    fault.failEveryCall(APPLIED_NOTE_INSERT)
    await server.db.transaction(async tx => {
      await tx.execute("INSERT INTO notes (id, body) VALUES (40, 'first')")
      await tx.execute("INSERT INTO notes (id, body) VALUES (41, 'second')")
    })
    await eventually(
      () => stagedCount(device.path) === 2 && fault.failures() >= 1,
      'the transaction staged and failing',
    )
    await device.reopen()

    const failuresAtReopen = fault.failures()
    const second = device.sync()
    await second.start()
    await eventually(() => fault.failures() - failuresAtReopen >= 2, 'the start retrying the staged work')
    expect(countOf(device.path, 'notes')).toBe(0)

    fault.disarm()
    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the staged work applying')
    await eventually(() => stagedCount(device.path) === 0, 'the staging table emptying')
  }, 30_000)

  it('keeps the transaction that it applied before a later one failed, and applies the later one once it can', async () => {
    const server = await startSyncServer({ schema: [NOTES_TABLE, TAGS_TABLE], watch: ['notes', 'tags'] })
    const device = await openDevice(server.url)
    const controller = device.sync({ tables: ['notes', 'tags'] })
    await controller.start()

    await server.db.execute("INSERT INTO notes (id, body) VALUES (50, 'applies')")
    await server.db.execute("INSERT INTO tags (id, label) VALUES (51, 'has no table on the device')")
    await eventually(async () => (await controller.status()).lastError !== null, 'the failure on the missing table')

    expect(readSqlite(device.path, 'SELECT id FROM notes')).toEqual([{ id: 50n }])
    expect(metaValue(device.path, 'device_sync_pull_seq')).toBe(serverSeqOf(server.path, 'notes', 50))
    expect(String((await controller.status()).lastPulledSeq)).toBe(serverSeqOf(server.path, 'notes', 50))

    await device.db.execute(TAGS_TABLE)
    await eventually(() => sameRows(server.path, device.path, 'tags'), 'the later transaction applying')
    expect(metaValue(device.path, 'device_sync_pull_seq')).toBe(serverSeqOf(server.path, 'tags', 51))
  }, 30_000)
})
