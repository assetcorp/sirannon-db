import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../core/database.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import { Sirannon } from '../../core/sirannon.js'
import type { ChangeEvent } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SyncController } from '../sync-controller.js'

const driver = betterSqlite3()

interface Device {
  name: string
  path: string
  sirannon: Sirannon
  db: Database
  controller: SyncController
  pulled: ChangeEvent[]
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string
let serverDb: Database
let devices: Device[]

async function makeDevice(name: string): Promise<Device> {
  const path = join(tempDir, `${name}.db`)
  const deviceSirannon = new Sirannon({ driver })
  const db = await deviceSirannon.open('appdb', path)
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await db.watch('notes')
  const pulled: ChangeEvent[] = []
  const controller = new SyncController(db, {
    url: baseUrl,
    databaseId: 'appdb',
    tables: ['notes'],
    pushIntervalMs: 25,
    ackIntervalMs: 25,
    autoResync: false,
    onChange: event => {
      pulled.push(event)
    },
  })
  const device: Device = { name, path, sirannon: deviceSirannon, db, controller, pulled }
  devices.push(device)
  return device
}

async function until(predicate: () => boolean | Promise<boolean>, timeout = 10_000): Promise<void> {
  const start = Date.now()
  while (!(await predicate())) {
    if (Date.now() - start >= timeout) throw new Error('condition never became true')
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

async function bodyOf(db: Database, id: number): Promise<string | undefined> {
  const rows = await db.query<{ body: string }>('SELECT body FROM notes WHERE id = ?', [id])
  return rows[0]?.body
}

async function ownEditHlc(device: Device, rowId: string): Promise<string> {
  const nodeId = (await device.db.deviceSync().identity()).nodeId
  const inspect = await driver.open(device.path)
  try {
    const stmt = await inspect.prepare(
      `SELECT MAX(hlc) AS hlc FROM ${CHANGES_TABLE} WHERE table_name = 'notes' AND row_id = ? AND node_id = ?`,
    )
    const row = (await stmt.get(rowId, nodeId)) as { hlc: string | null } | undefined
    const hlc = row?.hlc
    if (!hlc) throw new Error(`${device.name} logged no local edit of row ${rowId}`)
    return hlc
  } finally {
    await inspect.close()
  }
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-converge-'))
  devices = []
  sirannon = new Sirannon({ driver })
  serverDb = await sirannon.open('appdb', join(tempDir, 'server.db'))
  await serverDb.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await serverDb.watch('notes')
  server = createServer(sirannon, { acceptSql: true, port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  for (const device of devices) {
    await device.controller.stop()
    await device.sirannon.shutdown()
  }
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('two devices editing the same row offline', () => {
  it('converges every replica on the edit carrying the higher HLC', async () => {
    const laptop = await makeDevice('laptop')
    const phone = await makeDevice('phone')
    await laptop.controller.start()
    await phone.controller.start()

    await laptop.db.execute("INSERT INTO notes (id, body) VALUES (1, 'original')")
    await until(async () => (await bodyOf(serverDb, 1)) === 'original')
    await until(async () => (await bodyOf(phone.db, 1)) === 'original')

    laptop.controller.pause()
    phone.controller.pause()

    await laptop.db.execute("UPDATE notes SET body = 'laptop edit' WHERE id = 1")
    await new Promise(resolve => setTimeout(resolve, 20))
    await phone.db.execute("UPDATE notes SET body = 'phone edit' WHERE id = 1")

    const laptopHlc = await ownEditHlc(laptop, '1')
    const phoneHlc = await ownEditHlc(phone, '1')
    expect(phoneHlc > laptopHlc).toBe(true)

    await laptop.controller.resume()
    await phone.controller.resume()

    await until(async () => {
      const [onServer, onLaptop, onPhone] = await Promise.all([
        bodyOf(serverDb, 1),
        bodyOf(laptop.db, 1),
        bodyOf(phone.db, 1),
      ])
      return onServer === 'phone edit' && onLaptop === 'phone edit' && onPhone === 'phone edit'
    })

    expect(await bodyOf(serverDb, 1)).toBe('phone edit')
    expect(await bodyOf(laptop.db, 1)).toBe('phone edit')
    expect(await bodyOf(phone.db, 1)).toBe('phone edit')
  })

  it('rejects an older remote edit of a row it has already changed locally', async () => {
    const laptop = await makeDevice('laptop')
    const phone = await makeDevice('phone')
    await laptop.controller.start()
    await phone.controller.start()

    await laptop.db.execute("INSERT INTO notes (id, body) VALUES (2, 'original')")
    await until(async () => (await bodyOf(phone.db, 2)) === 'original')

    phone.controller.pause()
    await laptop.db.execute("UPDATE notes SET body = 'laptop edit' WHERE id = 2")
    await until(async () => (await bodyOf(serverDb, 2)) === 'laptop edit')

    await new Promise(resolve => setTimeout(resolve, 20))
    await phone.db.execute("UPDATE notes SET body = 'phone edit' WHERE id = 2")
    expect((await ownEditHlc(phone, '2')) > (await ownEditHlc(laptop, '2'))).toBe(true)

    phone.pulled.length = 0
    await phone.controller.resume()
    await until(() => phone.pulled.some(event => event.rowId === '2'))

    expect(await bodyOf(phone.db, 2)).toBe('phone edit')
    await until(async () => (await bodyOf(serverDb, 2)) === 'phone edit')
    await until(async () => (await bodyOf(laptop.db, 2)) === 'phone edit')
    expect(await bodyOf(phone.db, 2)).toBe('phone edit')
  })
})
