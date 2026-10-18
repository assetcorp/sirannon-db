import { afterEach, describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/index.js'
import { readSqlite } from '../lib/served-registry.js'
import { eventually, openDevice, releaseDeviceSync, sameRows, sleep, startSyncServer } from './harness.js'

const SHARED_ROWS = 12
const EDITS_PER_DEVICE = 60

afterEach(releaseDeviceSync)

function bodyOf(path: string, id: number): unknown {
  return readSqlite<{ body: unknown }>(path, 'SELECT body FROM notes WHERE id = ?', [id])[0]?.body
}

function ownEditClock(path: string, id: number): string {
  const [row] = readSqlite<{ hlc: string | null }>(
    path,
    "SELECT max(hlc) AS hlc FROM _sirannon_changes WHERE table_name = 'notes' AND row_id = ? AND node_id = (SELECT value FROM _sirannon_meta WHERE key = 'node_id')",
    [String(id)],
  )
  return row?.hlc ?? ''
}

function seededRandom(seed: number): () => number {
  let state = seed
  return () => {
    state = (state * 1_103_515_245 + 12_345) % 2_147_483_648
    return state / 2_147_483_648
  }
}

describe('devices editing the same rows', () => {
  it('settles two offline edits of one row on the later clock everywhere, and keeps a later local edit over an older remote one', async () => {
    const server = await startSyncServer()
    const laptop = await openDevice(server.url)
    const phone = await openDevice(server.url)
    const phonePulled: ChangeEvent[] = []
    const laptopSync = laptop.sync({ autoResync: false })
    const phoneSync = phone.sync({ autoResync: false, onChange: event => phonePulled.push(event) })
    await laptopSync.start()
    await phoneSync.start()

    await laptop.db.execute("INSERT INTO notes (id, body) VALUES (1, 'original'), (2, 'original')")
    await eventually(() => bodyOf(phone.path, 1) === 'original' && bodyOf(phone.path, 2) === 'original', 'the seed')

    laptopSync.pause()
    phoneSync.pause()
    await laptop.db.execute("UPDATE notes SET body = 'laptop edit' WHERE id = 1")
    await sleep(20)
    await phone.db.execute("UPDATE notes SET body = 'phone edit' WHERE id = 1")
    expect(ownEditClock(phone.path, 1) > ownEditClock(laptop.path, 1)).toBe(true)
    await laptopSync.resume()
    await phoneSync.resume()
    await eventually(
      () => [server.path, laptop.path, phone.path].every(path => bodyOf(path, 1) === 'phone edit'),
      'every replica holding the later edit',
    )

    phoneSync.pause()
    await laptop.db.execute("UPDATE notes SET body = 'older laptop edit' WHERE id = 2")
    await eventually(() => bodyOf(server.path, 2) === 'older laptop edit', 'the laptop edit reaching the server')
    await sleep(20)
    await phone.db.execute("UPDATE notes SET body = 'later phone edit' WHERE id = 2")
    phonePulled.length = 0
    await phoneSync.resume()
    await eventually(() => phonePulled.some(event => event.rowId === '2'), 'the phone receiving the older edit')

    expect(bodyOf(phone.path, 2)).toBe('later phone edit')
    await eventually(
      () => [server.path, laptop.path, phone.path].every(path => bodyOf(path, 2) === 'later phone edit'),
      'every replica holding the later phone edit',
    )
  })

  it('brings three devices and the server to identical rows after offline inserts and edits of the same rows', async () => {
    const server = await startSyncServer()
    const devices = [await openDevice(server.url), await openDevice(server.url), await openDevice(server.url)]
    const controllers = devices.map(device => device.sync({ autoResync: false }))
    for (const controller of controllers) await controller.start()
    for (let id = 1; id <= SHARED_ROWS; id++) {
      await server.db.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [id, `seed ${id}`])
    }
    await eventually(() => devices.every(device => sameRows(server.path, device.path, 'notes')), 'the seed')

    for (const controller of controllers) controller.pause()
    await Promise.all(
      devices.map(async (device, index) => {
        const random = seededRandom(index + 7)
        for (let edit = 0; edit < EDITS_PER_DEVICE; edit++) {
          const id = 1 + Math.floor(random() * (SHARED_ROWS + 4))
          if (random() < 0.4) {
            await device.db.execute('INSERT OR REPLACE INTO notes (id, body) VALUES (?, ?)', [
              id,
              `device ${index} put ${edit}`,
            ])
          } else {
            await device.db.execute('UPDATE notes SET body = ? WHERE id = ?', [`device ${index} edit ${edit}`, id])
          }
        }
      }),
    )
    for (let id = 1; id <= SHARED_ROWS; id += 3) {
      await server.db.execute('UPDATE notes SET body = ? WHERE id = ?', [`server edit ${id}`, id])
    }
    for (const controller of controllers) await controller.resume()

    await eventually(
      () => devices.every(device => sameRows(server.path, device.path, 'notes')),
      'every replica holding the same rows',
      30_000,
    )
  }, 45_000)

  it('converges when one device deletes a row that another deletes and creates again later, keeping the later row', async () => {
    const server = await startSyncServer()
    const first = await openDevice(server.url)
    const second = await openDevice(server.url)
    const firstSync = first.sync({ autoResync: false })
    const secondSync = second.sync({ autoResync: false })
    await firstSync.start()
    await secondSync.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'seed')")
    await eventually(
      () => sameRows(server.path, first.path, 'notes') && sameRows(server.path, second.path, 'notes'),
      'seed',
    )

    firstSync.pause()
    secondSync.pause()
    await first.db.execute('DELETE FROM notes WHERE id = 1')
    await sleep(5)
    await second.db.execute('DELETE FROM notes WHERE id = 1')
    await second.db.execute("INSERT INTO notes (id, body) VALUES (1, 'created again later')")
    await firstSync.resume()
    await sleep(300)
    await secondSync.resume()

    await eventually(
      () => sameRows(server.path, first.path, 'notes') && sameRows(server.path, second.path, 'notes'),
      'every replica holding the same rows',
    )
    expect(bodyOf(server.path, 1)).toBe('created again later')
  })

  it('settles a delete against a concurrent edit of the same row on the later clock, in either order', async () => {
    const server = await startSyncServer()
    const first = await openDevice(server.url)
    const second = await openDevice(server.url)
    const firstSync = first.sync({ autoResync: false })
    const secondSync = second.sync({ autoResync: false })
    await firstSync.start()
    await secondSync.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'seed'), (2, 'seed')")
    const replicas = [server.path, first.path, second.path]
    await eventually(() => replicas.every(path => bodyOf(path, 1) === 'seed' && bodyOf(path, 2) === 'seed'), 'seed')

    firstSync.pause()
    secondSync.pause()
    await first.db.execute('DELETE FROM notes WHERE id = 1')
    await second.db.execute("UPDATE notes SET body = 'edit earlier than the delete' WHERE id = 2")
    await sleep(20)
    await second.db.execute("UPDATE notes SET body = 'edit later than the delete' WHERE id = 1")
    await first.db.execute('DELETE FROM notes WHERE id = 2')
    await firstSync.resume()
    await sleep(300)
    await secondSync.resume()

    await eventually(
      () => sameRows(server.path, first.path, 'notes') && sameRows(server.path, second.path, 'notes'),
      'every replica holding the same rows',
    )
    expect(replicas.map(path => [bodyOf(path, 1), bodyOf(path, 2)])).toEqual(
      replicas.map(() => ['edit later than the delete', undefined]),
    )
  })
})
