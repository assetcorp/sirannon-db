import { afterEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/index.js'
import { readSqlite } from '../lib/served-registry.js'
import { countOf, eventually, openDevice, releaseDeviceSync, sameRows, startSyncServer } from './harness.js'

const CREATE_NOTES: Migration = {
  version: 1,
  name: 'create_notes',
  up: 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)',
}
const ADD_FLAG: Migration = { version: 2, name: 'add_flag', up: 'ALTER TABLE notes ADD COLUMN flag INTEGER' }
const ADD_PINNED: Migration = { version: 3, name: 'add_pinned', up: 'ALTER TABLE notes ADD COLUMN pinned INTEGER' }

afterEach(releaseDeviceSync)

function history(path: string): unknown[] {
  return readSqlite(path, 'SELECT version, name, checksum FROM _sirannon_migrations ORDER BY version')
}

function columns(path: string): unknown[] {
  return readSqlite(path, "SELECT name, type FROM pragma_table_info('notes') ORDER BY cid")
}

describe('a device following the server schema', () => {
  it('migrates a device behind the server on start and pushes under the new schema', async () => {
    const server = await startSyncServer({ migrations: [CREATE_NOTES, ADD_FLAG] })
    const device = await openDevice(server.url, { migrations: [CREATE_NOTES] })
    const controller = device.sync()
    await controller.start()

    expect((await controller.status()).schemaVersion).toBe(2)
    expect(history(device.path)).toEqual(history(server.path))
    expect(columns(device.path)).toEqual(columns(server.path))
    await device.db.execute("INSERT INTO notes (id, body, flag) VALUES (7, 'written under version 2', 1)")
    await eventually(() => sameRows(device.path, server.path, 'notes'), 'the write reaching the server')
  })

  it('follows a server that migrates while devices stay connected, for a device that writes and for one that only reads', async () => {
    const registry = [CREATE_NOTES, ADD_FLAG]
    const server = await startSyncServer({ migrations: registry })
    const writer = await openDevice(server.url, { migrations: [CREATE_NOTES, ADD_FLAG] })
    const reader = await openDevice(server.url, { migrations: [CREATE_NOTES, ADD_FLAG] })
    const writing = writer.sync()
    const reading = reader.sync()
    await writing.start()
    await reading.start()

    registry.push(ADD_PINNED)
    await server.db.migrate(registry)
    await writer.db.execute("INSERT INTO notes (id, body) VALUES (8, 'written while behind')")
    await server.db.execute("INSERT INTO notes (id, body, pinned) VALUES (9, 'written after the migration', 1)")

    await eventually(() => countOf(server.path, 'notes') === 2, 'the stale write reaching the server')
    await eventually(() => sameRows(server.path, reader.path, 'notes'), 'the reader catching up')
    await eventually(() => sameRows(server.path, writer.path, 'notes'), 'the writer catching up')
    expect([(await writing.status()).schemaVersion, (await reading.status()).schemaVersion]).toEqual([3, 3])
    expect(history(writer.path)).toEqual(history(server.path))
    expect(history(reader.path)).toEqual(history(server.path))
  })

  it('follows a server that restarts with a new migration while the device stays connected', async () => {
    const registry = [CREATE_NOTES, ADD_FLAG]
    const server = await startSyncServer({ migrations: registry })
    const device = await openDevice(server.url, { migrations: [CREATE_NOTES, ADD_FLAG] })
    const controller = device.sync()
    await controller.start()
    await server.db.execute("INSERT INTO notes (id, body) VALUES (1, 'before the restart')")
    await eventually(() => countOf(device.path, 'notes') === 1, 'the first row')

    await server.stop()
    registry.push(ADD_PINNED)
    await server.db.migrate(registry)
    await server.start()
    await server.db.execute("INSERT INTO notes (id, body, pinned) VALUES (2, 'after the restart', 1)")

    await eventually(() => sameRows(server.path, device.path, 'notes'), 'the row written under the new schema')
    expect((await controller.status()).schemaVersion).toBe(3)
    expect(columns(device.path)).toEqual(columns(server.path))
  })

  it('replaces a device whose history diverges, or that lacks a migration that the server cannot send as SQL, by a snapshot', async () => {
    const addFlagInCode: Migration = {
      version: 2,
      name: 'add_flag',
      up: async tx => {
        await tx.execute('ALTER TABLE notes ADD COLUMN flag INTEGER')
      },
    }
    const server = await startSyncServer({ migrations: [CREATE_NOTES, addFlagInCode] })
    await server.db.execute("INSERT INTO notes (id, body, flag) VALUES (1, 'server truth', 0)")
    const divergent = await openDevice(server.url, {
      migrations: [{ version: 1, name: 'create_notes', up: 'CREATE TABLE notes (id INTEGER PRIMARY KEY, other TEXT)' }],
    })
    const behind = await openDevice(server.url, { migrations: [CREATE_NOTES] })

    for (const device of [divergent, behind]) {
      const announced: string[] = []
      const controller = device.sync({ snapshotRetryDelayMs: 50, onResyncRequired: () => announced.push('resync') })
      await controller.start()
      await eventually(async () => {
        const status = await controller.status()
        return status.state === 'running' && !status.resyncRequired && status.schemaVersion === 2
      }, 'the resync finishing')
      expect(announced).toEqual(['resync'])
      expect(history(device.path)).toEqual(history(server.path))
      expect(columns(device.path)).toEqual(columns(server.path))
      expect(sameRows(server.path, device.path, 'notes')).toBe(true)
    }
  })

  it('refuses to sync a device whose schema is ahead of the server', async () => {
    const server = await startSyncServer({ migrations: [CREATE_NOTES, ADD_FLAG] })
    const device = await openDevice(server.url, { migrations: [CREATE_NOTES, ADD_FLAG, ADD_PINNED] })
    await device.db.execute("INSERT INTO notes (id, body, pinned) VALUES (1, 'needs version 3', 1)")
    const controller = device.sync()

    await expect(controller.start()).rejects.toMatchObject({ code: 'SCHEMA_AHEAD' })
    const refused = await controller.status()
    expect([refused.state, refused.schemaVersion]).toEqual(['stopped', 3])
    expect(countOf(server.path, 'notes')).toBe(0)
  })
})
