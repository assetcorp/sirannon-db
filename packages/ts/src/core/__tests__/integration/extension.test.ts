import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  compiledExtensionPath,
  EXTENSION_PROBE_FUNCTION,
  EXTENSION_PROBE_VALUE,
} from '../../../__tests__/helpers/compiled-extension.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { nodeSqlite } from '../../../drivers/node/index.js'
import { Database } from '../../database.js'
import type { SQLiteConnection, SQLiteDriver } from '../../driver/types.js'
import { ExtensionError, SirannonError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

const extensionPath = compiledExtensionPath() ?? ''

const drivers: [string, SQLiteDriver][] = [
  ['better-sqlite3', betterSqlite3()],
  ['node:sqlite', nodeSqlite()],
]

describe('Extension loading via Database', () => {
  it('throws ExtensionError for nonexistent extension path', async () => {
    const db = await Database.create('test', join(tempDir, 'ext.db'), testDriver)

    await expect(db.loadExtension('/nonexistent/extension.so')).rejects.toThrow(SirannonError)
    await expect(db.loadExtension('/nonexistent/extension.so')).rejects.toThrow('Failed to load extension')

    await db.close()
  })

  for (const [label, driver] of drivers) {
    it.skipIf(!extensionPath)(`answers a query through the extension's own function on ${label}`, async () => {
      const db = await Database.create('test', join(tempDir, `${label}.db`), driver)
      await db.loadExtension(extensionPath)

      const rows = await db.query<{ value: string }>(`SELECT ${EXTENSION_PROBE_FUNCTION}() AS value`)
      expect(rows[0]?.value).toBe(EXTENSION_PROBE_VALUE)

      await db.close()
    })
  }

  it('reports a missing file and a refused loading interface as different errors', async () => {
    const db = await Database.create('test', join(tempDir, 'distinct.db'), testDriver)
    const missingFile = await db.loadExtension(join(tempDir, 'absent.so')).catch((err: Error) => err.message)
    await db.close()

    const refusing: SQLiteDriver = {
      ...testDriver,
      capabilities: { multipleConnections: false, extensions: false },
      open: async (path, options) => {
        const conn = await testDriver.open(path, options)
        return { ...conn, loadExtension: undefined }
      },
    }
    const refusedDb = await Database.create('test', join(tempDir, 'refused.db'), refusing)
    const refusedInterface = await refusedDb
      .loadExtension(join(tempDir, 'absent.so'))
      .catch((err: Error) => err.message)
    await refusedDb.close()

    expect(missingFile).toMatch(/dlopen|cannot open|No such file|image not found/i)
    expect(refusedInterface).toMatch(/not supported by the current driver/)
    expect(missingFile).not.toBe(refusedInterface)
  })

  it('reports the refusal a runtime without extension support writes about itself', async () => {
    const refusal =
      'wa-sqlite runs SQLite compiled to WebAssembly, and a browser loads no native shared library into it'
    const browserLike: SQLiteDriver = {
      ...testDriver,
      capabilities: { multipleConnections: false, extensions: false },
      open: async (path, options) => {
        const conn = await testDriver.open(path, options)
        return {
          ...conn,
          loadExtension: async (extensionPath: string) => {
            throw new ExtensionError(extensionPath, refusal)
          },
        }
      },
    }
    const db = await Database.create('test', join(tempDir, 'browser.db'), browserLike)

    await expect(db.loadExtension(join(tempDir, 'absent.so'))).rejects.toThrow(refusal)

    await db.close()
  })

  it.skipIf(!extensionPath)(
    'answers a query through the extension on a database whose writes run in a worker',
    async () => {
      const db = await Database.create('test', join(tempDir, 'worker.db'), betterSqlite3(), {
        writerWorker: true,
      })
      await db.loadExtension(extensionPath)

      const rows = await db.query<{ value: string }>(`SELECT ${EXTENSION_PROBE_FUNCTION}() AS value`)
      expect(rows[0]?.value).toBe(EXTENSION_PROBE_VALUE)
      await db.execute(`CREATE TABLE marks (id INTEGER PRIMARY KEY, note TEXT DEFAULT (${EXTENSION_PROBE_FUNCTION}()))`)
      await db.execute('INSERT INTO marks (id) VALUES (1)')
      const written = await db.query<{ note: string }>('SELECT note FROM marks WHERE id = 1')
      expect(written[0]?.note).toBe(EXTENSION_PROBE_VALUE)

      await db.close()
    },
  )

  it.skipIf(!extensionPath)(
    'answers a live query through the extension on a connection opened after the load',
    async () => {
      const db = await Database.create('test', join(tempDir, 'live-after.db'), testDriver)
      await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
      await db.execute("INSERT INTO notes (id, body) VALUES (1, 'first')")
      await db.loadExtension(extensionPath)

      const live = await db.live<{ value: string }>(`SELECT ${EXTENSION_PROBE_FUNCTION}() AS value FROM notes`)
      const state = live.getState()
      expect(state.status === 'ready' && state.rows[0]?.value).toBe(EXTENSION_PROBE_VALUE)

      await live.close()
      await db.close()
    },
  )

  it.skipIf(!extensionPath)(
    'answers a live query through the extension on a connection opened before the load',
    async () => {
      const db = await Database.create('test', join(tempDir, 'live-before.db'), testDriver)
      await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
      await db.execute("INSERT INTO notes (id, body) VALUES (1, 'first')")

      const opened = await db.live<{ body: string }>('SELECT body FROM notes')
      const openedState = opened.getState()
      expect(openedState.status === 'ready' && openedState.rows).toHaveLength(1)
      await db.loadExtension(extensionPath)

      const live = await db.live<{ value: string }>(`SELECT ${EXTENSION_PROBE_FUNCTION}() AS value FROM notes`)
      const state = live.getState()
      expect(state.status === 'ready' && state.rows[0]?.value).toBe(EXTENSION_PROBE_VALUE)

      await live.close()
      await opened.close()
      await db.close()
    },
  )

  it('refuses a driver that declares extension support but resolves no absolute path', async () => {
    const unresolving: SQLiteDriver = { ...testDriver, resolveExtensionPath: undefined }
    const db = await Database.create('test', join(tempDir, 'unresolved.db'), unresolving)

    await expect(db.loadExtension(join(tempDir, 'absent.so'))).rejects.toThrow(
      /declares extension support but resolves no absolute path/,
    )

    await db.close()
  })

  it('refuses a resolver that returns a relative path', async () => {
    const relativeResolver: SQLiteDriver = { ...testDriver, resolveExtensionPath: () => 'probe.so' }
    const db = await Database.create('test', join(tempDir, 'relative.db'), relativeResolver)

    await expect(db.loadExtension('probe.so')).rejects.toThrow(/resolved the extension to a relative path/)

    await db.close()
  })

  it('reads and writes through a connection opened from a class-based driver', async () => {
    class ClassConnection {
      constructor(private readonly inner: SQLiteConnection) {}
      exec(sql: string) {
        return this.inner.exec(sql)
      }
      prepare(sql: string) {
        return this.inner.prepare(sql)
      }
      transaction<T>(fn: (conn: SQLiteConnection) => Promise<T>) {
        return this.inner.transaction(fn)
      }
      close() {
        return this.inner.close()
      }
      loadExtension(extensionPath: string) {
        return this.inner.loadExtension?.(extensionPath) ?? Promise.resolve()
      }
    }
    const classDriver: SQLiteDriver = {
      ...testDriver,
      open: async (path, options) => new ClassConnection(await testDriver.open(path, options)),
    }
    const db = await Database.create('test', join(tempDir, 'class.db'), classDriver)
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'first')")

    const live = await db.live<{ body: string }>('SELECT body FROM notes')
    const state = live.getState()
    expect(state.status === 'ready' && state.rows[0]?.body).toBe('first')

    await live.close()
    await db.close()
  })

  it('names the runtime when it carries no extension loading call', async () => {
    const conn = await betterSqlite3().open(':memory:')
    const withoutLoading: SQLiteDriver = {
      ...testDriver,
      open: async () => ({ ...conn, loadExtension: undefined }),
    }
    const db = await Database.create('test', join(tempDir, 'nameless.db'), withoutLoading)

    await expect(db.loadExtension(join(tempDir, 'absent.so'))).rejects.toThrow(
      /declares extension support but opens connections with no loading call/,
    )

    await db.close()
  })
})
