import { copyFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { basename, dirname, extname, join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { SirannonError } from '../../../core/errors.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  driverFailingToLoad,
  driverOpeningConnectionsWithoutLoading,
  driverResolvingToRelativePath,
  driverWithoutExtensionSupport,
  driverWithoutPathResolver,
} from '../../helpers/fault-drivers/extension-contract-drivers.js'
import {
  createWorkspace,
  DRIVER_PLACEMENTS,
  PROBE,
  PROBE_LIBRARY,
  PROBE_VALUE,
  probeAnswers,
  probeFailure,
  readRaw,
  type Workspace,
} from './harness.js'

const library = PROBE_LIBRARY ?? ''
const LONGEST_FILE_NAME_BYTES = 255
const LONGEST_PATH_BYTES = 4096

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('hostile')
})

afterEach(async () => {
  await workspace.dispose()
})

async function openNotes(driver: SQLiteDriver, name: string, writerWorker = false): Promise<Database> {
  const db = await workspace.registry(driver).open(name, workspace.path(`${name}.db`), { writerWorker })
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
  return db
}

async function loadFailure(db: Database, extensionPath: string): Promise<unknown> {
  return db.loadExtension(extensionPath).then(
    () => new Error(`${JSON.stringify(extensionPath)} loaded`),
    (err: unknown) => err,
  )
}

async function expectStillServing(db: Database, name: string, body: string): Promise<void> {
  await db.execute('INSERT INTO notes (body) VALUES (?)', [body])
  expect(readRaw(workspace.path(`${name}.db`), 'SELECT body FROM notes ORDER BY id').at(-1)).toEqual({ body })
}

describe.skipIf(PROBE_LIBRARY === null).each(DRIVER_PLACEMENTS)(
  'loading an extension from a hostile path with $label',
  ({ driver, writerWorker }) => {
    it('refuses an empty path, a null byte, a control character, and each parent segment, even one that leads to the library', async () => {
      const db = await openNotes(driver(), 'refusals', writerWorker)
      const libraryDir = dirname(library)
      const refused: [string, RegExp][] = [
        ['', /empty or contains null bytes/],
        [`${library}\0`, /empty or contains null bytes/],
        ['\0', /empty or contains null bytes/],
        [`${library}\n`, /control characters/],
        [join(workspace.dir, 'tab\there.so'), /control characters/],
        [`${library}\u001f`, /control characters/],
        [`${libraryDir}/../${basename(libraryDir)}/${basename(library)}`, /directory traversal/],
        [`${libraryDir}\\..\\${basename(libraryDir)}\\${basename(library)}`, /directory traversal/],
        ['../sneaky/extension.so', /directory traversal/],
        ['..', /directory traversal/],
        [`${workspace.dir}/..`, /directory traversal/],
        ['C:\\extensions\\..\\system32\\probe.dll', /directory traversal/],
      ]

      for (const [extensionPath, reason] of refused) {
        const failure = await loadFailure(db, extensionPath)
        expect(failure, JSON.stringify(extensionPath)).toBeInstanceOf(SirannonError)
        expect(failure, JSON.stringify(extensionPath)).toMatchObject({
          name: 'ExtensionError',
          code: 'EXTENSION_ERROR',
          message: expect.stringMatching(reason),
        })
      }

      expect(await probeFailure(db)).toMatch(/no such function/)
      await expectStillServing(db, 'refusals', 'after the refusals')
    })

    it('reports a missing file, a directory, text, an empty file, a library with no entry point, and an overlong path as ExtensionError naming that path', async () => {
      const db = await openNotes(driver(), 'unloadable', writerWorker)
      const text = workspace.path(`notes${extname(library)}`)
      writeFileSync(text, 'these bytes are a shopping list, not a shared library')
      const empty = workspace.path(`empty${extname(library)}`)
      writeFileSync(empty, '')
      const renamed = workspace.path(`renamed${extname(library)}`)
      copyFileSync(library, renamed)
      const unloadable = [
        workspace.path(`absent${extname(library)}`),
        workspace.dir,
        text,
        empty,
        renamed,
        workspace.path(`${'n'.repeat(LONGEST_FILE_NAME_BYTES + 1)}${extname(library)}`),
        join(workspace.dir, ...Array.from({ length: LONGEST_PATH_BYTES / 64 + 1 }, () => 'd'.repeat(63))),
      ]

      for (const extensionPath of unloadable) {
        const failure = await loadFailure(db, extensionPath)
        expect(failure, extensionPath.slice(0, 120)).toMatchObject({
          name: 'ExtensionError',
          code: 'EXTENSION_ERROR',
          message: expect.stringContaining(extensionPath),
        })
      }
      expect(await loadFailure(db, unloadable[0] ?? '')).toMatchObject({
        message: expect.stringMatching(/no such file|image not found|cannot open|not found/i),
      })
      expect(await loadFailure(db, renamed)).toMatchObject({
        message: expect.stringMatching(/sqlite3_renamed_init|entry point|symbol not found|undefined symbol/i),
      })
      expect(await probeFailure(db)).toMatch(/no such function/)

      await db.loadExtension(library)
      expect(await loadFailure(db, text)).toMatchObject({ code: 'EXTENSION_ERROR' })
      expect(await probeAnswers(db)).toBe(PROBE_VALUE)
      await expectStillServing(db, 'unloadable', 'after the failures')
    })

    it('resolves a relative path, a name without its suffix, and a bare file name against the working directory at the time of the call', async () => {
      const db = await openNotes(driver(), 'resolution', writerWorker)
      await db.execute("INSERT INTO notes (body) VALUES ('first')")
      const relativeDir = join('libraries with spaces', 'ünïcødé', 'v1..2')
      mkdirSync(join(workspace.dir, relativeDir), { recursive: true })
      copyFileSync(library, join(workspace.dir, relativeDir, basename(library)))
      const callerDir = process.cwd()
      let bareFailure: unknown
      let bareResolved = ''

      process.chdir(workspace.dir)
      try {
        await db.loadExtension(join(relativeDir, basename(library)))
        await db.loadExtension(join(relativeDir, basename(library, extname(library))))
        bareResolved = join(process.cwd(), basename(library))
        bareFailure = await loadFailure(db, basename(library))
      } finally {
        process.chdir(callerDir)
      }

      expect(bareFailure).toMatchObject({ code: 'EXTENSION_ERROR', message: expect.stringContaining(bareResolved) })
      const marked = await db.live<{ mark: string }>(`SELECT ${PROBE}() AS mark FROM notes ORDER BY id`, undefined, {
        maxTransactionChanges: 1,
      })
      await db.executeTransaction([
        { sql: `INSERT INTO notes (body) VALUES (${PROBE}())` },
        { sql: `INSERT INTO notes (body) VALUES (${PROBE}())` },
      ])
      await vi.waitFor(() => {
        const state = marked.getState()
        expect(state.status === 'ready' && state.rows).toEqual([1, 2, 3].map(() => ({ mark: PROBE_VALUE })))
      })
      await marked.close()
      expect(readRaw(workspace.path('resolution.db'), 'SELECT body FROM notes ORDER BY id')).toEqual([
        { body: 'first' },
        { body: PROBE_VALUE },
        { body: PROBE_VALUE },
      ])
    })
  },
)

describe.skipIf(PROBE_LIBRARY === null)(
  'loading an extension through a driver that breaks the loading contract',
  () => {
    it('refuses a driver that cannot load an extension safely, with a message naming what the driver lacks', async () => {
      const real = await openNotes(betterSqlite3(), 'real')
      const missingFile = await loadFailure(real, workspace.path(`absent${extname(library)}`))
      const refusals: [string, SQLiteDriver, RegExp][] = [
        ['unsupported', driverWithoutExtensionSupport(), /not supported by the current driver/],
        ['callless', driverOpeningConnectionsWithoutLoading(), /declares extension support.*no loading call/],
        ['unresolving', driverWithoutPathResolver(), /resolves no absolute path/],
        ['relative', driverResolvingToRelativePath(), /resolved the extension to a relative path/],
      ]

      for (const [name, driver, reason] of refusals) {
        const db = await openNotes(driver, name)
        const failure = await loadFailure(db, library)
        expect(failure, name).toMatchObject({ name: 'ExtensionError', code: 'EXTENSION_ERROR' })
        expect(failure, name).toHaveProperty('message', expect.stringMatching(reason))
        expect(failure, name).not.toHaveProperty('message', (missingFile as Error).message)
        expect(await probeFailure(db), name).toMatch(/no such function/)
        await expectStillServing(db, name, `after the ${name} refusal`)
      }
    })

    it("passes a driver's own SirannonError through unchanged and reports any other failure as ExtensionError naming the caller's path", async () => {
      const refusal = new SirannonError(
        'this runtime runs SQLite compiled to WebAssembly, and a browser loads no native shared library into it',
        'EXTENSION_RUNTIME_REFUSED',
      )
      const ownWords = await openNotes(driverFailingToLoad(refusal), 'own-words')
      expect(await loadFailure(ownWords, library)).toBe(refusal)

      const thrown: [string, unknown, string][] = [
        ['plain-error', new Error('the mapping of the library failed'), 'the mapping of the library failed'],
        ['string', 'the loader gave up', 'the loader gave up'],
        ['number', 42, '42'],
      ]
      for (const [name, value, text] of thrown) {
        const db = await openNotes(driverFailingToLoad(value), name)
        const failure = await loadFailure(db, library)
        expect(failure, name).toMatchObject({ name: 'ExtensionError', code: 'EXTENSION_ERROR' })
        expect(failure, name).toHaveProperty('message', expect.stringContaining(library))
        expect(failure, name).toHaveProperty('message', expect.stringContaining(text))
        await expectStillServing(db, name, `after the ${name} failure`)
      }
    })
  },
)
