import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, dirname, extname, join, relative } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { type BetterSqlite3Options, betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { memoryDestination } from '../../helpers/memory-destination.js'
import { builtStreamingExtensionPath } from '../../helpers/streaming-extension.js'

const extension = builtStreamingExtensionPath()
const PIECE_BYTES = 65_536
const SEEDED_ROWS = 400
const URI_SWITCH = 'SQLITE_USE_URI'

let dir: string
const registries: Sirannon[] = []

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-streaming-'))
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
  rmSync(dir, { recursive: true, force: true })
})

function seededBodies(): string[] {
  return Array.from({ length: SEEDED_ROWS }, (_, row) => `entry-${row}`.padEnd(180, '.'))
}

async function openSeeded(name: string, options: BetterSqlite3Options): Promise<Database> {
  const sirannon = new Sirannon({ driver: betterSqlite3(options) })
  registries.push(sirannon)
  const db = await sirannon.open(name, join(dir, `${name}.db`))
  await db.execute('CREATE TABLE journal (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
  await db.bulkLoad(
    'INSERT INTO journal (body) VALUES (?)',
    seededBodies().map(body => [body]),
  )
  return db
}

async function copyAndReadBack(db: Database, name: string): Promise<{ route: string; bodies: unknown[] }> {
  const destination = memoryDestination()
  const report = await db.backupTo({ destination, pieceBytes: PIECE_BYTES, stagingDir: dir })
  const restored = join(dir, `${name}-restored.db`)
  writeFileSync(restored, destination.bytesFor(report.destinationName))
  const raw = new BetterSqlite3(restored)
  try {
    expect(raw.pragma('integrity_check', { simple: true })).toBe('ok')
    const bodies = raw
      .prepare('SELECT body FROM journal ORDER BY id')
      .all()
      .map(row => (row as { body: string }).body)
    return { route: report.route, bodies }
  } finally {
    raw.close()
  }
}

function withoutUriSwitch<T>(action: () => Promise<T>): Promise<T> {
  const saved = process.env[URI_SWITCH]
  delete process.env[URI_SWITCH]
  return action().finally(() => {
    if (saved !== undefined) process.env[URI_SWITCH] = saved
  })
}

describe('streaming a backup through the compiled extension on better-sqlite3', () => {
  it.skipIf(process.env.SIRANNON_REQUIRE_STREAMING_EXTENSION !== '1')(
    'finds the compiled streaming extension on a runner that requires it',
    () => {
      expect(extension).not.toBeNull()
    },
  )

  it.skipIf(extension === null)(
    'streams a copy through an extension named by a relative path or a bare file name, resolved as the database opens',
    async () => {
      const library = extension ?? ''
      const callerDir = process.cwd()
      process.chdir(dirname(library))
      let bare: Database
      try {
        bare = await openSeeded('bare', { vfsExtensionPath: basename(library) })
      } finally {
        process.chdir(callerDir)
      }
      const nearby = await openSeeded('relative', { vfsExtensionPath: relative(callerDir, library) })

      for (const [name, db] of [
        ['bare', bare],
        ['relative', nearby],
      ] as const) {
        expect(db.backupCapabilities(), name).toMatchObject({ streamedCopy: true, localDiskRequired: 'none' })
        expect(await copyAndReadBack(db, name), name).toEqual({ route: 'streamed', bodies: seededBodies() })
      }
    },
  )

  it.skipIf(extension === null)(
    'copies through a local file when the runtime parses no URI file names, even with the extension named',
    async () => {
      const db = await withoutUriSwitch(() => openSeeded('staged', { vfsExtensionPath: extension ?? '' }))

      expect(db.backupCapabilities()).toMatchObject({ streamedCopy: false, localDiskRequired: 'equal-to-backup' })
      expect(await copyAndReadBack(db, 'staged')).toEqual({ route: 'staged', bodies: seededBodies() })
    },
  )

  it('fails each streamed copy with ExtensionError naming the setting when it names a missing file, text, or a directory, and writes nothing', async () => {
    const text = join(dir, `notes${extname(extension ?? '.so')}`)
    writeFileSync(text, 'these bytes are a packing list, not a shared library')
    const stagingDir = join(dir, 'staging')
    mkdirSync(stagingDir)

    for (const [name, vfsExtensionPath] of [
      ['absent', join(dir, `absent${extname(extension ?? '.so')}`)],
      ['text', text],
      ['directory', stagingDir],
    ] as const) {
      const db = await openSeeded(name, { vfsExtensionPath })
      const destination = memoryDestination()

      await expect(db.backupTo({ destination, pieceBytes: PIECE_BYTES, stagingDir }), name).rejects.toMatchObject({
        name: 'ExtensionError',
        code: 'EXTENSION_ERROR',
        message: expect.stringContaining(vfsExtensionPath),
      })
      expect(destination.names(), name).toEqual([])
      expect(readdirSync(stagingDir), name).toEqual([])
      await db.execute('INSERT INTO journal (body) VALUES (?)', [`after the ${name} failure`])
      expect(await db.queryOne('SELECT count(*) AS rows FROM journal'), name).toEqual({ rows: SEEDED_ROWS + 1 })
    }
  })
})
