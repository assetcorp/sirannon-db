import { existsSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  countSqlite,
  errorCodeOf,
  postJson,
  RawSocket,
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const INSERT_NOTE = 'INSERT INTO notes (body) VALUES (?)'
const SEEDED_NOTES = 3

let dir: string
let path: string
let served: ServedRegistry

function schemaRows(): Array<{ type: string; name: string; sql: string | null }> {
  return readSqlite(path, 'SELECT type, name, sql FROM sqlite_master ORDER BY name')
}

function bodyFor(route: string, sql: string): Record<string, unknown> {
  if (route === 'transaction')
    return { statements: [{ sql: INSERT_NOTE, params: ['first in the transaction'] }, { sql }] }
  if (route === 'batch' || route === 'load') return { sql, paramsBatch: [[]] }
  return { sql }
}

beforeEach(async () => {
  dir = scratchDirectory('forbidden-sql')
  path = join(dir, 'notes.db')
  const sirannon = trackedRegistry()
  const db = await sirannon.open('notes', path)
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)')
  await db.watch('notes')
  for (let index = 0; index < SEEDED_NOTES; index++) await db.execute(INSERT_NOTE, [`note ${index}`])
  served = await serveRegistry(sirannon, { acceptSql: true })
})

afterEach(releaseServedRegistries)

describe("statements that reach Sirannon's own tables or the SQLite catalogue", () => {
  it('refuses each one with FORBIDDEN_SQL on every route of both transports and leaves the file untouched', async () => {
    const attached = join(dir, 'smuggled.db')
    const refused = [
      'SELECT * FROM _sirannon_changes',
      'SELECT * FROM _SIRANNON_CHANGES',
      'SELECT * FROM "_sirannon_changes"',
      'SELECT * FROM [_sirannon_changes]',
      'SELECT * FROM `_sirannon_changes`',
      'SELECT * FROM main._sirannon_changes',
      'SELECT n.* FROM notes n JOIN _sirannon_changes c ON c.seq = n.id',
      'SELECT * FROM notes WHERE id IN (SELECT seq FROM _sirannon_changes)',
      'DELETE FROM _sirannon_changes',
      'UPDATE _sirannon_changes SET seq = 0',
      'DROP TABLE _sirannon_changes',
      'CREATE TABLE _sirannon_shadow (x)',
      `ATTACH DATABASE '${attached}' AS smuggled`,
      `  attach database '${attached}' as smuggled`,
      `/* a comment first */ ATTACH '${attached}' AS smuggled`,
      `SELECT 1; ATTACH DATABASE '${attached}' AS smuggled`,
      'DETACH DATABASE main',
      'PRAGMA writable_schema = ON',
      'pragma WRITABLE_SCHEMA=1',
      'PRAGMA main.writable_schema = 1',
      'DROP TABLE sqlite_sequence',
      'DELETE FROM sqlite_sequence',
      "UPDATE sqlite_master SET sql = 'x'",
      "INSERT INTO sqlite_master VALUES ('table', 'ghost', 'ghost', 0, 'CREATE TABLE ghost (x)')",
      'WITH doomed AS (SELECT 1) DELETE FROM sqlite_sequence',
    ]
    const changesBefore = countSqlite(path, '_sirannon_changes')
    const schemaBefore = schemaRows()
    const notesBefore = readSqlite(path, 'SELECT id, body FROM notes ORDER BY id')
    const ws = await RawSocket.open(`${served.wsBase}/db/notes`)

    let sent = 0
    for (const sql of refused) {
      for (const route of ['query', 'execute', 'transaction', 'batch', 'load']) {
        const answer = await postJson(served.base, `/db/notes/${route}`, bodyFor(route, sql))
        const reply = await ws.request({ id: `refused-${sent++}`, type: route, ...bodyFor(route, sql) })

        expect([answer.status, errorCodeOf(answer.body)], `${route} ${sql}`).toEqual([403, 'FORBIDDEN_SQL'])
        expect([reply.type, reply.error?.code], `${route} ${sql}`).toEqual(['error', 'FORBIDDEN_SQL'])
      }
    }

    expect(existsSync(attached)).toBe(false)
    expect(countSqlite(path, '_sirannon_changes')).toBe(changesBefore)
    expect(schemaRows()).toEqual(schemaBefore)
    expect(readSqlite(path, 'SELECT id, body FROM notes ORDER BY id')).toEqual(notesBefore)
  })

  it('answers reads of the SQLite catalogue with what the file holds', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/notes`)
    const reads = [
      "SELECT name, type FROM sqlite_master WHERE name = 'notes'",
      "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name",
      'SELECT name, seq FROM sqlite_sequence',
      "SELECT name FROM pragma_table_info('notes') ORDER BY cid",
    ]

    for (const [index, sql] of reads.entries()) {
      const expected = readSqlite(path, sql).map(row =>
        Object.fromEntries(
          Object.entries(row).map(([key, value]) => [key, typeof value === 'bigint' ? Number(value) : value]),
        ),
      )
      const answer = await postJson(served.base, '/db/notes/query', { sql })
      const reply = await ws.request({ id: `catalogue-${index}`, type: 'query', sql })

      expect(expected.length, sql).toBeGreaterThan(0)
      expect(answer.body, sql).toEqual({ rows: expected })
      expect(reply.data, sql).toEqual({ rows: expected })
    }
  })

  it("refuses a subscription to one of Sirannon's own tables", async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/notes`)

    for (const table of ['_sirannon_changes', '_SIRANNON_MIGRATIONS']) {
      const reply = await ws.request({ id: table, type: 'subscribe', table })
      expect([reply.type, reply.error?.code], table).toEqual(['error', 'FORBIDDEN_SQL'])
    }
    const allowed = await ws.request({ id: 'notes', type: 'subscribe', table: 'notes' })
    expect(allowed.type).toBe('subscribed')
  })
})
