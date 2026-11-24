import { existsSync } from 'node:fs'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
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

function processTempStoreDirectory(): unknown {
  const raw = new BetterSqlite3(':memory:')
  try {
    return raw.prepare('PRAGMA temp_store_directory').all()
  } finally {
    raw.close()
  }
}

function bodyFor(route: string, sql: string): Record<string, unknown> {
  if (route === 'transaction')
    return { statements: [{ sql: INSERT_NOTE, params: ['first in the transaction'] }, { sql }] }
  if (route === 'batch' || route === 'load') return { sql, paramsBatch: [[]] }
  if (route === 'columns') return { sql, shape: 'columns', maxRows: 5, maxBytes: 4096 }
  return { sql }
}

function routePath(route: string): string {
  return route === 'columns' ? 'query' : route
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

describe("statements that reach Sirannon's own tables, the SQLite catalogue, or a file other than the database", () => {
  it('refuses each one with FORBIDDEN_SQL on every route of both transports and leaves every file untouched', async () => {
    const attached = join(dir, 'smuggled.db')
    const copy = join(dir, 'copy.db')
    const refused = [
      `\ufeffVACUUM INTO '${copy}'`,
      `; /* prefix */ \ufeffVACUUM 'main' INTO '${copy}'`,
      `\ufeffATTACH '${attached}' AS other`,
      `\ufeffPRAGMA 'temp_store_directory' = '${dir}'`,
      `\ufeffPRAGMA data_store_directory = '${dir}'`,
      '\ufeffPRAGMA writable_schema = 1',
      `EXPLAIN PRAGMA temp_store_directory = '${dir}'`,
      `EXPLAIN QUERY PLAN PRAGMA 'main'.'temp_store_directory'('${dir}')`,
      `EXPLAIN PRAGMA data_store_directory = '${dir}'`,
      'EXPLAIN QUERY PLAN PRAGMA "writable_schema" = 1',
      `VACUUM INTO '${copy}'`,
      `vacuum main into '${copy}'`,
      `VACUUM 'main' /* then */ INTO '${copy}'`,
      `SELECT 1; VACUUM INTO '${copy}'`,
      'PRAGMA temp_store_directory',
      `PRAGMA temp_store_directory = '${dir}'`,
      `PRAGMA 'temp_store_directory' = '${dir}'`,
      `PRAGMA main."temp_store_directory" = '${dir}'`,
      `PRAGMA data_store_directory = '${dir}'`,
      "PRAGMA 'writable_schema' = 1",
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
      "SELECT * FROM '_sirannon_changes'",
      "SELECT * FROM main.'_sirannon_meta'",
      "SELECT n.* FROM notes n JOIN '_sirannon_changes' c ON c.seq = n.id",
      "SELECT * FROM notes, '_sirannon_meta'",
      "WITH leaked AS (SELECT * FROM '_sirannon_meta') SELECT * FROM leaked",
      "INSERT INTO '_sirannon_meta' (key, value) VALUES ('a', 'b')",
      "UPDATE '_sirannon_changes' SET seq = 0",
      "DELETE FROM '_sirannon_changes'",
      "DROP TABLE '_sirannon_changes'",
      "PRAGMA table_info('_sirannon_changes')",
      "SELECT * FROM ('_sirannon_changes')",
      "SELECT * FROM notes JOIN ('_sirannon_meta')",
      "SELECT * FROM ((notes, ('_sirannon_meta')))",
    ]
    const changesBefore = countSqlite(path, '_sirannon_changes')
    const schemaBefore = schemaRows()
    const notesBefore = readSqlite(path, 'SELECT id, body FROM notes ORDER BY id')
    const ws = await RawSocket.open(`${served.wsBase}/db/notes`)

    let sent = 0
    for (const sql of refused) {
      for (const route of ['query', 'columns', 'execute', 'transaction', 'batch', 'load']) {
        const answer = await postJson(served.base, `/db/notes/${routePath(route)}`, bodyFor(route, sql))
        const reply = await ws.request({ id: `refused-${sent++}`, type: routePath(route), ...bodyFor(route, sql) })

        expect([answer.status, errorCodeOf(answer.body)], `${route} ${sql}`).toEqual([403, 'FORBIDDEN_SQL'])
        expect([reply.type, reply.error?.code], `${route} ${sql}`).toEqual(['error', 'FORBIDDEN_SQL'])
      }
    }

    expect(existsSync(attached)).toBe(false)
    expect(existsSync(copy)).toBe(false)
    expect(processTempStoreDirectory()).toEqual([])
    expect(countSqlite(path, '_sirannon_changes')).toBe(changesBefore)
    expect(schemaRows()).toEqual(schemaBefore)
    expect(readSqlite(path, 'SELECT id, body FROM notes ORDER BY id')).toEqual(notesBefore)
  })

  it("refuses a caller's transaction control and temporary objects on every route of both transports, so the next caller's write commits", async () => {
    const refused = [
      'BEGIN',
      'BEGIN IMMEDIATE',
      'COMMIT',
      'END TRANSACTION',
      'ROLLBACK',
      'SAVEPOINT caller',
      'RELEASE caller',
      'SELECT 1; BEGIN',
      'CREATE TEMP VIEW notes AS SELECT 99 AS id, 0 AS body',
      'CREATE TEMPORARY TABLE notes (id INTEGER, body TEXT)',
      'CREATE TABLE temp.notes (id INTEGER, body TEXT)',
      'CREATE TABLE "temp".notes (id INTEGER, body TEXT)',
      'CREATE TRIGGER temp.notes_blank AFTER INSERT ON notes BEGIN UPDATE notes SET body = 0; END',
      'DROP TABLE temp.notes',
    ]
    const schemaBefore = schemaRows()
    const ws = await RawSocket.open(`${served.wsBase}/db/notes`)

    let sent = 0
    for (const sql of refused) {
      for (const route of ['query', 'columns', 'execute', 'transaction', 'batch', 'load']) {
        const answer = await postJson(served.base, `/db/notes/${routePath(route)}`, bodyFor(route, sql))
        const reply = await ws.request({
          id: `connection-state-${sent++}`,
          type: routePath(route),
          ...bodyFor(route, sql),
        })

        expect([answer.status, errorCodeOf(answer.body)], `${route} ${sql}`).toEqual([403, 'FORBIDDEN_SQL'])
        expect([reply.type, reply.error?.code], `${route} ${sql}`).toEqual(['error', 'FORBIDDEN_SQL'])
      }
    }

    const written = await postJson(served.base, '/db/notes/execute', {
      sql: INSERT_NOTE,
      params: ['after the refusals'],
    })
    const stored = readSqlite<{ id: bigint; body: string }>(path, 'SELECT id, body FROM notes ORDER BY id').map(
      row => ({
        id: Number(row.id),
        body: row.body,
      }),
    )
    const read = await ws.request({ id: 'read-after', type: 'query', sql: 'SELECT id, body FROM notes ORDER BY id' })

    expect(written.status).toBe(200)
    expect(stored.map(row => row.body)).toEqual([
      ...Array.from({ length: SEEDED_NOTES }, (_, index) => `note ${index}`),
      'after the refusals',
    ])
    expect(read.data).toEqual({ rows: stored })
    expect(schemaRows()).toEqual(schemaBefore)
  })

  it('answers reads of the SQLite catalogue with what the file holds', async () => {
    const ws = await RawSocket.open(`${served.wsBase}/db/notes`)
    for (const table of ['temp_store_directory', 'data_store_directory', 'writable_schema', 'café_sirannon_archive']) {
      const answer = await postJson(served.base, '/db/notes/execute', { sql: `CREATE TABLE ${table} (id INTEGER)` })
      expect(answer.status).toBe(200)
    }
    const reads = [
      "SELECT name, type FROM sqlite_master WHERE name = 'notes'",
      "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name",
      'SELECT name, seq FROM sqlite_sequence',
      "SELECT name FROM pragma_table_info('notes') ORDER BY cid",
      "PRAGMA table_info('temp_store_directory')",
      'PRAGMA main.table_info = "writable_schema"',
      'PRAGMA table_info([data_store_directory])',
      'PRAGMA table_info(café_sirannon_archive)',
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
