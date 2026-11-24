import { execFileSync, spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

const FIXTURE = fileURLToPath(new URL('./bun-fresh-open.mjs', import.meta.url))
const VALUES_FIXTURE = fileURLToPath(new URL('./bun-values.mjs', import.meta.url))
const COLUMNS_FIXTURE = fileURLToPath(new URL('./bun-columns.mjs', import.meta.url))
const bunInstalled = spawnSync('bun', ['--version']).status === 0

let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-bun-'))
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

describe.skipIf(!bunInstalled)('the Bun driver', () => {
  it('opens a file that does not exist yet with a read pool, writes to it, and reopens it read-only', () => {
    const path = join(dir, 'fresh.db')
    expect(existsSync(path)).toBe(false)

    const output = execFileSync('bun', [FIXTURE, dir], { encoding: 'utf8', timeout: 60_000 })

    const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
    try {
      const bodies = raw.prepare('SELECT body FROM notes ORDER BY id').all()
      const stamped = raw.prepare("SELECT count(DISTINCT tx_id) AS n FROM _sirannon_changes WHERE tx_id <> ''").get()
      expect(JSON.parse(output)).toEqual({ read: bodies, readAgain: [{ n: 3 }] })
      expect(bodies).toEqual([
        { body: 'first write' },
        { body: 'through the handle' },
        { body: 'through the database' },
      ])
      expect(stamped).toEqual({ n: 2 })
    } finally {
      raw.close()
    }
  }, 90_000)

  it('binds named parameters in every prefix form, booleans as 1 or 0, and an empty BLOB, and refuses what SQLite cannot bind', () => {
    const output = execFileSync('bun', [VALUES_FIXTURE, dir], { encoding: 'utf8', timeout: 60_000 })

    const raw = new BetterSqlite3(join(dir, 'values.db'), { readonly: true, fileMustExist: true })
    try {
      raw.defaultSafeIntegers(true)
      const stored = raw.prepare('SELECT id, label, typeof(value) AS kind, value FROM cells ORDER BY id').all()
      expect(stored).toEqual([
        { id: 1n, label: 'colon', kind: 'text', value: 'named' },
        { id: 2n, label: 'at sign', kind: 'integer', value: 1n },
        { id: 3n, label: 'dollar', kind: 'integer', value: 0n },
        { id: 4n, label: 'empty blob', kind: 'blob', value: Buffer.alloc(0) },
        { id: 5n, label: 'largest integer', kind: 'integer', value: 2n ** 63n - 1n },
        { id: 6n, label: 'batch', kind: 'blob', value: Buffer.from([0, 255]) },
      ])
    } finally {
      raw.close()
    }
    expect(JSON.parse(output)).toEqual({
      refusals: { missingNamed: 'QUERY_ERROR', beyondInt64: 'QUERY_ERROR', plainObject: 'QUERY_ERROR' },
      read: [
        { id: 2, value: 1 },
        { id: 5, value: `${2n ** 63n - 1n}n` },
      ],
    })
  }, 90_000)

  it('returns columns with their declared types and rows in column order, and refuses a limited read whose names repeat', () => {
    const output = JSON.parse(execFileSync('bun', [COLUMNS_FIXTURE, dir], { encoding: 'utf8', timeout: 60_000 }))

    const raw = new BetterSqlite3(join(dir, 'columns.db'), { readonly: true, fileMustExist: true })
    let stored: unknown[][]
    try {
      raw.defaultSafeIntegers(true)
      stored = raw
        .prepare('SELECT id, label, amount, payload FROM items ORDER BY id')
        .raw(true)
        .all()
        .map(row =>
          (row as unknown[]).map(value => {
            if (typeof value === 'bigint') return Number.isSafeInteger(Number(value)) ? Number(value) : `${value}n`
            if (value instanceof Uint8Array) return `x'${Buffer.from(value).toString('hex')}'`
            return value
          }),
        )
    } finally {
      raw.close()
    }
    const detail = (name: string, declaredType: string | null) => ({
      name,
      declaredType,
      table: null,
      column: null,
      database: null,
    })

    expect(output.repeated.columns).toEqual([
      detail('id', 'INTEGER'),
      detail('id', 'INTEGER'),
      detail('label', 'TEXT'),
      detail('amount', 'INTEGER'),
      detail('payload', 'BLOB'),
      detail('total', null),
    ])
    expect(output.repeated.rows).toEqual(
      stored.map(([id, label, amount, payload]) => [id, id, label, amount, payload, amount]),
    )
    expect(output.repeatedLimited).toEqual({ refused: 'COLUMN_DETAILS_UNSUPPORTED' })
    expect(output.unique.rows).toEqual(stored.map(row => [...row, 1, 2]))
    expect(output.uniqueLimited).toMatchObject({
      rows: stored.slice(0, 2).map(row => [...row, 1, 2]),
      truncated: 'rows',
    })
    expect(output.bytes).toMatchObject({ rows: [[stored[0][1]]], truncated: 'bytes' })
    expect(output.reserved).toEqual({ refused: 'FORBIDDEN_SQL' })
  }, 90_000)
})
