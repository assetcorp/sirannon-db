import { execFileSync, spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

const FIXTURE = fileURLToPath(new URL('./bun-fresh-open.mjs', import.meta.url))
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
})
