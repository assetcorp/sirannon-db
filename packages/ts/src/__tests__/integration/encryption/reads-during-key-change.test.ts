import { randomBytes } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { yieldingReadDriver } from '../../helpers/fault-drivers/yielding-read-driver.js'
import { readWithIndependentAegis } from '../../helpers/independent-aegis.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'

const binding = builtSirannonSqliteBindingPath()
const ACCOUNTS = 400
const RANGE_WIDTH = 5
const READ_LANES = 16
const READ_YIELD_MS = 2
const RETRY_PAUSE_MS = 1
const READ_POOL_SIZE = 3

type Row = Record<string, unknown>

interface ReadTally {
  served: number
  refused: number
  failures: string[]
}

function ownerOf(id: number): string {
  return `account-${id}-holder-confidential`
}

function pauseFor(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function asAccount(row: Row | undefined): { id: number; owner: unknown } | undefined {
  return row === undefined ? undefined : { id: Number(row.id), owner: row.owner }
}

async function readAccounts(db: Database, finished: () => boolean, tally: ReadTally): Promise<void> {
  while (!finished()) {
    const id = 1 + Math.floor(Math.random() * (ACCOUNTS - RANGE_WIDTH))
    try {
      const one = asAccount(await db.queryOne<Row>('SELECT id, owner FROM accounts WHERE id = ?', [id]))
      if (one?.id !== id || one.owner !== ownerOf(id))
        tally.failures.push(`point read of ${id} gave ${JSON.stringify(one)}`)
      const range = (
        await db.query<Row>('SELECT id, owner FROM accounts WHERE id BETWEEN ? AND ? ORDER BY id', [
          id,
          id + RANGE_WIDTH - 1,
        ])
      ).map(asAccount)
      const expected = Array.from({ length: RANGE_WIDTH }, (_, at) => ({ id: id + at, owner: ownerOf(id + at) }))
      if (JSON.stringify(range) !== JSON.stringify(expected)) {
        tally.failures.push(`range read from ${id} gave ${JSON.stringify(range)}`)
      }
      tally.served++
    } catch (err) {
      const code = (err as { code?: string }).code
      if (code !== 'ENCRYPTION_CHANGE_IN_PROGRESS') {
        tally.failures.push(`${code ?? 'no code'}: ${err instanceof Error ? err.message : String(err)}`)
        return
      }
      tally.refused++
      await pauseFor(RETRY_PAUSE_MS)
    }
  }
}

describe.skipIf(binding === null)('reads in flight while a key change replaces the file', () => {
  let dir: string
  let sirannon: Sirannon | null = null

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-reads-during-swap-'))
  })

  afterEach(async () => {
    await sirannon?.shutdown()
    sirannon = null
    rmSync(dir, { recursive: true, force: true })
  })

  it.each([
    { observation: 'no hook or metric', metrics: undefined },
    { observation: 'a metrics callback', metrics: { onQueryComplete: () => undefined } },
  ])('answers every read with the right rows or a retryable refusal on a driver whose reads span event-loop turns, under $observation', async ({
    metrics,
  }) => {
    if (binding === null) return
    sirannon = new Sirannon({
      driver: yieldingReadDriver({ sqliteBindingPath: binding, yieldMs: READ_YIELD_MS }),
      ...(metrics ? { metrics } : {}),
    })
    const path = join(dir, 'accounts.db')
    const db = await sirannon.open('accounts', path, { readPoolSize: READ_POOL_SIZE })
    await db.execute('CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT NOT NULL)')
    await db.executeBatch(
      'INSERT INTO accounts (id, owner) VALUES (?, ?)',
      Array.from({ length: ACCOUNTS }, (_, at) => [at + 1, ownerOf(at + 1)]),
    )

    const key = randomBytes(32)
    let changed = false
    const tally: ReadTally = { served: 0, refused: 0, failures: [] }
    const lanes = Array.from({ length: READ_LANES }, () => readAccounts(db, () => changed, tally))
    const status = await db.changeEncryption({ key: () => ({ key: Uint8Array.from(key), id: 'accounts-key' }) })
    changed = true
    await Promise.all(lanes)
    const servedBeforeLastLane = tally.served
    await readAccounts(db, () => tally.served > servedBeforeLastLane, tally)

    expect(status.change).toMatchObject({ state: 'done' })
    expect(status.change.reopenError).toBeUndefined()
    expect(tally.failures).toEqual([])
    expect(tally.served).toBeGreaterThan(READ_LANES)
    expect(
      readWithIndependentAegis<Row>(path, key, 'SELECT id, owner FROM accounts ORDER BY id').map(row => ({
        id: Number(row.id),
        owner: row.owner,
      })),
    ).toEqual(Array.from({ length: ACCOUNTS }, (_, at) => ({ id: at + 1, owner: ownerOf(at + 1) })))
  }, 60_000)
})
