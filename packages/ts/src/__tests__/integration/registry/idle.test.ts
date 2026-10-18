import { rmSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { connectionsHeldOn, freshDirectory, readTotals } from './sqlite-oracle.js'

const IDLE_TIMEOUT_MS = 200
const CLOSE_DEADLINE_MS = 3_000
const KEEP_ALIVE_PERIOD_MS = 40
const KEEP_ALIVE_SPAN_MS = IDLE_TIMEOUT_MS * 4
const LONGEST_CHECK_GAP_MS = 400
const ONE_HOUR_MS = 3_600_000

let dir: string
const registries: Sirannon[] = []

function tenantFile(id: string): string {
  return join(dir, `${id}.db`)
}

function registry(idleTimeout: number): Sirannon {
  const sirannon = new Sirannon({
    driver: betterSqlite3(),
    lifecycle: { idleTimeout, autoOpen: { resolver: id => ({ path: tenantFile(id) }) } },
  })
  registries.push(sirannon)
  return sirannon
}

async function resolved(sirannon: Sirannon, id: string): Promise<Database> {
  const db = await sirannon.resolve(id)
  if (db === undefined) throw new Error(`the resolver returned no database for '${id}'`)
  return db
}

function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function closedAndReleased(sirannon: Sirannon, id: string): Promise<void> {
  await vi.waitFor(
    () => {
      expect(sirannon.has(id)).toBe(false)
      expect(connectionsHeldOn(tenantFile(id))).toBe(false)
    },
    { timeout: CLOSE_DEADLINE_MS, interval: 25 },
  )
}

beforeEach(() => {
  dir = freshDirectory('idle')
})

afterEach(async () => {
  vi.useRealTimers()
  for (const sirannon of registries.splice(0)) await sirannon.shutdown()
  rmSync(dir, { recursive: true, force: true })
})

describe('closing idle databases', () => {
  it('closes a database that nobody reaches for longer than the idle timeout, and opens it again on the next resolve', async () => {
    const sirannon = registry(IDLE_TIMEOUT_MS)
    const first = await resolved(sirannon, 'acme')
    await first.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
    await first.execute('INSERT INTO orders (total) VALUES (12)')

    await closedAndReleased(sirannon, 'acme')

    expect(first.closed).toBe(true)
    expect(readTotals(tenantFile('acme'))).toEqual([12])
    const second = await resolved(sirannon, 'acme')
    expect(second).not.toBe(first)
    expect(await second.query('SELECT total FROM orders')).toEqual([{ total: 12 }])
  })

  it('keeps a database open while callers keep reaching it through get or resolve, and closes its idle neighbour', async () => {
    const sirannon = registry(IDLE_TIMEOUT_MS)
    await resolved(sirannon, 'busy')
    await resolved(sirannon, 'quiet')

    const started = performance.now()
    let reachedThroughGet = true
    while (performance.now() - started < KEEP_ALIVE_SPAN_MS) {
      await pause(KEEP_ALIVE_PERIOD_MS)
      if (reachedThroughGet) sirannon.get('busy')
      else await sirannon.resolve('busy')
      reachedThroughGet = !reachedThroughGet
    }

    await closedAndReleased(sirannon, 'quiet')
    expect(sirannon.has('busy')).toBe(true)
    expect(connectionsHeldOn(tenantFile('busy'))).toBe(true)
  })

  it('never closes a database when the idle timeout is zero, negative, or not a number, and closes nothing early for a timeout past the timer limit', async () => {
    const warnings: string[] = []
    const recordWarning = (warning: Error): void => {
      warnings.push(warning.name)
    }
    process.on('warning', recordWarning)
    const settings = [0, -1, Number.NEGATIVE_INFINITY, Number.NaN, Number.MAX_SAFE_INTEGER]
    try {
      const opened = await Promise.all(
        settings.map(async (idleTimeout, at) => {
          const sirannon = registry(idleTimeout)
          await resolved(sirannon, `tenant-${at}`)
          return sirannon
        }),
      )

      await pause(LONGEST_CHECK_GAP_MS)

      settings.forEach((idleTimeout, at) => {
        expect(opened[at]?.has(`tenant-${at}`), String(idleTimeout)).toBe(true)
        expect(connectionsHeldOn(tenantFile(`tenant-${at}`)), String(idleTimeout)).toBe(true)
      })
      expect(warnings).toEqual([])
    } finally {
      process.off('warning', recordWarning)
    }
  })

  it('still closes an idle database when the wall clock stands still or jumps backwards', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const sirannon = registry(IDLE_TIMEOUT_MS)
    await resolved(sirannon, 'acme')
    vi.setSystemTime(Date.now() - ONE_HOUR_MS)

    await closedAndReleased(sirannon, 'acme')
  })
})
