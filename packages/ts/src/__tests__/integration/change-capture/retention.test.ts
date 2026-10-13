import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { pruneRacingDriver } from '../../helpers/fault-drivers/prune-racing-driver.js'
import {
  collect,
  createWorkspace,
  FAST_POLL_MS,
  pause,
  readRaw,
  type Workspace,
  waitFor,
  waitForCount,
  writeRaw,
} from './harness.js'

const HOUR_MS = 3_600_000
const SLOW_POLL_MS = 700

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('retention')
})

afterEach(async () => {
  await workspace.dispose()
})

function changeLogSize(path: string): number {
  return Number(readRaw<{ n: bigint }>(path, 'SELECT count(*) AS n FROM _sirannon_changes')[0].n)
}

describe('change-log retention', () => {
  it('prunes delivered changes once they pass the retention window, including a window of zero or less', async () => {
    const retentions = [
      { name: 'zero', cdcRetention: 0 },
      { name: 'negative', cdcRetention: -HOUR_MS },
      { name: 'hour', cdcRetention: HOUR_MS },
    ]
    const writes = 40
    for (const { name, cdcRetention } of retentions) {
      const db = await workspace.open(name, { cdcRetention, cdcPollInterval: 1 })
      await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
      await db.watch('orders')
      const seen = collect(db.on('orders'))
      for (let id = 1; id <= writes; id++) await db.execute('INSERT INTO orders VALUES (?)', [id])
      await waitForCount(seen, writes)
      expect(seen.events.map(event => event.row.id)).toEqual(Array.from({ length: writes }, (_, index) => index + 1))
    }

    await waitFor(
      () => changeLogSize(workspace.path('zero')) === 0 && changeLogSize(workspace.path('negative')) === 0,
      'the delivered changes to leave the change log',
    )
    expect(changeLogSize(workspace.path('hour'))).toBe(writes)
  })

  it('keeps a change that commits between a poll that found the change log empty and the prune that follows it', async () => {
    const db = await workspace.open(
      'racing',
      { cdcRetention: -HOUR_MS, cdcPollInterval: 1 },
      pruneRacingDriver("INSERT INTO orders VALUES (1, 'committed while the log was pruned')"),
    )
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)')
    await db.watch('orders')
    const seen = collect(db.on('orders'))

    await waitForCount(seen, 1)
    expect(seen.events.map(event => event.row)).toEqual([{ id: 1, note: 'committed while the log was pruned' }])
  })
})

describe('the poll interval', () => {
  it('sets how soon a subscriber hears of a change, and an interval of zero still delivers', async () => {
    const slow = await workspace.open('slow', { cdcPollInterval: SLOW_POLL_MS })
    const instant = await workspace.open('instant', { cdcPollInterval: 0 })
    for (const db of [slow, instant]) {
      await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
      await db.watch('orders')
    }
    const slowSeen = collect(slow.on('orders'))
    const instantSeen = collect(instant.on('orders'))

    await slow.execute('INSERT INTO orders VALUES (1)')
    await instant.execute('INSERT INTO orders VALUES (1)')
    await pause(SLOW_POLL_MS / 3)
    expect([slowSeen.events.length, instantSeen.events.length]).toEqual([0, 1])

    await waitForCount(slowSeen, 1)
    expect(slowSeen.events.map(event => event.row.id)).toEqual([1])
  })
})

describe('closing and reopening a database', () => {
  it('loses no change, whether it committed before the close, while the database was closed, or after it reopened', async () => {
    const registry = workspace.registry()
    const path = workspace.path('reopened')
    const db = await registry.open('reopened', path, { cdcPollInterval: FAST_POLL_MS })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, phase TEXT NOT NULL)')
    await db.watch('orders')
    const beforeClose = collect(db.on('orders'))

    const inFlight = Array.from({ length: 30 }, (_, index) =>
      db.execute('INSERT INTO orders VALUES (?, ?)', [index + 1, 'in flight']),
    )
    const closing = registry.close('reopened')
    const outcomes = await Promise.allSettled(inFlight)
    await closing
    for (const outcome of outcomes) {
      if (outcome.status === 'rejected') expect(outcome.reason).toMatchObject({ code: 'DATABASE_CLOSED' })
    }
    writeRaw(path, "INSERT INTO orders VALUES (101, 'while closed'), (102, 'while closed')")

    const reopened = await registry.open('reopened', path, { cdcPollInterval: FAST_POLL_MS })
    const afterReopen = collect(reopened.on('orders'))
    await reopened.watch('orders')
    await reopened.execute("INSERT INTO orders VALUES (201, 'after reopening')")

    const committed = readRaw<{ id: bigint }>(path, 'SELECT id FROM orders ORDER BY id').map(row => Number(row.id))
    const fulfilled = outcomes.flatMap((outcome, index) => (outcome.status === 'fulfilled' ? [index + 1] : []))
    expect(committed).toEqual([...fulfilled, 101, 102, 201])
    await waitFor(() => afterReopen.events.some(event => event.row.id === 201), 'the write after reopening')

    const seenAfter = afterReopen.events.map(event => Number(event.row.id))
    const seenAnywhere = new Set([...beforeClose.events.map(event => Number(event.row.id)), ...seenAfter])
    expect([...seenAnywhere].sort((a, b) => a - b)).toEqual(committed)
    expect(new Set(seenAfter).size).toBe(seenAfter.length)
    expect(seenAfter.slice(-3)).toEqual([101, 102, 201])
    for (let index = 1; index < afterReopen.events.length; index++) {
      expect(afterReopen.events[index].seq > afterReopen.events[index - 1].seq).toBe(true)
    }
  })
})
