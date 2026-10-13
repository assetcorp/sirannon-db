import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { ChangeEvent } from '../../../core/types.js'
import { createWorkspace, RECORD_LEVEL, readRaw, rowsFrom, type Workspace, WRITER_PLACEMENTS } from './harness.js'

const LOADED_ROWS = 300
const QUIET_PERIOD_MS = 200
const WAIT_LIMIT_MS = 6_000

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('change-events')
})

afterEach(async () => {
  await workspace.dispose()
})

function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function settledEvents(events: ChangeEvent[], count: number): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (events.length < count) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${count} change events`)
    await pause(5)
  }
  await pause(QUIET_PERIOD_MS)
}

describe.each(WRITER_PLACEMENTS)('change events from a bulk load with $label', ({ writerWorker }) => {
  it('emits one insert event per loaded row in load order with the row as stored, and none for a load that rolls back', async () => {
    const path = workspace.path('watched')
    const db = await workspace.open('watched', { writerWorker, cdcPollInterval: 5 })
    await db.watch('readings')
    const events: ChangeEvent[] = []
    db.on('readings').subscribe(event => events.push(event))

    await expect(db.bulkLoad(RECORD_LEVEL, [...rowsFrom(1, 50, 'refused'), [1, 'refused', 0]])).rejects.toMatchObject({
      code: 'QUERY_ERROR',
    })
    await db.bulkLoad(RECORD_LEVEL, rowsFrom(1, LOADED_ROWS, 'load'), { checkpoint: false })
    await settledEvents(events, LOADED_ROWS)

    const stored = readRaw<{ id: bigint; source: string; level: bigint; value: number }>(
      path,
      'SELECT id, source, level, value FROM readings ORDER BY id',
    ).map(row => ({ id: Number(row.id), source: row.source, level: Number(row.level), value: row.value }))
    expect(stored).toHaveLength(LOADED_ROWS)
    expect(events.map(event => event.type)).toEqual(Array(LOADED_ROWS).fill('insert'))
    expect(events.map(event => event.table)).toEqual(Array(LOADED_ROWS).fill('readings'))
    expect(events.map(event => event.row)).toEqual(stored)
    expect(new Set(events.map(event => event.txId)).size).toBe(1)
  })
})
