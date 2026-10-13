import { writeFileSync } from 'node:fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { COPY_STALL_MS, stallingCopyDriver } from '../../helpers/fault-drivers/stalling-copy-driver.js'
import { memoryDestination } from '../../helpers/memory-destination.js'
import {
  columnRaw,
  createWorkspace,
  HELD_WRITE_DEADLINE_MS,
  integrityRaw,
  nextTurn,
  type Workspace,
} from './harness.js'

const SEEDED_ROWS = 20_000
const PADDED_NAME_LENGTH = 200
const INSERT_ITEM = 'INSERT INTO items (name) VALUES (?)'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('backup')
})

afterEach(async () => {
  await workspace.dispose()
})

function restoredCopy(name: string, bytes: Uint8Array): string {
  const path = workspace.path(`${name}-restored.db`)
  writeFileSync(path, bytes)
  return path
}

describe('a backup copy that runs on the writer worker', () => {
  it('lets writes commit between the steps of a copy and produces a consistent database', async () => {
    const path = workspace.path('busy.db')
    const db = await workspace.registry().open('busy', path, { synchronous: 'full', writerWorker: true })
    await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
    await db.bulkLoad(
      INSERT_ITEM,
      Array.from({ length: SEEDED_ROWS }, (_, index) => [`row-${index}`.padEnd(PADDED_NAME_LENGTH, 'x')]),
    )

    let backingUp = true
    let copySteps = 0
    let committedSinceLastStep = 0
    let writesBetweenCopySteps = 0
    const writer = (async () => {
      while (backingUp) {
        await nextTurn()
        if (!backingUp) break
        await db.execute(INSERT_ITEM, ['written during the backup'])
        committedSinceLastStep++
      }
    })()
    const destination = memoryDestination()
    const report = await db.backupTo({
      destination,
      pagesPerStep: 8,
      stagingDir: workspace.path(''),
      onProgress: progress => {
        if (progress.phase !== 'copy') return
        copySteps++
        if (progress.remainingPages > 0) writesBetweenCopySteps += committedSinceLastStep
        committedSinceLastStep = 0
      },
    })
    backingUp = false
    await writer

    expect(report.restarts).toBe(0)
    expect(copySteps).toBeGreaterThan(10)
    expect(writesBetweenCopySteps).toBeGreaterThan(0)
    const copy = restoredCopy('busy', destination.bytesFor(report.destinationName))
    expect(integrityRaw(copy)).toEqual(['ok'])
    const [copiedRows] = columnRaw(copy, 'SELECT count(*) FROM items') as bigint[]
    const [liveRows] = columnRaw(path, 'SELECT count(*) FROM items') as bigint[]
    expect(copiedRows).toBeGreaterThanOrEqual(BigInt(SEEDED_ROWS + writesBetweenCopySteps))
    expect(copiedRows).toBeLessThanOrEqual(liveRows)
    expect(columnRaw(copy, 'SELECT name FROM items WHERE id = ?', [SEEDED_ROWS])).toEqual(
      columnRaw(path, 'SELECT name FROM items WHERE id = ?', [SEEDED_ROWS]),
    )
  }, 60_000)

  it('finishes a copy whose step outlasts writeTimeoutMs, and stops one that moves no page within stallTimeoutMs while the worker keeps serving', async () => {
    const path = workspace.path('stalling.db')
    const db = await workspace
      .registry({ driver: stallingCopyDriver() })
      .open('stalling', path, { writerWorker: { writeTimeoutMs: HELD_WRITE_DEADLINE_MS } })
    await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
    await db.execute(INSERT_ITEM, ['kettle'])

    const patient = memoryDestination()
    const finished = await db.backupTo({
      destination: patient,
      pagesPerStep: 1,
      stagingDir: workspace.path(''),
      stallTimeoutMs: COPY_STALL_MS * 5,
    })
    const copy = restoredCopy('patient', patient.bytesFor(finished.destinationName))
    expect(integrityRaw(copy)).toEqual(['ok'])
    expect(columnRaw(copy, 'SELECT name FROM items')).toEqual(['kettle'])

    await expect(
      db.backupTo({
        destination: memoryDestination(),
        pagesPerStep: 1,
        stagingDir: workspace.path(''),
        stallTimeoutMs: COPY_STALL_MS / 4,
      }),
    ).rejects.toMatchObject({ code: 'BACKUP_STALLED' })
    const refusals: string[] = []
    for (;;) {
      const outcome = await db.execute(INSERT_ITEM, ['teapot']).then(
        () => 'ok',
        (error: { code?: string }) => String(error.code),
      )
      if (outcome === 'ok') break
      refusals.push(outcome)
      expect(refusals.length).toBeLessThan(10)
    }
    expect(refusals.every(code => code === 'WRITE_OVERLOADED')).toBe(true)
    expect(columnRaw(path, 'SELECT name FROM items ORDER BY id')).toEqual(['kettle', 'teapot'])
  }, 60_000)
})
