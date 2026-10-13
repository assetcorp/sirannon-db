import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SynchronousLevel } from '../../../core/driver/types.js'
import type { BulkLoadDurability } from '../../../core/types.js'
import {
  countRaw,
  createWorkspace,
  distinctLevels,
  journalFormat,
  RECORD_LEVEL,
  recordLevel,
  rowsFrom,
  SQLITE_SYNCHRONOUS,
  type Workspace,
  WRITER_PLACEMENTS,
  walBytes,
  walCommitCount,
} from './harness.js'

const CONFIGURED_LEVELS: SynchronousLevel[] = ['off', 'normal', 'full', 'extra']
const LOAD_DURABILITIES: (BulkLoadDurability | undefined)[] = [undefined, 'off', 'normal']
const ROWS_PER_LOAD = 200
const LARGE_LOAD_ROWS = 100_000
const UNKNOWN_DURABILITIES: unknown[] = ['full', 'extra', 'OFF', 'Normal', '', 'off ', 0, 1, true, {}]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('durability')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(WRITER_PLACEMENTS)('bulk load durability with $label', ({ writerWorker }) => {
  it('loads every row in one commit at the durability asked for, and commits the next write at the configured level', async () => {
    for (const configured of CONFIGURED_LEVELS) {
      for (const durability of LOAD_DURABILITIES) {
        const name = `${configured}-${durability ?? 'default'}`
        const path = workspace.path(name)
        const db = await workspace.open(name, { synchronous: configured, writerWorker })
        const commitsBefore = walCommitCount(path)

        const summary = await db.bulkLoad(RECORD_LEVEL, rowsFrom(1, ROWS_PER_LOAD, 'load'), {
          checkpoint: false,
          ...(durability === undefined ? {} : { durability }),
        })
        const commitsForLoad = walCommitCount(path) - commitsBefore
        await recordLevel(db, ROWS_PER_LOAD + 1, 'after')

        expect(summary, name).toEqual({ rowsLoaded: ROWS_PER_LOAD, changes: ROWS_PER_LOAD })
        expect(commitsForLoad, name).toBe(1)
        expect(countRaw(path), name).toBe(ROWS_PER_LOAD + 1)
        expect(distinctLevels(path, 'load'), name).toEqual([SQLITE_SYNCHRONOUS[durability ?? 'off']])
        expect(distinctLevels(path, 'after'), name).toEqual([SQLITE_SYNCHRONOUS[configured]])
        await db.close()
      }
    }

    const largePath = workspace.path('large')
    const large = await workspace.open('large', { synchronous: 'full', writerWorker })
    const commitsBefore = walCommitCount(largePath)
    await expect(
      large.bulkLoad(RECORD_LEVEL, rowsFrom(1, LARGE_LOAD_ROWS, 'large'), { checkpoint: false }),
    ).resolves.toEqual({ rowsLoaded: LARGE_LOAD_ROWS, changes: LARGE_LOAD_ROWS })
    expect(walCommitCount(largePath) - commitsBefore).toBe(1)
    expect(countRaw(largePath)).toBe(LARGE_LOAD_ROWS)
    expect(distinctLevels(largePath, 'large')).toEqual([SQLITE_SYNCHRONOUS.off])
  })

  it('truncates the WAL after a load that changes rows, and leaves it growing across loads that skip the checkpoint', async () => {
    const path = workspace.path('wal')
    const db = await workspace.open('wal', { synchronous: 'full', writerWorker })

    await db.bulkLoad(RECORD_LEVEL, rowsFrom(1, ROWS_PER_LOAD, 'first'))
    expect(walBytes(path)).toBe(0)

    await db.bulkLoad(RECORD_LEVEL, rowsFrom(1_001, ROWS_PER_LOAD, 'deferred'), { checkpoint: false })
    const afterFirstDeferred = walBytes(path) ?? 0
    await recordLevel(db, 1_500, 'between')
    await db.bulkLoad(RECORD_LEVEL, rowsFrom(2_001, ROWS_PER_LOAD, 'deferred'), { checkpoint: false })
    const afterSecondDeferred = walBytes(path) ?? 0
    expect(afterFirstDeferred).toBeGreaterThan(0)
    expect(afterSecondDeferred).toBeGreaterThan(afterFirstDeferred)
    expect(distinctLevels(path, 'between')).toEqual([SQLITE_SYNCHRONOUS.full])

    const nothingChanged = await Promise.all([
      db.bulkLoad(RECORD_LEVEL, []),
      db.bulkLoad(RECORD_LEVEL.replace('INSERT INTO', 'INSERT OR IGNORE INTO'), rowsFrom(1, 3, 'duplicate')),
    ])
    expect(nothingChanged).toEqual([
      { rowsLoaded: 0, changes: 0 },
      { rowsLoaded: 3, changes: 0 },
    ])
    expect(walBytes(path)).toBe(afterSecondDeferred)

    await db.bulkLoad(RECORD_LEVEL, rowsFrom(3_001, ROWS_PER_LOAD, 'last'))
    expect(walBytes(path)).toBe(0)
    expect(countRaw(path)).toBe(ROWS_PER_LOAD * 4 + 1)

    const rollbackPath = workspace.path('rollback-journal')
    const rollback = await workspace.open('rollback-journal', { synchronous: 'full', walMode: false, writerWorker })
    await expect(rollback.bulkLoad(RECORD_LEVEL, rowsFrom(1, ROWS_PER_LOAD, 'load'))).resolves.toEqual({
      rowsLoaded: ROWS_PER_LOAD,
      changes: ROWS_PER_LOAD,
    })
    await recordLevel(rollback, ROWS_PER_LOAD + 1, 'after')
    expect(walBytes(rollbackPath)).toBeNull()
    expect(journalFormat(rollbackPath)).toBe('rollback')
    expect(distinctLevels(rollbackPath, 'load')).toEqual([SQLITE_SYNCHRONOUS.off])
    expect(distinctLevels(rollbackPath, 'after')).toEqual([SQLITE_SYNCHRONOUS.full])
  })

  it('commits every write made during a load at the configured level, and runs overlapping loads one after another', async () => {
    const path = workspace.path('concurrent')
    const db = await workspace.open('concurrent', { synchronous: 'extra', writerWorker })

    const settled = await Promise.allSettled([
      db.bulkLoad(RECORD_LEVEL, rowsFrom(1, 2_000, 'load-off'), { durability: 'off' }),
      db.execute(RECORD_LEVEL, [10_001, 'execute', 0]),
      db.executeBatch(RECORD_LEVEL, [
        [10_002, 'batch', 0],
        [10_003, 'batch', 0],
      ]),
      db.executeTransaction([{ sql: RECORD_LEVEL, params: [10_004, 'list', 0] }]),
      db.transaction(tx => tx.execute(RECORD_LEVEL, [10_005, 'callback', 0])),
      db.bulkLoad(RECORD_LEVEL, rowsFrom(20_001, 500, 'load-normal'), { durability: 'normal' }),
      db.bulkLoad(RECORD_LEVEL, rowsFrom(30_001, 500, 'load-default')),
      db.execute(RECORD_LEVEL, [10_006, 'execute', 0]),
    ])

    expect(settled.map(outcome => outcome.status)).toEqual(Array(8).fill('fulfilled'))
    expect(countRaw(path)).toBe(2_000 + 500 + 500 + 6)
    expect(distinctLevels(path, 'load-off')).toEqual([SQLITE_SYNCHRONOUS.off])
    expect(distinctLevels(path, 'load-normal')).toEqual([SQLITE_SYNCHRONOUS.normal])
    expect(distinctLevels(path, 'load-default')).toEqual([SQLITE_SYNCHRONOUS.off])
    for (const source of ['execute', 'batch', 'list', 'callback']) {
      expect(distinctLevels(path, source), source).toEqual([SQLITE_SYNCHRONOUS.extra])
    }
  })

  it('refuses an unknown durability with INVALID_DURABILITY and any load on a read-only database, writing nothing', async () => {
    const path = workspace.path('refusals')
    const db = await workspace.open('refusals', { synchronous: 'full', writerWorker })

    for (const durability of UNKNOWN_DURABILITIES) {
      await expect(
        db.bulkLoad(RECORD_LEVEL, rowsFrom(1, 10, 'refused'), { durability: durability as BulkLoadDurability }),
        String(durability),
      ).rejects.toMatchObject({ code: 'INVALID_DURABILITY' })
    }
    await recordLevel(db, 1_000, 'after')
    expect(countRaw(path)).toBe(1)
    expect(distinctLevels(path, 'after')).toEqual([SQLITE_SYNCHRONOUS.full])
    await db.close()

    const readOnly = await workspace.registry().open('read-only', path, { readOnly: true, writerWorker })
    await expect(readOnly.bulkLoad(RECORD_LEVEL, rowsFrom(1, 10, 'refused'))).rejects.toMatchObject({
      code: 'READ_ONLY',
    })
    expect(countRaw(path)).toBe(1)
  })
})
