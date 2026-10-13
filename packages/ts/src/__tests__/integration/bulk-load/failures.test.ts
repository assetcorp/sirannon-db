import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Params } from '../../../core/types.js'
import {
  armExecFault,
  armedExecFaultDriver,
  execFaultArmed,
} from '../../helpers/fault-drivers/armed-exec-fault-driver.js'
import { checkpointObservingDriver } from '../../helpers/fault-drivers/checkpoint-observing-driver.js'
import { exitingMidBatchDriver, exitsMidBatch } from '../../helpers/fault-drivers/exiting-mid-batch-driver.js'
import {
  columnRaw,
  countRaw,
  createWorkspace,
  distinctLevels,
  RECORD_LEVEL,
  recordLevel,
  rowsFrom,
  SQLITE_SYNCHRONOUS,
  type Workspace,
  WRITER_PLACEMENTS,
  walBytes,
} from './harness.js'

const SEEDED = 50
const LOAD_SIZE = 400

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('failures')
})

afterEach(async () => {
  await workspace.dispose()
})

function failingLoads(): [label: string, sql: string, rows: Params[], code: string][] {
  const valid = rowsFrom(1_001, LOAD_SIZE, 'refused')
  return [
    ['a duplicate key in the last row', RECORD_LEVEL, [...valid, [1_001, 'refused', 0]], 'QUERY_ERROR'],
    ['a key already stored', RECORD_LEVEL, [...valid.slice(0, 10), [1, 'refused', 0]], 'QUERY_ERROR'],
    ['a null where the column refuses one', RECORD_LEVEL, [...valid.slice(0, 10), [5_000, null, 0]], 'QUERY_ERROR'],
    ['a row with too few values', RECORD_LEVEL, [...valid.slice(0, 10), [5_000, 'refused']], 'QUERY_ERROR'],
    [
      'a row with a value SQLite cannot bind',
      RECORD_LEVEL,
      [...valid.slice(0, 10), [5_000, 'refused', {}]],
      'QUERY_ERROR',
    ],
    ['a missing table', 'INSERT INTO missing (id) VALUES (?)', [[1]], 'QUERY_ERROR'],
    ['a syntax error', 'INSERT INTO readings VALUES (?', [[1]], 'QUERY_ERROR'],
    ['a reserved table', 'INSERT INTO _sirannon_changes (table_name) VALUES (?)', [['readings']], 'FORBIDDEN_SQL'],
  ]
}

describe.each(WRITER_PLACEMENTS)('bulk load failures with $label', ({ writerWorker }) => {
  it('rolls back a load that fails on any row, restores the configured level, and accepts the corrected load', async () => {
    const path = workspace.path('rollback')
    const db = await workspace.open('rollback', { synchronous: 'full', writerWorker })
    await db.bulkLoad(RECORD_LEVEL, rowsFrom(1, SEEDED, 'seed'))
    const seeded = columnRaw(path, 'SELECT id FROM readings ORDER BY id')

    for (const [index, [label, sql, rows, code]] of failingLoads().entries()) {
      await expect(db.bulkLoad(sql, rows), label).rejects.toMatchObject({ code })
      await recordLevel(db, 10_000 + index, 'after-failure')
      expect(columnRaw(path, "SELECT id FROM readings WHERE source != 'after-failure' ORDER BY id"), label).toEqual(
        seeded,
      )
    }
    expect(distinctLevels(path, 'after-failure')).toEqual([SQLITE_SYNCHRONOUS.full])

    await expect(db.bulkLoad(RECORD_LEVEL, rowsFrom(1_001, LOAD_SIZE, 'corrected'))).resolves.toEqual({
      rowsLoaded: LOAD_SIZE,
      changes: LOAD_SIZE,
    })
    expect(distinctLevels(path, 'corrected')).toEqual([SQLITE_SYNCHRONOUS.off])
    expect(countRaw(path)).toBe(SEEDED + failingLoads().length + LOAD_SIZE)
  })

  it('reports DURABILITY_RESTORE_FAILED when restoring the level fails after a committed load, and the load error when the load failed as well', async () => {
    const path = workspace.path('restore')
    const config = { statement: 'PRAGMA synchronous = FULL', armFile: `${path}.armed` }
    const db = await workspace.open(
      'restore',
      { synchronous: 'full', writerWorker },
      { driver: armedExecFaultDriver(config) },
    )

    armExecFault(config)
    await expect(db.bulkLoad(RECORD_LEVEL, rowsFrom(1, LOAD_SIZE, 'committed'))).rejects.toMatchObject({
      code: 'DURABILITY_RESTORE_FAILED',
    })
    expect(execFaultArmed(config)).toBe(false)
    expect(countRaw(path)).toBe(LOAD_SIZE)
    await recordLevel(db, 3_000, 'after-failed-restore')
    expect(distinctLevels(path, 'after-failed-restore')).toEqual([SQLITE_SYNCHRONOUS.full])

    armExecFault(config)
    await expect(db.bulkLoad(RECORD_LEVEL, [[1, 'duplicate', 0]])).rejects.toMatchObject({ code: 'QUERY_ERROR' })
    expect(execFaultArmed(config)).toBe(false)
    expect(countRaw(path)).toBe(LOAD_SIZE + 1)

    await expect(db.bulkLoad(RECORD_LEVEL, rowsFrom(1_001, 10, 'clean'))).resolves.toEqual({
      rowsLoaded: 10,
      changes: 10,
    })
    await recordLevel(db, 2_000, 'after-clean-load')
    expect(distinctLevels(path, 'after-clean-load')).toEqual([SQLITE_SYNCHRONOUS.full])
  })

  it('refuses every later write while the configured level stays unrestorable after a bulk load, commits none of them, and commits the next one at that level once it restores', async () => {
    const path = workspace.path('unrestorable')
    const config = { statement: 'PRAGMA synchronous = FULL', armFile: `${path}.armed` }
    const db = await workspace.open(
      'unrestorable',
      { synchronous: 'full', writerWorker },
      { driver: armedExecFaultDriver(config) },
    )
    armExecFault(config)
    await expect(db.bulkLoad(RECORD_LEVEL, rowsFrom(1, LOAD_SIZE, 'committed'))).rejects.toMatchObject({
      code: 'DURABILITY_RESTORE_FAILED',
    })

    const refusals: [string, () => Promise<unknown>][] = [
      ['execute', () => recordLevel(db, 5_000, 'refused')],
      ['executeBatch', () => db.executeBatch(RECORD_LEVEL, [[5_001, 'refused', 0]])],
      ['executeTransaction', () => db.executeTransaction([{ sql: RECORD_LEVEL, params: [5_002, 'refused', 0] }])],
      ['transaction', () => db.transaction(tx => tx.execute(RECORD_LEVEL, [5_003, 'refused', 0]))],
      ['bulkLoad', () => db.bulkLoad(RECORD_LEVEL, [[5_004, 'refused', 0]])],
      ['migrate', () => db.migrate([{ version: 1, name: 'refused', up: 'CREATE TABLE refused (id INTEGER)' }])],
    ]
    for (const [label, write] of refusals) {
      armExecFault(config)
      await expect(write(), label).rejects.toMatchObject({ code: 'DURABILITY_RESTORE_FAILED' })
      expect(execFaultArmed(config), label).toBe(false)
    }
    expect(countRaw(path)).toBe(LOAD_SIZE)
    expect(columnRaw(path, "SELECT name FROM sqlite_master WHERE name = 'refused'")).toEqual([])

    await recordLevel(db, 6_000, 'restored')
    await db.executeBatch(RECORD_LEVEL, [[6_001, 'restored', 0]])
    expect(distinctLevels(path, 'restored')).toEqual([SQLITE_SYNCHRONOUS.full])
    expect(countRaw(path)).toBe(LOAD_SIZE + 2)
  })

  it('refuses a load issued inside a transaction callback, a nested callback, or a migration with BULK_LOAD_IN_TRANSACTION, writes none of it, and leaves the transaction around it to commit at the configured level', async () => {
    const path = workspace.path('nested')
    const db = await workspace.open('nested', { synchronous: 'full', writerWorker })
    const codeOf = (load: Promise<unknown>) =>
      load.then(
        () => 'loaded',
        (err: { code?: string }) => err.code,
      )

    const nestedLoads = await db.transaction(async tx => {
      await tx.execute(RECORD_LEVEL, [1, 'callback', 0])
      const direct = await codeOf(db.bulkLoad(RECORD_LEVEL, rowsFrom(100, 5, 'nested')))
      const deeper = await db.transaction(() => codeOf(db.bulkLoad(RECORD_LEVEL, rowsFrom(200, 5, 'nested'))))
      const invalid = await codeOf(
        db.bulkLoad(RECORD_LEVEL, rowsFrom(300, 5, 'nested'), { durability: 'full' as 'off' }),
      )
      await tx.executeBatch(RECORD_LEVEL, [[2, 'callback', 0]])
      return [direct, deeper, invalid]
    })
    let migrationLoad: string | undefined
    await db.migrate([
      {
        version: 1,
        name: 'load_inside_migration',
        up: async () => {
          migrationLoad = await codeOf(db.bulkLoad(RECORD_LEVEL, rowsFrom(400, 5, 'nested')))
        },
      },
    ])

    expect(nestedLoads).toEqual(Array(3).fill('BULK_LOAD_IN_TRANSACTION'))
    expect(migrationLoad).toBe('BULK_LOAD_IN_TRANSACTION')
    expect(columnRaw(path, 'SELECT id FROM readings ORDER BY id')).toEqual([1n, 2n])
    expect(distinctLevels(path, 'callback')).toEqual([SQLITE_SYNCHRONOUS.full])
    await recordLevel(db, 3, 'after')
    expect(distinctLevels(path, 'after')).toEqual([SQLITE_SYNCHRONOUS.full])
  })

  it('refuses a parameter list that is not a list of rows with QUERY_ERROR, writing nothing', async () => {
    const path = workspace.path('malformed')
    const db = await workspace.open('malformed', { synchronous: 'full', writerWorker })

    for (const paramsBatch of [null, 'rows', [5], [null], [[1, 'kept?', 0], 7]] as unknown as Params[][]) {
      await expect(db.bulkLoad(RECORD_LEVEL, paramsBatch), JSON.stringify(paramsBatch)).rejects.toMatchObject({
        code: 'QUERY_ERROR',
      })
    }
    await recordLevel(db, 1, 'after')
    expect(countRaw(path)).toBe(1)
    expect(distinctLevels(path, 'after')).toEqual([SQLITE_SYNCHRONOUS.full])
  })
})

describe('the checkpoint after a bulk load', () => {
  it('retries while a reader holds the WAL, resolves the committed load either way, and truncates the WAL once the reader lets go', async () => {
    for (const readerLetsGoBeforeAttempt of [2, null]) {
      const name = `reader-${readerLetsGoBeforeAttempt ?? 'stays'}`
      const path = workspace.path(name)
      let reader: BetterSqlite3.Database | null = null
      let attempts = 0
      const driver = checkpointObservingDriver(
        () => {
          if (reader === null) return
          attempts++
          if (attempts === readerLetsGoBeforeAttempt) reader.close()
        },
        { busyTimeout: 0 },
      )
      const db = await workspace.open(name, { synchronous: 'full' }, { driver })
      await db.bulkLoad(RECORD_LEVEL, rowsFrom(1, SEEDED, 'seed'))
      expect(walBytes(path), name).toBe(0)
      reader = new BetterSqlite3(path, { readonly: true })
      reader.exec('BEGIN')
      reader.prepare('SELECT count(*) FROM readings').get()

      await expect(db.bulkLoad(RECORD_LEVEL, rowsFrom(1_001, LOAD_SIZE, 'load'))).resolves.toEqual({
        rowsLoaded: LOAD_SIZE,
        changes: LOAD_SIZE,
      })

      expect(countRaw(path), name).toBe(SEEDED + LOAD_SIZE)
      if (readerLetsGoBeforeAttempt === null) {
        expect(attempts, name).toBeGreaterThan(1)
        expect(walBytes(path), name).toBeGreaterThan(0)
        reader.close()
        reader = null
        await db.bulkLoad(RECORD_LEVEL, rowsFrom(5_001, 1, 'after-reader'))
      } else {
        expect(attempts, name).toBe(readerLetsGoBeforeAttempt)
      }
      expect(walBytes(path), name).toBe(0)
    }
  })
})

describe('a writer worker that dies during a bulk load', () => {
  it('rolls back the rows that it loaded before dying, and the restarted worker writes at the configured level and accepts the same load again', async () => {
    const path = workspace.path('crash')
    const db = await workspace.open(
      'crash',
      { synchronous: 'full', writerWorker: { writeTimeoutMs: 10_000 } },
      { driver: exitingMidBatchDriver() },
    )
    await db.bulkLoad(RECORD_LEVEL, rowsFrom(1, SEEDED, 'seed'))

    await expect(db.bulkLoad(exitsMidBatch(RECORD_LEVEL), rowsFrom(1_001, LOAD_SIZE, 'load'))).rejects.toMatchObject({
      code: 'WRITER_WORKER_EXIT',
    })
    expect(columnRaw(path, 'PRAGMA integrity_check')).toEqual(['ok'])
    expect(countRaw(path)).toBe(SEEDED)
    await recordLevel(db, 900, 'restarted')
    expect(distinctLevels(path, 'restarted')).toEqual([SQLITE_SYNCHRONOUS.full])

    await expect(db.bulkLoad(RECORD_LEVEL, rowsFrom(1_001, LOAD_SIZE, 'load'))).resolves.toEqual({
      rowsLoaded: LOAD_SIZE,
      changes: LOAD_SIZE,
    })
    expect(distinctLevels(path, 'load')).toEqual([SQLITE_SYNCHRONOUS.off])
    expect(countRaw(path)).toBe(SEEDED + 1 + LOAD_SIZE)
  }, 30_000)
})
