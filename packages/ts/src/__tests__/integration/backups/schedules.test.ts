import { chmodSync, existsSync, mkdirSync, realpathSync, statSync, writeFileSync } from 'node:fs'
import { basename, join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { BackupFileReport } from '../../../core/backup/report.js'
import type { Database } from '../../../core/database.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { BackupScheduler } from '../../../utils/backup-scheduler/index.js'
import { failingCopyDriver } from '../../helpers/fault-drivers/failing-copy-driver.js'
import {
  codeOf,
  createWorkspace,
  filesIn,
  integrityOf,
  ORDER_PADDING,
  orderLabels,
  orderRows,
  seedOrders,
  type Workspace,
  watchUnhandledRejections,
} from './harness.js'
import { advanceSeconds, letCopiesFinish, schedule, startScheduleClock } from './schedule-harness.js'

const LONGEST_TIMER_MS = 2_147_483_647

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('schedules')
})

afterEach(async () => {
  vi.useRealTimers()
  await workspace.dispose()
})

async function seeded(name: string): Promise<{ db: Database; labels: string[] }> {
  const db = await workspace.open(name)
  return { db, labels: await seedOrders(db, name, 20) }
}

function copiesIn(dir: string): string[] {
  return filesIn(dir).filter(name => name.startsWith('backup-') && name.endsWith('.db'))
}

describe('settings a schedule refuses', () => {
  it('refuses a cron expression it cannot read, naming the expression and the fault, and creates no directory', async () => {
    const { db } = await seeded('crons')
    const refused: [string, string][] = [
      ['not a cron', 'expected 5 or 6 fields'],
      ['', 'empty'],
      ['   ', 'empty'],
      ['* * *', 'expected 5 or 6 fields'],
      ['* * * * * * *', 'expected 5 or 6 fields'],
      ['60 * * * *', 'out of range'],
      ['0 24 * * *', 'out of range'],
      ['0 0 0 * *', 'out of range'],
      ['0 0 32 * *', 'out of range'],
      ['0 0 * 13 *', 'out of range'],
      ['0 0 * * 8', 'out of range'],
      ['60 * * * * *', 'out of range'],
      ['0 17-9 * * *', 'after end'],
      ['*/0 * * * *', 'step must be at least 1'],
      ['0 0 * FOO *', 'unknown name'],
      ['0 0 L * *', 'unsupported syntax'],
      ['0 0 * * 5#2', 'unsupported syntax'],
      ['0 0 15W * *', 'unsupported syntax'],
      ['0 0 ? * *', 'unsupported syntax'],
      ['@reboot', 'unsupported nickname'],
    ]

    for (const [cron, fault] of refused) {
      const destDir = workspace.path(`refused-${refused.findIndex(([expr]) => expr === cron)}`)
      let thrown: unknown
      try {
        db.scheduleBackup({ cron, destDir })
      } catch (err) {
        thrown = err
      }
      expect(codeOf(thrown), cron).toBe('BACKUP_ERROR')
      expect((thrown as Error).message, cron).toContain(`Invalid cron expression '${cron}'`)
      expect((thrown as Error).message, cron).toContain(fault)
      expect(existsSync(destDir), cron).toBe(false)
    }
  })

  it('refuses an unknown time zone, a callback deadline no timer holds, and a directory it cannot create', async () => {
    const { db } = await seeded('settings')
    const blocker = workspace.path('blocker')
    writeFileSync(blocker, 'a file where a directory has to go')
    const attempts: [string, () => void, string][] = [
      [
        'an unknown zone',
        () => db.scheduleBackup({ cron: '0 0 * * *', destDir: workspace.path('a'), timezone: 'Not/AZone' }),
        "Invalid timezone 'Not/AZone'",
      ],
      [
        'a negative deadline',
        () => db.scheduleBackup({ cron: '0 0 * * *', destDir: workspace.path('b'), onBackupTimeoutMs: -1 }),
        'onBackupTimeoutMs',
      ],
      [
        'a deadline that is no number',
        () => db.scheduleBackup({ cron: '0 0 * * *', destDir: workspace.path('c'), onBackupTimeoutMs: Number.NaN }),
        'onBackupTimeoutMs',
      ],
      [
        'a deadline past the longest timer',
        () =>
          db.scheduleBackup({
            cron: '0 0 * * *',
            destDir: workspace.path('d'),
            onBackupTimeoutMs: LONGEST_TIMER_MS + 1,
          }),
        'onBackupTimeoutMs',
      ],
      [
        'a directory under a file',
        () => db.scheduleBackup({ cron: '0 0 * * *', destDir: join(blocker, 'nested') }),
        'Failed to create backup directory',
      ],
    ]

    for (const [label, attempt, message] of attempts) {
      expect(attempt, label).toThrow(
        expect.objectContaining({ code: 'BACKUP_ERROR', message: expect.stringContaining(message) }),
      )
    }
    for (const accepted of ['@daily', '@hourly', '0 0 * JAN MON', '0 9-17/4 * * 1-5', '0 0,12 * * 7', '30 0 0 1 1 *']) {
      expect(() => db.scheduleBackup({ cron: accepted, destDir: workspace.path('accepted') }), accepted).not.toThrow()
    }
    expect(() =>
      db.scheduleBackup({ cron: '0 0 1 1 *', destDir: workspace.path('edges'), onBackupTimeoutMs: LONGEST_TIMER_MS }),
    ).not.toThrow()
    expect(existsSync(workspace.path('accepted'))).toBe(true)
    await db.close()
    expect(() => db.scheduleBackup({ cron: '0 0 * * *', destDir: workspace.path('closed') })).toThrow(
      expect.objectContaining({ code: 'DATABASE_CLOSED' }),
    )
  })
})

describe('copies taken on a schedule', () => {
  it('writes a copy at each scheduled time that opens as the database, and reports each one', async () => {
    const { db, labels } = await seeded('scheduled')
    const destDir = workspace.path('copies')
    startScheduleClock('2026-03-01T10:00:00.500Z')
    const scheduled = schedule(db, { destDir })

    await advanceSeconds(3)

    expect(scheduled.errors).toEqual([])
    expect(scheduled.reports.length).toBeGreaterThanOrEqual(2)
    expect(copiesIn(destDir)).toEqual(scheduled.reports.map(report => basename(report.destPath)).sort())
    for (const report of scheduled.reports) {
      expect(report).toMatchObject({ databaseId: 'scheduled', sourcePath: workspace.path('scheduled.db'), restarts: 0 })
      expect(report.byteLength).toBe(statSync(report.destPath).size)
      expect(integrityOf(report.destPath)).toBe('ok')
      expect(orderLabels(report.destPath)).toEqual(labels)
    }
  })

  it('keeps the newest maxFiles copies, keeps every copy at zero or below, keeps five by default, and leaves other files alone', async () => {
    const strangers = ['notes.txt', 'other.db', 'backup-notes.txt']
    const cases: [string, number | undefined, number][] = [
      ['two', 2, 2],
      ['zero', 0, 7],
      ['negative', -1, 7],
      ['default', undefined, 5],
    ]
    const runs = []
    for (const [label, maxFiles] of cases) {
      const { db } = await seeded(`rotated-${label}`)
      const destDir = workspace.path(`rotated-${label}`)
      mkdirSync(destDir)
      for (const stranger of strangers) writeFileSync(join(destDir, stranger), 'not a copy')
      runs.push({ db, destDir, maxFiles })
    }
    startScheduleClock('2026-03-01T10:00:00.500Z')
    const schedules = runs.map(run =>
      schedule(run.db, {
        destDir: run.destDir,
        ...(run.maxFiles === undefined ? { maxFiles: undefined } : { maxFiles: run.maxFiles }),
      }),
    )

    await advanceSeconds(7)

    for (const [at, [label, , kept]] of cases.entries()) {
      const run = runs[at]
      const scheduled = schedules[at]
      if (!run || !scheduled) continue
      expect(scheduled.reports, label).toHaveLength(7)
      const newest = scheduled.reports
        .slice(-kept)
        .map(report => basename(report.destPath))
        .sort()
      expect(copiesIn(run.destDir), label).toEqual(newest)
      expect(
        filesIn(run.destDir).filter(name => strangers.includes(name)),
        label,
      ).toEqual([...strangers].sort())
    }
  })

  it('carries on after onBackup throws, rejects, or hangs, passes each failure to onError, and rotates anyway', async () => {
    const watch = watchUnhandledRejections()
    try {
      const behaviours = {
        throws: () => {
          throw new Error('the caller could not store the file')
        },
        rejects: async () => {
          throw new Error('the object store refused the upload')
        },
        hangs: () => new Promise<void>(() => {}),
      }
      const runs = []
      for (const [label, onBackup] of Object.entries(behaviours)) {
        const { db } = await seeded(`callback-${label}`)
        runs.push({ label, db, destDir: workspace.path(`callback-${label}`), onBackup, calls: 0 })
      }
      startScheduleClock('2026-03-01T10:00:00.500Z')
      const schedules = runs.map(run =>
        schedule(run.db, {
          destDir: run.destDir,
          maxFiles: 1,
          onBackupTimeoutMs: 50,
          onBackup: report => {
            run.calls++
            expect(existsSync(report.destPath)).toBe(true)
            return run.onBackup()
          },
        }),
      )

      await advanceSeconds(4)
      await vi.advanceTimersByTimeAsync(100)
      await letCopiesFinish()

      const expected = {
        throws: 'the caller could not store the file',
        rejects: 'the object store refused the upload',
        hangs: 'did not return within 50ms',
      }
      for (const [at, run] of runs.entries()) {
        const errors = schedules[at]?.errors ?? []
        expect(run.calls, run.label).toBeGreaterThanOrEqual(3)
        expect(errors.length, run.label).toBe(run.calls)
        expect(errors[0]?.message, run.label).toContain(expected[run.label as keyof typeof expected])
        expect(copiesIn(run.destDir), run.label).toHaveLength(1)
      }
      expect(watch.reasons).toEqual([])
    } finally {
      watch.stop()
    }
  })

  it('waits on onBackup without a limit at a deadline of zero, and holds the next copy and the rotation until it returns', async () => {
    const { db } = await seeded('unbounded')
    const destDir = workspace.path('unbounded')
    let release: () => void = () => {}
    let calls = 0
    startScheduleClock('2026-03-01T10:00:00.500Z')
    const scheduled = schedule(db, {
      destDir,
      maxFiles: 1,
      onBackupTimeoutMs: 0,
      onBackup: () => {
        calls++
        return new Promise<void>(settle => {
          release = settle
        })
      },
    })

    await advanceSeconds(2)
    await vi.advanceTimersByTimeAsync(700_000)
    await letCopiesFinish()

    expect(calls).toBe(1)
    expect(scheduled.errors).toEqual([])
    expect(copiesIn(destDir)).toHaveLength(1)
    const first = copiesIn(destDir)[0]

    release()
    await advanceSeconds(2)

    expect(calls).toBe(2)
    expect(copiesIn(destDir)).toHaveLength(2)

    release()
    await letCopiesFinish()

    expect(copiesIn(destDir)).toHaveLength(1)
    expect(copiesIn(destDir)[0]).not.toBe(first)
    expect(scheduled.errors).toEqual([])
  })

  it.skipIf(process.platform === 'win32' || process.getuid?.() === 0)(
    'reports a copy it finished even when it cannot list the directory to rotate it, and reports the rotation failure',
    async () => {
      const { db, labels } = await seeded('unlistable')
      const destDir = workspace.path('unlistable')
      mkdirSync(destDir)
      chmodSync(destDir, 0o300)
      try {
        startScheduleClock('2026-03-01T10:00:00.500Z')
        const scheduled = schedule(db, { destDir, maxFiles: 1 })

        await advanceSeconds(2)

        expect(scheduled.reports.length).toBeGreaterThanOrEqual(1)
        expect(scheduled.errors.length).toBe(scheduled.reports.length)
        expect(scheduled.errors.map(codeOf).every(code => code === 'BACKUP_ERROR')).toBe(true)
        expect(scheduled.errors[0]?.message).toContain('Failed to list backup files')
        chmodSync(destDir, 0o700)
        expect(copiesIn(destDir)).toHaveLength(scheduled.reports.length)
        expect(orderLabels(scheduled.reports[0]?.destPath ?? '')).toEqual(labels)
      } finally {
        chmodSync(destDir, 0o700)
      }
    },
  )

  it('names the database after the file a connection has open when a scheduler is given only the connection', async () => {
    const path = workspace.path('inventory.db')
    const { db } = await seeded('inventory')
    await db.close()
    const conn = await betterSqlite3().open(path)
    try {
      const reports: BackupFileReport[] = []
      startScheduleClock('2026-03-01T10:00:00.500Z')
      const cancel = new BackupScheduler().schedule(conn, {
        cron: '* * * * * *',
        destDir: workspace.path('inventory-copies'),
        onBackup: report => {
          reports.push(report)
        },
      })

      await advanceSeconds(2)
      cancel()

      expect(reports.length).toBeGreaterThanOrEqual(1)
      expect(reports[0]).toMatchObject({ databaseId: 'inventory', sourcePath: realpathSync(path) })
      expect(orderLabels(reports[0]?.destPath ?? '')).toEqual(orderRows('inventory', 20).map(([label]) => label))
    } finally {
      await conn.close()
    }
  })

  it('reports each copy that fails to onError, leaves no partial file, and stops once the database closes', async () => {
    const watch = watchUnhandledRejections()
    try {
      const db = await workspace
        .registry(failingCopyDriver({ failAtStep: 1 }))
        .open('failing', workspace.path('failing.db'))
      await seedOrders(db, 'failing', 20_000, ORDER_PADDING)
      const destDir = workspace.path('failing-copies')
      const quiet = await seeded('quiet')
      startScheduleClock('2026-03-01T10:00:00.500Z')
      const scheduled = schedule(db, { destDir })
      let throwingHandlerCalls = 0
      db.scheduleBackup({ cron: '* * * * * *', destDir: workspace.path('unheard') })
      db.scheduleBackup({
        cron: '* * * * * *',
        destDir: workspace.path('handler-throws'),
        onError: () => {
          throwingHandlerCalls++
          throw new Error('the pager went away')
        },
      })
      quiet.db.scheduleBackup({ cron: '* * * * * *', destDir: workspace.path('quiet-copies'), maxFiles: 100 })

      await advanceSeconds(3)
      const errorsBeforeClose = scheduled.errors.length
      await db.close()
      await quiet.db.close()
      const quietCopies = copiesIn(workspace.path('quiet-copies')).length
      await advanceSeconds(3)

      expect(errorsBeforeClose).toBeGreaterThanOrEqual(2)
      expect(throwingHandlerCalls).toBeGreaterThanOrEqual(2)
      expect(filesIn(workspace.path('unheard'))).toEqual([])
      expect(filesIn(workspace.path('handler-throws'))).toEqual([])
      expect(scheduled.errors.map(codeOf).every(code => code === 'BACKUP_ERROR')).toBe(true)
      expect(scheduled.errors.length).toBe(errorsBeforeClose)
      expect(filesIn(destDir)).toEqual([])
      expect(quietCopies).toBeGreaterThanOrEqual(2)
      expect(copiesIn(workspace.path('quiet-copies'))).toHaveLength(quietCopies)
      expect(watch.reasons).toEqual([])
    } finally {
      watch.stop()
    }
  })
})
