import { describe, expect, it } from 'vitest'
import { copyDatabaseStepwise } from '../../backup/stepped-copy.js'
import type { DatabaseCopyRequest, DatabaseCopyStep, SQLiteConnection } from '../../driver/types.js'
import { SirannonError } from '../../errors.js'

function connectionStepping(steps: DatabaseCopyStep[]): SQLiteConnection {
  return {
    async copyDatabase(request: DatabaseCopyRequest) {
      for (const step of steps) request.onStep?.(step)
      return steps[steps.length - 1]
    },
  } as unknown as SQLiteConnection
}

describe('copyDatabaseStepwise', () => {
  it('reports the pages it moved and no restarts on an undisturbed copy', async () => {
    const conn = connectionStepping([
      { totalPages: 100, remainingPages: 60 },
      { totalPages: 100, remainingPages: 20 },
      { totalPages: 100, remainingPages: 0 },
    ])

    const result = await copyDatabaseStepwise(conn, { destPath: '/tmp/copy.db' })

    expect(result).toEqual({ pageCount: 100, restarts: 0 })
  })

  it('counts a restart when the copy returns to page one', async () => {
    const seen: number[] = []
    const conn = connectionStepping([
      { totalPages: 100, remainingPages: 40 },
      { totalPages: 100, remainingPages: 100 },
      { totalPages: 100, remainingPages: 0 },
    ])

    const result = await copyDatabaseStepwise(conn, {
      destPath: '/tmp/copy.db',
      onStep: step => seen.push(step.restarts),
    })

    expect(result.restarts).toBe(1)
    expect(seen).toEqual([0, 1, 1])
  })

  it('counts no restart when writes on the same connection grow the source', async () => {
    const conn = connectionStepping([
      { totalPages: 100, remainingPages: 40 },
      { totalPages: 108, remainingPages: 48 },
      { totalPages: 112, remainingPages: 0 },
    ])

    const result = await copyDatabaseStepwise(conn, { destPath: '/tmp/copy.db' })

    expect(result).toEqual({ pageCount: 112, restarts: 0 })
  })

  it('stops after the restart limit and names both the cause and what to do', async () => {
    const conn = connectionStepping([
      { totalPages: 100, remainingPages: 40 },
      { totalPages: 100, remainingPages: 100 },
      { totalPages: 100, remainingPages: 40 },
      { totalPages: 100, remainingPages: 100 },
      { totalPages: 100, remainingPages: 40 },
      { totalPages: 100, remainingPages: 100 },
    ])

    const error = await copyDatabaseStepwise(conn, { destPath: '/tmp/copy.db', restartLimit: 2 }).catch(
      (err: unknown) => err,
    )

    expect(error).toBeInstanceOf(SirannonError)
    expect((error as SirannonError).code).toBe('BACKUP_RESTARTED')
    expect((error as SirannonError).message).toContain('another connection wrote to the source database')
    expect((error as SirannonError).message).toContain('run the copy again')
  })

  it('honours a restart limit the caller raises', async () => {
    const conn = connectionStepping([
      { totalPages: 100, remainingPages: 40 },
      { totalPages: 100, remainingPages: 100 },
      { totalPages: 100, remainingPages: 40 },
      { totalPages: 100, remainingPages: 100 },
      { totalPages: 100, remainingPages: 0 },
    ])

    const result = await copyDatabaseStepwise(conn, { destPath: '/tmp/copy.db', restartLimit: 2 })

    expect(result.restarts).toBe(2)
  })

  it('stops a copy that restarts on every step and never gains ground', async () => {
    const conn = {
      async copyDatabase(request: DatabaseCopyRequest) {
        for (let step = 0; step < 500; step++) {
          request.onStep?.({ totalPages: 2007, remainingPages: 2007 - request.pagesPerStep })
        }
        return { totalPages: 2007, remainingPages: 0 }
      },
    } as unknown as SQLiteConnection

    const error = await copyDatabaseStepwise(conn, {
      destPath: '/tmp/copy.db',
      pagesPerStep: 64,
      noProgressStepLimit: 32,
    }).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_RESTARTED')
    expect((error as SirannonError).message).toContain('moved no page it had not already moved')
  })

  it('stops a copy that moves no pages and says what holds it still', async () => {
    const conn = {
      copyDatabase() {
        return new Promise<never>(() => {})
      },
    } as unknown as SQLiteConnection

    const error = await copyDatabaseStepwise(conn, { destPath: '/tmp/copy.db', stallTimeoutMs: 20 }).catch(
      (err: unknown) => err,
    )

    expect((error as SirannonError).code).toBe('BACKUP_STALLED')
    expect((error as SirannonError).message).toContain('moved no pages')
    expect((error as SirannonError).message).toContain('event loop')
  })

  it('refuses a connection whose runtime carries no stepped copy call', async () => {
    const error = await copyDatabaseStepwise({} as SQLiteConnection, { destPath: '/tmp/copy.db' }).catch(
      (err: unknown) => err,
    )

    expect((error as SirannonError).code).toBe('BACKUP_UNSUPPORTED')
  })

  it('passes the step size the caller chose to the driver', async () => {
    let seenPagesPerStep = 0
    const conn = {
      async copyDatabase(request: DatabaseCopyRequest) {
        seenPagesPerStep = request.pagesPerStep
        return { totalPages: 1, remainingPages: 0 }
      },
    } as unknown as SQLiteConnection

    await copyDatabaseStepwise(conn, { destPath: '/tmp/copy.db', pagesPerStep: 17 })

    expect(seenPagesPerStep).toBe(17)
  })
})
