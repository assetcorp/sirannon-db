import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../sirannon.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Metrics integration', () => {
  it('tracks query metrics through MetricsConfig', async () => {
    const queryMetrics: { sql: string; durationMs: number }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      metrics: {
        onQueryComplete: m => queryMetrics.push({ sql: m.sql, durationMs: m.durationMs }),
      },
    })

    const db = await sir.open('main', join(tempDir, 'metrics.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.query('SELECT * FROM users')

    expect(queryMetrics.length).toBeGreaterThanOrEqual(3)
    expect(queryMetrics.every(m => m.durationMs >= 0)).toBe(true)

    await sir.shutdown()
  })

  it('tracks connection open and close metrics', async () => {
    const connectionEvents: { databaseId: string; event: 'open' | 'close' }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      metrics: {
        onConnectionOpen: m => connectionEvents.push({ databaseId: m.databaseId, event: m.event }),
        onConnectionClose: m => connectionEvents.push({ databaseId: m.databaseId, event: m.event }),
      },
    })

    await sir.open('db1', join(tempDir, 'metrics-conn1.db'))
    await sir.open('db2', join(tempDir, 'metrics-conn2.db'))

    expect(connectionEvents).toEqual([
      { databaseId: 'db1', event: 'open' },
      { databaseId: 'db2', event: 'open' },
    ])

    await sir.close('db1')

    expect(connectionEvents).toEqual([
      { databaseId: 'db1', event: 'open' },
      { databaseId: 'db2', event: 'open' },
      { databaseId: 'db1', event: 'close' },
    ])

    await sir.shutdown()
  })

  it('tracks errors in query metrics', async () => {
    const metrics: { sql: string; error?: boolean }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      metrics: {
        onQueryComplete: m => metrics.push({ sql: m.sql, error: m.error }),
      },
    })

    const db = await sir.open('main', join(tempDir, 'metrics-err.db'))

    try {
      await db.query('SELECT * FROM nonexistent_table')
    } catch {
      /* expected */
    }

    expect(metrics.length).toBe(1)
    expect(metrics[0].error).toBe(true)

    await sir.shutdown()
  })
})
