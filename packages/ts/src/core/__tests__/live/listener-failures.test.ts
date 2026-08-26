import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { type LiveHarness, openHarness } from './_helpers.js'

const SCHEMA = 'CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)'

async function settleRejections(): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 60))
  await new Promise(resolve => setImmediate(resolve))
}

describe('live query listener failures', () => {
  let harness: LiveHarness
  let unhandled: unknown[]
  let capture: (reason: unknown) => void

  beforeEach(async () => {
    harness = await openHarness(SCHEMA)
    unhandled = []
    capture = reason => unhandled.push(reason)
    process.on('unhandledRejection', capture)
  })

  afterEach(async () => {
    process.off('unhandledRejection', capture)
    await harness.dispose()
  })

  it('keeps delivering to other listeners when one throws, and reports to onError', async () => {
    const reported: string[] = []
    const delivered: string[] = []
    const query = await harness.db.live('SELECT id, status FROM orders ORDER BY id', undefined, {
      onError: error => reported.push(error.message),
    })

    query.subscribe(() => {
      throw new Error('listener failed')
    })
    query.subscribe(() => {
      delivered.push('second')
    })

    await harness.db.execute("INSERT INTO orders (id, status) VALUES (1, 'open')")
    await settleRejections()

    expect(delivered.length).toBeGreaterThan(0)
    expect(reported).toContain('listener failed')
    await query.close()
  })

  it('leaves no unhandled rejection when a listener rejects', async () => {
    const query = await harness.db.live('SELECT id, status FROM orders ORDER BY id')

    query.subscribe(async () => {
      throw new Error('async listener failed')
    })

    await harness.db.execute("INSERT INTO orders (id, status) VALUES (2, 'open')")
    await settleRejections()

    expect(unhandled).toEqual([])
    await query.close()
  })
})
