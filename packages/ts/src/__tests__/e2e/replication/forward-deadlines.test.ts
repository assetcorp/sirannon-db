import { describe, expect, it } from 'vitest'
import { tableRows } from '../lib/replication-scene.js'
import { converged, INSERT_ORDER, ORDERS_SCHEMA, order, PRIMARY, REPLICA, ready, sceneForEachTest } from './harness.js'

const handle = sceneForEachTest()

describe('existing forwarded write deadlines', () => {
  it('accepts a fractional deadline while replicating forwarded writes', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const replica = await scene.replica(REPLICA, {
      port: primary.port,
      transportOptions: { forwardDeadlineMs: 30_000.5 },
    })
    await ready(replica)
    const result = await replica.engine.execute(INSERT_ORDER, [1, 'forwarded', 10])
    expect(result.changes).toBe(1)
    await converged(primary, [replica], 'orders', [order(1, 'forwarded', 10)])
  })

  it('lets an expired forward deadline refuse the write without preventing replication', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    await primary.engine.execute(INSERT_ORDER, [1, 'from-primary', 10])
    for (const forwardDeadlineMs of [0, -1]) {
      const replica = await scene.replica(REPLICA, { port: primary.port, transportOptions: { forwardDeadlineMs } })
      await ready(replica)
      await expect(replica.engine.execute(INSERT_ORDER, [2, 'expired', 20])).rejects.toMatchObject({
        code: 'TRANSPORT_ERROR',
      })
      await converged(primary, [replica], 'orders', [order(1, 'from-primary', 10)])
      expect(tableRows(primary.dbPath, 'orders')).toEqual([order(1, 'from-primary', 10)])
      await scene.stop(replica)
    }
  })
})
