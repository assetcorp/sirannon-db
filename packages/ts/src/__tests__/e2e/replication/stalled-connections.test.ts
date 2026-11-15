import { status } from '@grpc/grpc-js'
import { describe, expect, it } from 'vitest'
import { eventually, tableRows } from '../lib/replication-scene.js'
import { waitForReady } from '../lib/waitForReplica.js'
import { converged, INSERT_ORDER, ORDERS_SCHEMA, order, PRIMARY, REPLICA, ready, sceneForEachTest } from './harness.js'
import { helloOutcome } from './peers.js'

const handle = sceneForEachTest()

const HELLO_TIMEOUT_MS = 1_000
const KEEPALIVE_INTERVAL_MS = 1_000
const KEEPALIVE_TIMEOUT_MS = 1_000
const HEALTHY_PINGS_MS = 3_500
const RECOVERY_DEADLINE_MS = 8_000

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

describe('a replica whose connection to the primary stalls', () => {
  it('dials the primary again and syncs when its first connection stalls before the primary answers its Hello', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    await primary.engine.execute(INSERT_ORDER, [1, 'placed-before-the-stall', 10])
    const link = await scene.proxy(primary.port)
    link.swallow(1)

    const replica = await scene.replica(REPLICA, {
      port: link.port,
      transportOptions: { helloTimeoutMs: HELLO_TIMEOUT_MS },
    })
    await waitForReady(replica.engine, RECOVERY_DEADLINE_MS)

    expect(link.opened).toBeGreaterThanOrEqual(2)
    await converged(primary, [replica], 'orders', [order(1, 'placed-before-the-stall', 10)])
  })

  it('finds a connection that goes silent after the Hello through keepalive, dials again, and receives the writes made meanwhile', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const link = await scene.proxy(primary.port)
    const replica = await scene.replica(REPLICA, {
      port: link.port,
      transportOptions: { keepaliveIntervalMs: KEEPALIVE_INTERVAL_MS, keepaliveTimeoutMs: KEEPALIVE_TIMEOUT_MS },
    })
    await ready(replica)
    await primary.engine.execute(INSERT_ORDER, [1, 'placed-before-the-silence', 10])
    await converged(primary, [replica], 'orders', [order(1, 'placed-before-the-silence', 10)])

    await sleep(HEALTHY_PINGS_MS)
    expect({ opened: link.opened, closed: link.closed }).toEqual({ opened: 1, closed: 0 })

    link.silence()
    await primary.engine.execute(INSERT_ORDER, [2, 'placed-during-the-silence', 20])
    const expected = [order(1, 'placed-before-the-silence', 10), order(2, 'placed-during-the-silence', 20)]
    await eventually(
      () => JSON.stringify(tableRows(replica.dbPath, 'orders')) === JSON.stringify(expected),
      'the replica to receive the write made while its first connection was silent',
      RECOVERY_DEADLINE_MS,
    )

    expect(link.opened).toBeGreaterThanOrEqual(2)
  })
})

describe('a primary that a peer dials', () => {
  it('ends a replication stream whose first message never arrives', async () => {
    const { scene, certs } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [ORDERS_SCHEMA],
      transportOptions: { helloTimeoutMs: HELLO_TIMEOUT_MS },
    })
    const identity = { nodeId: REPLICA, cert: certs.certForNode(REPLICA), caCertPath: certs.caCertPath }

    const outcome = await Promise.race([
      helloOutcome(primary.port, identity, REPLICA, 'nothing'),
      sleep(RECOVERY_DEADLINE_MS).then(() => 'still open'),
    ])

    expect(outcome).toEqual({ answeredBy: null, code: status.DEADLINE_EXCEEDED })
  })
})
