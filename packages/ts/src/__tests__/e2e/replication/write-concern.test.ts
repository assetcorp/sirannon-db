import { describe, expect, it } from 'vitest'
import type { ManagedNode } from '../lib/factory.js'
import type { FaultProxy } from '../lib/fault-proxy.js'
import { eventually, readRows } from '../lib/replication-scene.js'
import {
  converged,
  INSERT_ORDER,
  ORDERS_SCHEMA,
  order,
  PRIMARY,
  REPLICA,
  ready,
  SECOND_REPLICA,
  sceneForEachTest,
  THIRD_REPLICA,
} from './harness.js'

const handle = sceneForEachTest()

const HELD_MS = 60_000
const SHORT_TIMEOUT_MS = 400
const LONG_TIMEOUT_MS = 5_000

interface FourNodes {
  primary: ManagedNode
  direct: ManagedNode
  second: ManagedNode
  third: ManagedNode
  secondLink: FaultProxy
  thirdLink: FaultProxy
}

async function fourNodes(): Promise<FourNodes> {
  const { scene } = handle
  const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
  const secondLink = await scene.proxy(primary.port)
  const thirdLink = await scene.proxy(primary.port)
  const direct = await scene.replica(REPLICA, { port: primary.port })
  const second = await scene.replica(SECOND_REPLICA, { port: secondLink.port })
  const third = await scene.replica(THIRD_REPLICA, { port: thirdLink.port })
  await ready(direct, second, third)
  return { primary, direct, second, third, secondLink, thirdLink }
}

function write(primary: ManagedNode, id: number, level: 'local' | 'majority' | 'all', timeoutMs: number) {
  return primary.engine
    .execute(INSERT_ORDER, [id, `${level}-${id}`, id], { writeConcern: { level, timeoutMs } })
    .then(() => 'met')
    .catch((error: unknown) => error)
}

function connectedReplicas(primary: ManagedNode): number {
  return primary.engine.status().peers.filter(peer => peer.connected).length
}

describe('the write concern of a primary with three replicas', () => {
  it('meets majority with two of three replicas acknowledging, and fails it with one', async () => {
    const { primary, direct, second, third, secondLink, thirdLink } = await fourNodes()

    thirdLink.delay('to-target', HELD_MS)
    const twoOfThree = await write(primary, 1, 'majority', LONG_TIMEOUT_MS)
    secondLink.delay('to-target', HELD_MS)
    const startedAt = Date.now()
    const oneOfThree = await write(primary, 2, 'majority', SHORT_TIMEOUT_MS)
    const waitedMs = Date.now() - startedAt
    secondLink.delay('to-target', 0)
    thirdLink.delay('to-target', 0)

    expect(twoOfThree).toBe('met')
    expect(oneOfThree).toMatchObject({
      code: 'WRITE_CONCERN_ERROR',
      message: `Timed out waiting for majority ACK of seq ${primary.engine.getCurrentSeq()}`,
    })
    expect(waitedMs).toBeGreaterThanOrEqual(SHORT_TIMEOUT_MS - 50)
    await converged(primary, [direct, second, third], 'orders', [order(1, 'majority-1', 1), order(2, 'majority-2', 2)])
  })

  it('fails all while one connected replica holds back its acknowledgement, and meets it once that replica answers', async () => {
    const { primary, direct, second, third, thirdLink } = await fourNodes()

    thirdLink.delay('to-target', HELD_MS)
    const heldBack = await write(primary, 1, 'all', SHORT_TIMEOUT_MS)
    thirdLink.delay('to-target', 0)
    const answered = await write(primary, 2, 'all', LONG_TIMEOUT_MS)

    expect(heldBack).toMatchObject({ code: 'WRITE_CONCERN_ERROR' })
    expect(readRows(primary.dbPath, 'SELECT id FROM orders ORDER BY id')).toEqual([{ id: 1 }, { id: 2 }])
    expect(answered).toBe('met')
    await converged(primary, [direct, second, third], 'orders', [order(1, 'all-1', 1), order(2, 'all-2', 2)])
  })

  it('returns a local write at once while every replica holds back its acknowledgement', async () => {
    const { primary, direct, second, third, secondLink, thirdLink } = await fourNodes()
    const { scene } = handle
    await scene.stop(direct)
    await eventually(() => connectedReplicas(primary) === 2, 'the direct replica to leave')
    secondLink.delay('to-target', HELD_MS)
    thirdLink.delay('to-target', HELD_MS)

    const startedAt = Date.now()
    const local = await write(primary, 1, 'local', SHORT_TIMEOUT_MS)

    expect(local).toBe('met')
    expect(Date.now() - startedAt).toBeLessThan(SHORT_TIMEOUT_MS)
    secondLink.delay('to-target', 0)
    thirdLink.delay('to-target', 0)
    await converged(primary, [second, third], 'orders', [order(1, 'local-1', 1)])
  })

  it('meets all from the replicas that remain connected after one replica stops', async () => {
    const { primary, direct, second, third } = await fourNodes()
    const { scene } = handle
    await scene.stop(third)
    await eventually(() => connectedReplicas(primary) === 2, 'the stopped replica to leave')

    const outcomes = []
    for (let id = 1; id <= 8; id += 1) outcomes.push(await write(primary, id, 'all', LONG_TIMEOUT_MS))

    expect(outcomes).toEqual(Array.from({ length: 8 }, () => 'met'))
    await converged(
      primary,
      [direct, second],
      'orders',
      Array.from({ length: 8 }, (_, at) => order(at + 1, `all-${at + 1}`, at + 1)),
    )
  })

  it('meets majority from the one replica that remains connected after two replicas stop', async () => {
    const { primary, direct, second, third } = await fourNodes()
    const { scene } = handle
    await scene.stop(second)
    await scene.stop(third)
    await eventually(() => connectedReplicas(primary) === 1, 'the stopped replicas to leave')

    const outcomes = []
    for (let id = 1; id <= 8; id += 1) outcomes.push(await write(primary, id, 'majority', LONG_TIMEOUT_MS))

    expect(outcomes).toEqual(Array.from({ length: 8 }, () => 'met'))
    await converged(
      primary,
      [direct],
      'orders',
      Array.from({ length: 8 }, (_, at) => order(at + 1, `majority-${at + 1}`, at + 1)),
    )
  })
})
