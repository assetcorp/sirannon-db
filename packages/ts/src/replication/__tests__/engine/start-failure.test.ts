import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { TransportConfig } from '../../types.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, teardownHarness } from './helpers.js'

describe('a start that fails', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('leaves the engine stopped, so the next start connects the transport', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const transport = harness.transport
    const connect = transport.connect.bind(transport)
    let refuseNext = true
    transport.connect = async (nodeId: string, config: TransportConfig) => {
      if (refuseNext) {
        refuseNext = false
        throw new Error('transport refused the connection')
      }
      await connect(nodeId, config)
    }

    const engine = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport,
      initialSync: false,
    })

    await expect(engine.start()).rejects.toThrow('transport refused the connection')
    expect(engine.status().replicating).toBe(false)

    await engine.start()
    expect(transport.connected).toBe(true)
    expect(engine.status().replicating).toBe(true)

    await engine.stop()
  })
})
