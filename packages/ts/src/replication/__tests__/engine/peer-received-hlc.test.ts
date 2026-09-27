import { createHash } from 'node:crypto'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { HLC } from '../../../core/sync/hlc.js'
import { ReplicationEngine } from '../../engine.js'
import { canonicaliseForChecksum } from '../../log.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationBatch } from '../../types.js'
import {
  createDbAndConn,
  createHarness,
  type EngineTestHarness,
  makeConfig,
  NODE_B,
  teardownHarness,
} from './helpers.js'

let harness: EngineTestHarness

beforeEach(() => {
  harness = createHarness()
})

afterEach(async () => {
  await teardownHarness(harness)
})

function batchFromNodeB(seq: bigint, rowId: number, hlc: string): ReplicationBatch {
  const changes = [
    {
      table: 'users',
      operation: 'insert' as const,
      rowId: String(rowId),
      primaryKey: { id: rowId },
      hlc,
      txId: `remote-tx-${seq}`,
      nodeId: NODE_B,
      newData: { id: rowId, name: `Remote ${rowId}` },
      oldData: null,
    },
  ]
  return {
    sourceNodeId: NODE_B,
    batchId: `${NODE_B}-${seq}-${seq}`,
    fromSeq: seq,
    toSeq: seq,
    hlcRange: { min: hlc, max: hlc },
    changes,
    checksum: createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex'),
  }
}

it('records the highest clock stamp that an applied batch from a peer carries', async () => {
  const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  const engine = new ReplicationEngine(
    db,
    conn,
    makeConfig(harness.transport, { topology: new PrimaryReplicaTopology('replica') }),
  )
  await engine.start()
  harness.transport.addPeer(NODE_B, 'primary')
  const clock = new HLC(NODE_B)
  const first = clock.now()
  const second = clock.now()

  await harness.transport.triggerBatchReceived(batchFromNodeB(1n, 1, first), NODE_B)
  expect(engine.peerTracker.getPeerState(NODE_B)?.lastReceivedHlc).toBe(first)

  await harness.transport.triggerBatchReceived(batchFromNodeB(2n, 2, second), NODE_B)
  expect(engine.peerTracker.getPeerState(NODE_B)?.lastReceivedHlc).toBe(second)

  await engine.stop()
})
