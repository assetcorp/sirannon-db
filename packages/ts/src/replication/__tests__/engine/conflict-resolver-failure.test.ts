import { createHash } from 'node:crypto'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { HLC } from '../../../core/sync/hlc.js'
import { ReplicationEngine } from '../../engine.js'
import { canonicaliseForChecksum } from '../../log.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationErrorEvent } from '../../types.js'
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

it('reports a resolver failure as CONFLICT_ERROR naming the table and row', async () => {
  const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.execute("INSERT INTO users (id, name) VALUES (7, 'Local')")
  const resolverFailure = new Error('pricing rules unavailable')
  const engine = new ReplicationEngine(
    db,
    conn,
    makeConfig(harness.transport, {
      topology: new PrimaryReplicaTopology('replica'),
      defaultConflictResolver: {
        resolve: () => {
          throw resolverFailure
        },
      },
    }),
  )
  const reported: ReplicationErrorEvent[] = []
  engine.on('replication-error', event => reported.push(event))
  await engine.start()
  harness.transport.addPeer(NODE_B, 'primary')

  const hlc = new HLC(NODE_B).now()
  const changes = [
    {
      table: 'users',
      operation: 'update' as const,
      rowId: '7',
      primaryKey: { id: 7 },
      hlc,
      txId: 'remote-tx',
      nodeId: NODE_B,
      newData: { id: 7, name: 'Remote' },
      oldData: null,
    },
  ]
  await harness.transport.triggerBatchReceived(
    {
      sourceNodeId: NODE_B,
      batchId: `${NODE_B}-1-1`,
      fromSeq: 1n,
      toSeq: 1n,
      hlcRange: { min: hlc, max: hlc },
      changes,
      checksum: createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex'),
    },
    NODE_B,
  )

  expect(reported[0]?.error).toMatchObject({ code: 'CONFLICT_ERROR', table: 'users', rowId: '7' })
  expect(reported[0]?.error.cause).toBe(resolverFailure)
  await engine.stop()
})
