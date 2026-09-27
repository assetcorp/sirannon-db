import { describe, expect, it } from 'vitest'
import type { OperationRegistry } from '../../core/operation-registry.js'
import { operationRef } from '../../core/operation-registry.js'
import { TopologyAwareClient } from '../topology.js'
import { createClientServerHarness } from './server-harness.js'

interface Member {
  id: number
  name: string
}

const UNREACHABLE_REPLICA = 'http://127.0.0.1:1'
const members = operationRef<Record<string, never>, Member>('members')

const operations: OperationRegistry = {
  testdb: {
    reads: {
      members: { statement: () => ({ sql: 'SELECT id, name FROM users ORDER BY id' }) },
    },
  },
}

const harness = createClientServerHarness()

async function clusterWithUnreachableReplica(): Promise<string> {
  return harness.restart({
    acceptSql: true,
    operations,
    getClusterStatus: databaseId => ({
      databaseId,
      currentPrimary: { nodeId: 'node-a', endpoint: harness.baseUrl },
      primaryTerm: 1n,
      readEndpoints: [
        { nodeId: 'node-a', endpoint: harness.baseUrl, readConcerns: ['local', 'majority', 'linearizable'] },
        { nodeId: 'node-b', endpoint: UNREACHABLE_REPLICA, readConcerns: ['local', 'majority'] },
      ],
      health: 'healthy',
      healthReason: 'in-sync',
    }),
    authorizeClusterStatus: () => true,
  })
}

function coordinatorClient(baseUrl: string): TopologyAwareClient {
  return new TopologyAwareClient({
    endpoints: [baseUrl],
    discovery: 'coordinator',
    readPreference: 'replica',
    transport: 'http',
  })
}

describe('reads in coordinator mode', () => {
  it('answers a statement read from the primary once the replica refuses the connection', async () => {
    const baseUrl = await clusterWithUnreachableReplica()
    const client = coordinatorClient(baseUrl)

    const rows = await client.database('testdb').query<Member>('SELECT id, name FROM users ORDER BY id')

    expect(rows).toHaveLength(1)
    client.close()
  })

  it('answers a registered read from the primary once the replica refuses the connection', async () => {
    const baseUrl = await clusterWithUnreachableReplica()
    const client = coordinatorClient(baseUrl)

    const rows = await client.database('testdb').query(members, {})

    expect(rows).toHaveLength(1)
    client.close()
  })

  it('sends the next read straight to the primary, skipping the replica it could not reach', async () => {
    const baseUrl = await clusterWithUnreachableReplica()
    const client = coordinatorClient(baseUrl)
    const db = client.database('testdb')

    await db.query<Member>('SELECT id, name FROM users ORDER BY id')
    const started = Date.now()
    const rows = await db.query<Member>('SELECT id, name FROM users ORDER BY id')

    expect(rows).toHaveLength(1)
    expect(Date.now() - started).toBeLessThan(1_000)
    client.close()
  })
})
