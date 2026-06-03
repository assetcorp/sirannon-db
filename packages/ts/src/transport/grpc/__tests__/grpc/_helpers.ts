import { expect } from 'vitest'
import type { ReplicationAck, ReplicationBatch } from '../../../../replication/types.js'
import { GrpcReplicationTransport } from '../../index.js'

export function createBatch(overrides: Partial<ReplicationBatch> = {}): ReplicationBatch {
  return {
    sourceNodeId: 'primary-node',
    batchId: `batch-${Date.now()}`,
    fromSeq: 1n,
    toSeq: 5n,
    hlcRange: { min: '2024-01-01T00:00:00.000Z-0000-primary', max: '2024-01-01T00:00:01.000Z-0000-primary' },
    changes: [
      {
        table: 'users',
        operation: 'insert',
        rowId: 'user-1',
        primaryKey: { id: 1 },
        hlc: '2024-01-01T00:00:00.000Z-0000-primary',
        txId: 'tx-1',
        nodeId: 'primary-node',
        newData: { id: 1, name: 'Alice', email: 'alice@example.com' },
        oldData: null,
      },
    ],
    checksum: 'abc123',
    ...overrides,
  }
}

export function createAck(overrides: Partial<ReplicationAck> = {}): ReplicationAck {
  return {
    batchId: 'batch-1',
    ackedSeq: 5n,
    nodeId: 'replica-node',
    ...overrides,
  }
}

export function waitFor(conditionFn: () => boolean, timeoutMs = 5000, intervalMs = 50): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const start = Date.now()
    const check = () => {
      if (conditionFn()) {
        resolve()
        return
      }
      if (Date.now() - start > timeoutMs) {
        reject(new Error('waitFor timed out'))
        return
      }
      setTimeout(check, intervalMs)
    }
    check()
  })
}

export async function setupPrimaryReplica(
  transports: GrpcReplicationTransport[],
  primaryOpts: ConstructorParameters<typeof GrpcReplicationTransport>[0] = {},
  replicaOpts: ConstructorParameters<typeof GrpcReplicationTransport>[0] = {},
) {
  const primary = new GrpcReplicationTransport({
    insecure: true,
    port: 0,
    ...primaryOpts,
  })
  transports.push(primary)

  await primary.connect('primary-node', { localRole: 'primary' })
  const port = primary.getPort()
  expect(port).toBeGreaterThan(0)

  const replica = new GrpcReplicationTransport({
    insecure: true,
    ...replicaOpts,
  })
  transports.push(replica)

  await replica.connect('replica-node', {
    localRole: 'replica',
    endpoints: [`localhost:${port}`],
  })

  await waitFor(() => primary.peers().size > 0 && replica.peers().size > 0)

  return { primary, replica, port }
}

export async function teardown(transports: GrpcReplicationTransport[]) {
  for (const t of transports) {
    try {
      await t.disconnect()
    } catch {
      /* already disconnected */
    }
  }
  transports.length = 0
}
