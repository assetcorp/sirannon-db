import { describe, expect, it, vi } from 'vitest'
import { TopologyAwareClient } from '../topology.js'
import type { Transport } from '../types.js'
import { createClientServerHarness } from './server-harness.js'

const harness = createClientServerHarness()

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolveDeferred = () => {}
  const promise = new Promise<void>(resolve => {
    resolveDeferred = resolve
  })
  return { promise, resolve: resolveDeferred }
}

function createDelayedSubscriptionTransport(gate: Promise<void>, closedIndexes: number[], index: number): Transport {
  let closed = false
  return {
    query: async () => ({ rows: [] }),
    execute: async () => ({ changes: 0, lastInsertRowId: 0 }),
    transaction: async () => ({ results: [] }),
    batch: async () => ({ results: [] }),
    load: async () => ({ rowsLoaded: 0, changes: 0 }),
    queryNamed: async () => ({ rows: [] }),
    executeNamed: async () => ({ results: [] }),
    liveSubscribe: async () => ({ unsubscribe: () => {} }),
    subscribe: async () => {
      await gate
      if (closed) {
        throw new Error(`transport ${index} closed during subscription`)
      }
      return { unsubscribe: () => {} }
    },
    close: () => {
      closed = true
      closedIndexes.push(index)
    },
  }
}

interface RecordedSubscriptionOperation {
  action: 'subscribe' | 'unsubscribe'
  endpoint: string
  table: string
}

function createRecordedSubscriptionTransport(
  endpoint: string,
  operations: RecordedSubscriptionOperation[],
  closedEndpoints: string[],
  failSubscribe = false,
): Transport {
  let closed = false
  return {
    query: async () => ({ rows: [] }),
    execute: async () => ({ changes: 0, lastInsertRowId: 0 }),
    transaction: async () => ({ results: [] }),
    batch: async () => ({ results: [] }),
    load: async () => ({ rowsLoaded: 0, changes: 0 }),
    queryNamed: async () => ({ rows: [] }),
    executeNamed: async () => ({ results: [] }),
    liveSubscribe: async () => ({ unsubscribe: () => {} }),
    subscribe: async table => {
      if (closed) {
        throw new Error(`transport for ${endpoint} is closed`)
      }
      if (failSubscribe) {
        throw new Error(`subscriptions unavailable at ${endpoint}`)
      }
      let active = true
      operations.push({ action: 'subscribe', endpoint, table })
      return {
        unsubscribe: () => {
          if (!active) return
          active = false
          operations.push({ action: 'unsubscribe', endpoint, table })
        },
      }
    },
    close: () => {
      if (closed) return
      closed = true
      closedEndpoints.push(endpoint)
    },
  }
}

describe('TopologyAwareClientOptions', () => {
  it('keeps coordinator subscriptions on one stable read transport', async () => {
    const client = new TopologyAwareClient({
      endpoints: [harness.baseUrl],
      discovery: 'coordinator',
      readPreference: 'replica',
      transport: 'websocket',
    })
    const gate = deferred()
    const closedIndexes: number[] = []
    const endpoints = [
      'http://127.0.0.1:7101',
      'http://127.0.0.1:7102',
      'http://127.0.0.1:7103',
      'http://127.0.0.1:7104',
      'http://127.0.0.1:7105',
    ]

    const getReadEndpoint = vi.spyOn(client, '_getReadEndpoint').mockImplementation(async () => {
      return endpoints.shift() ?? harness.baseUrl
    })
    const createTransport = vi.spyOn(client, '_createTransportForEndpoint').mockImplementation(() => {
      const index = createTransport.mock.calls.length
      return createDelayedSubscriptionTransport(gate.promise, closedIndexes, index)
    })

    const db = client.database('testdb')
    const subscriptions = Promise.all([
      db.on('customers').subscribe(() => {}),
      db.on('entitlements').subscribe(() => {}),
      db.on('usage_events').subscribe(() => {}),
      db.on('billing_events').subscribe(() => {}),
      db.on('audit_log').subscribe(() => {}),
    ])

    await wait(0)
    gate.resolve()
    const handles = await subscriptions

    expect(getReadEndpoint).toHaveBeenCalledTimes(1)
    expect(createTransport).toHaveBeenCalledTimes(1)
    expect(closedIndexes).toEqual([])

    for (const handle of handles) {
      handle.unsubscribe()
    }
    client.close()
  })

  it('migrates active coordinator subscriptions when routing metadata changes', async () => {
    let readableEndpoint = 'http://127.0.0.1:7201'
    const baseUrl = await harness.restart({
      getClusterStatus: databaseId => ({
        databaseId,
        currentPrimary: { nodeId: 'node-a', endpoint: harness.baseUrl },
        primaryTerm: 1n,
        readEndpoints: [{ nodeId: 'node-b', endpoint: readableEndpoint, readConcerns: ['majority'] }],
        health: 'healthy',
      }),
      authorizeClusterStatus: () => true,
    })

    const client = new TopologyAwareClient({
      endpoints: [baseUrl],
      discovery: 'coordinator',
      readPreference: 'replica',
      transport: 'websocket',
    })
    const operations: RecordedSubscriptionOperation[] = []
    const closedEndpoints: string[] = []
    const createTransport = vi
      .spyOn(client, '_createTransportForEndpoint')
      .mockImplementation(endpoint =>
        createRecordedSubscriptionTransport(endpoint, operations, closedEndpoints, endpoint.endsWith(':7203')),
      )

    const db = client.database('testdb')
    const customers = await db.on('customers').subscribe(() => {})
    const usage = await db.on('usage_events').subscribe(() => {})

    expect(createTransport).toHaveBeenCalledTimes(1)
    expect(
      operations
        .filter(operation => operation.action === 'subscribe')
        .map(operation => [operation.endpoint, operation.table]),
    ).toEqual([
      ['http://127.0.0.1:7201', 'customers'],
      ['http://127.0.0.1:7201', 'usage_events'],
    ])

    customers.unsubscribe()
    readableEndpoint = 'http://127.0.0.1:7202'
    await client._refreshClusterRouting('testdb')

    expect(
      operations
        .filter(operation => operation.action === 'subscribe')
        .map(operation => [operation.endpoint, operation.table]),
    ).toEqual([
      ['http://127.0.0.1:7201', 'customers'],
      ['http://127.0.0.1:7201', 'usage_events'],
      ['http://127.0.0.1:7202', 'usage_events'],
    ])
    expect(operations).toContainEqual({
      action: 'unsubscribe',
      endpoint: 'http://127.0.0.1:7201',
      table: 'customers',
    })
    expect(operations).toContainEqual({
      action: 'unsubscribe',
      endpoint: 'http://127.0.0.1:7201',
      table: 'usage_events',
    })
    expect(closedEndpoints).toContain('http://127.0.0.1:7201')

    readableEndpoint = 'http://127.0.0.1:7203'
    await expect(client._refreshClusterRouting('testdb')).rejects.toMatchObject({
      code: 'ROUTING_ERROR',
      message: expect.stringContaining('Could not re-establish active subscriptions'),
    })
    expect(closedEndpoints).toContain('http://127.0.0.1:7203')
    expect(closedEndpoints).not.toContain('http://127.0.0.1:7202')

    usage.unsubscribe()
    client.close()
  })
})
