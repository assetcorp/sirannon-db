import { afterEach, describe, expect, it } from 'vitest'
import { parseClusterStatus, TopologyAwareClient } from '../../../client/topology.js'
import type { ClusterStatusInfo } from '../../../core/types.js'
import { sendRaw } from '../lib/served-registry.js'
import { DATABASE_ID, droppingListener, INSERT_ORDER, recordingProxy, releaseClientServers } from './harness.js'
import {
  type ClusterNode,
  clusterNode,
  healthyRouting,
  type Routing,
  readable,
  routingHolder,
} from './topology-cluster.js'

const TERM_PAST_SAFE = 2n ** 53n + 1n
const DISCOVERY_TIMEOUT_MS = 2_000

afterEach(releaseClientServers)

function codeOf(promise: Promise<unknown>): Promise<string> {
  return promise.then(
    () => 'resolved',
    (err: { code?: string }) => err.code ?? 'none',
  )
}

function dataRequestsAt(nodes: ClusterNode[]): string[] {
  return nodes.flatMap(node => node.tally.requests.filter(ctx => !ctx.path.endsWith('/cluster')).map(ctx => ctx.path))
}

function coordinatorClient(starters: string[]): TopologyAwareClient {
  return new TopologyAwareClient({
    endpoints: starters,
    discovery: 'coordinator',
    readPreference: 'replica',
    transport: 'http',
  })
}

describe('routing metadata a client cannot trust', () => {
  it('refuses each malformed or unsafe body a real server sends with INVALID_RESPONSE and sends no data request anywhere', async () => {
    const holder = routingHolder()
    const primary = await clusterNode('primary-row')
    const starter = await clusterNode('starter-row', {
      getClusterStatus: holder.getClusterStatus,
      authorizeClusterStatus: () => true,
    })
    const safe = healthyRouting(primary.base, [readable('primary', primary.base, 'local', 'majority')])
    const hostile: Array<[label: string, body: unknown]> = [
      ['another database', { ...safe, databaseId: 'refunds' }],
      [
        'a primary endpoint that is a file',
        { ...safe, currentPrimary: { nodeId: 'p', endpoint: 'file:///etc/passwd' } },
      ],
      [
        'a primary endpoint that runs script',
        { ...safe, currentPrimary: { nodeId: 'p', endpoint: 'javascript:alert(1)' } },
      ],
      [
        'a primary endpoint with credentials',
        { ...safe, currentPrimary: { nodeId: 'p', endpoint: `http://root:pw@127.0.0.1:${primary.shop.served.port}` } },
      ],
      [
        'a primary endpoint with a query string',
        { ...safe, currentPrimary: { nodeId: 'p', endpoint: `${primary.base}/?to=x` } },
      ],
      ['a primary endpoint that is a number', { ...safe, currentPrimary: { nodeId: 'p', endpoint: 5 } }],
      ['a primary that is a string', { ...safe, currentPrimary: primary.base }],
      ['a read endpoint that is a file', { ...safe, readEndpoints: [readable('r', 'file:///tmp/x', 'majority')] }],
      [
        'a read endpoint URL that is a number',
        { ...safe, readEndpoints: [{ nodeId: 'r', endpoint: 7, readConcerns: [] }] },
      ],
      [
        'read concerns that are a string',
        { ...safe, readEndpoints: [{ nodeId: 'r', endpoint: primary.base, readConcerns: 'majority' }] },
      ],
      [
        'a read concern the engine never names',
        { ...safe, readEndpoints: [readable('r', primary.base, 'strong' as 'local')] },
      ],
      ['a primary term that is a number', { ...safe, primaryTerm: { toString: () => 5 } }],
    ]

    for (const [label, body] of hostile) {
      holder.current = body as Routing
      const client = coordinatorClient([starter.base])
      const read = await codeOf(client.database(DATABASE_ID).query('SELECT 1'))
      const write = await codeOf(client.database(DATABASE_ID).execute(INSERT_ORDER, ['lost', 1]))
      client.close()
      expect([read, write], label).toEqual(['INVALID_RESPONSE', 'INVALID_RESPONSE'])
    }

    expect(dataRequestsAt([primary, starter])).toEqual([])
  })

  it('moves past a starter that stalls, drops, or answers with something other than routing', async () => {
    const holder = routingHolder()
    const primary = await clusterNode('primary-row', {
      getClusterStatus: holder.getClusterStatus,
      authorizeClusterStatus: () => true,
    })
    holder.current = healthyRouting(primary.base, [readable('primary', primary.base, 'local', 'majority')])
    const stalling = await recordingProxy(null)
    stalling.answerWith(() => ({ kind: 'stall' }))
    const garbled = await recordingProxy(primary.shop.served.port)
    garbled.answerWith(request =>
      request.path.endsWith('/cluster') ? { kind: 'reply', status: 200, text: 'routing: primary' } : undefined,
    )
    const failing = await recordingProxy(primary.shop.served.port)
    failing.answerWith(request =>
      request.path.endsWith('/cluster')
        ? { kind: 'reply', status: 503, text: '{"error":{"code":"SHUTDOWN"}}' }
        : undefined,
    )
    const silent = await droppingListener()
    const brokenStarters = [stalling.base, silent.base, garbled.base, failing.base]

    const startedRecovering = Date.now()
    const recovering = coordinatorClient([...brokenStarters, primary.base])
    const answered = await recovering.database(DATABASE_ID).query<{ customer: string }>('SELECT customer FROM orders')
    const recoveryMs = Date.now() - startedRecovering
    const stranded = coordinatorClient(brokenStarters.slice(1))
    const strandedRead = await codeOf(stranded.database(DATABASE_ID).query('SELECT 1'))
    const strandedWrite = await codeOf(stranded.database(DATABASE_ID).execute(INSERT_ORDER, ['lost', 1]))
    recovering.close()
    stranded.close()

    expect(answered).toEqual([{ customer: 'primary-row' }])
    expect(recoveryMs).toBeGreaterThanOrEqual(DISCOVERY_TIMEOUT_MS - 100)
    expect(recoveryMs).toBeLessThan(DISCOVERY_TIMEOUT_MS * 3)
    expect([strandedRead, strandedWrite]).toEqual(['ROUTING_ERROR', 'ROUTING_ERROR'])
    expect(stalling.requests.map(request => request.path)).toEqual([`/db/${DATABASE_ID}/cluster`])
    expect(dataRequestsAt([primary])).toEqual([`/db/${DATABASE_ID}/query`])
  }, 20_000)

  it('reads a primary term past 2^53 exactly and refuses a status whose fields the engine never reports', async () => {
    let status: Partial<ClusterStatusInfo> = {}
    const node = await clusterNode('primary-row', {
      getClusterStatus: databaseId => ({ ...status, databaseId }) as ClusterStatusInfo,
      authorizeClusterStatus: () => true,
    })
    const fetchStatus = async (): Promise<unknown> =>
      (await sendRaw(node.base, 'GET', `/db/${DATABASE_ID}/cluster`)).body
    const sound: Partial<ClusterStatusInfo> = {
      ...healthyRouting(node.base, [readable('primary', node.base, 'local', 'majority')]),
      primaryTerm: TERM_PAST_SAFE,
      role: 'primary',
      replicationGroupId: 'orders-group',
    }

    status = sound
    const body = await fetchStatus()
    const parsed = parseClusterStatus(body, DATABASE_ID)
    const routed = coordinatorClient([node.base])
    const routedRead = await routed.database(DATABASE_ID).query<{ customer: string }>('SELECT customer FROM orders')
    routed.close()
    const refusals: string[] = []
    for (const variant of [
      { ...sound, health: 'exhausted' },
      { ...sound, healthReason: 'tired' },
      { ...sound, role: 'leader' },
      { ...sound, primaryTerm: '-1' },
      { ...sound, primaryTerm: '12abc' },
      { ...sound, currentPrimary: { endpoint: node.base } },
      { ...sound, readEndpoints: [{ nodeId: 'r', endpoint: node.base, readConcerns: 'majority' }] },
    ]) {
      status = variant as Partial<ClusterStatusInfo>
      const hostileBody = await fetchStatus()
      try {
        parseClusterStatus(hostileBody, DATABASE_ID)
        refusals.push('accepted')
      } catch (err) {
        refusals.push(`${(err as Error).name}:${(err as { code?: string }).code}`)
      }
    }
    let otherDatabase = 'accepted'
    try {
      parseClusterStatus(body, 'refunds')
    } catch (err) {
      otherDatabase = (err as { code?: string }).code ?? 'none'
    }

    expect((body as { primaryTerm?: unknown }).primaryTerm).toBe(TERM_PAST_SAFE.toString())
    expect(parsed).toEqual({
      databaseId: DATABASE_ID,
      role: 'primary',
      health: 'healthy',
      healthReason: 'in-sync',
      currentPrimary: { nodeId: 'primary', endpoint: node.base },
      primaryTerm: TERM_PAST_SAFE,
      readEndpoints: [{ nodeId: 'primary', endpoint: node.base, readConcerns: ['local', 'majority'] }],
      replicationGroupId: 'orders-group',
    })
    expect(routedRead).toEqual([{ customer: 'primary-row' }])
    expect(refusals).toEqual(Array(7).fill('RemoteError:INVALID_RESPONSE'))
    expect(otherDatabase).toBe('INVALID_RESPONSE')
  })
})
