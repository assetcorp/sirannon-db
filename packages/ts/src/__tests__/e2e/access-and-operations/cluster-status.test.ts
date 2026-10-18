import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError } from '../../../core/errors.js'
import type { ClusterStatusAuthorizer, ClusterStatusInfo, RequestContext, ServerOptions } from '../../../core/types.js'
import { errorCodeOf, scratchDirectory, sendRaw, trackedRegistry } from '../lib/served-registry.js'
import { gate, openOrders, releaseAccessServers, serveTyped } from './served-access.js'

const PRIMARY_ADDRESS = '10.0.0.7'
const REPLICA_ADDRESS = '10.0.0.8'
const TERM_PAST_SAFE_INTEGERS = 2n ** 63n - 1n
const OPERATOR = 'Bearer operator'

function clusterStatus(databaseId: string): ClusterStatusInfo | null {
  if (databaseId !== 'orders') return null
  return {
    databaseId,
    replicationGroupId: 'orders-group',
    role: 'primary',
    currentPrimary: { nodeId: 'node-a', endpoint: `http://${PRIMARY_ADDRESS}:9876` },
    primaryTerm: TERM_PAST_SAFE_INTEGERS,
    readEndpoints: [
      { nodeId: 'node-a', endpoint: `http://${PRIMARY_ADDRESS}:9876`, readConcerns: ['local', 'majority'] },
      { nodeId: 'node-b', endpoint: `http://${REPLICA_ADDRESS}:9876`, readConcerns: ['local'] },
    ],
    health: 'degraded',
    healthReason: 'lagging',
  }
}

const EXPECTED_TOPOLOGY = {
  databaseId: 'orders',
  replicationGroupId: 'orders-group',
  role: 'primary',
  currentPrimary: { nodeId: 'node-a', endpoint: `http://${PRIMARY_ADDRESS}:9876` },
  primaryTerm: '9223372036854775807',
  readEndpoints: [
    { nodeId: 'node-a', endpoint: `http://${PRIMARY_ADDRESS}:9876`, readConcerns: ['local', 'majority'] },
    { nodeId: 'node-b', endpoint: `http://${REPLICA_ADDRESS}:9876`, readConcerns: ['local'] },
  ],
  health: 'degraded',
  healthReason: 'lagging',
}

let dir: string

beforeEach(() => {
  dir = scratchDirectory('cluster-status')
})

afterEach(releaseAccessServers)

async function clusterAnswer(options: ServerOptions) {
  const served = await serveTyped(trackedRegistry(), options)
  return sendRaw(served.base, 'GET', '/db/orders/cluster')
}

describe('the cluster status route', () => {
  it('serves the topology, with the primary term as an exact decimal string, only to a caller that the authorizer accepts', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const seen: RequestContext[] = []
    const served = await serveTyped(sirannon, {
      getClusterStatus: clusterStatus,
      authorizeClusterStatus: ctx => {
        seen.push(ctx)
        return ctx.headers.authorization === OPERATOR
      },
    })

    const accepted = await sendRaw(served.base, 'GET', '/db/orders/cluster', undefined, { Authorization: OPERATOR })
    const refused = await Promise.all(
      ['Bearer application', 'bearer operator', `${OPERATOR}x`, ''].map(credential =>
        sendRaw(served.base, 'GET', '/db/orders/cluster', undefined, { Authorization: credential }),
      ),
    )
    const unknown = await sendRaw(served.base, 'GET', '/db/refunds/cluster', undefined, { Authorization: OPERATOR })
    const unknownRefused = await sendRaw(served.base, 'GET', '/db/refunds/cluster')

    expect([accepted.status, accepted.body]).toEqual([200, EXPECTED_TOPOLOGY])
    for (const answer of refused) {
      expect([answer.status, errorCodeOf(answer.body)]).toEqual([404, 'NOT_FOUND'])
      expect(answer.text).not.toContain(PRIMARY_ADDRESS)
      expect(answer.text).not.toContain(REPLICA_ADDRESS)
    }
    expect([unknown.status, errorCodeOf(unknown.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect([unknownRefused.status, errorCodeOf(unknownRefused.body)]).toEqual([404, 'NOT_FOUND'])
    expect(seen[0]).toMatchObject({ method: 'get', path: '/db/orders/cluster', databaseId: 'orders' })
    expect(seen).toHaveLength(7)
  })

  it('answers a refusal byte for byte as a server with no cluster does, however the authorizer refuses', async () => {
    const plain = await clusterAnswer({})
    const variants: Array<[label: string, options: ServerOptions]> = [
      ['no authorizer', { getClusterStatus: clusterStatus }],
      ['no status source', { authorizeClusterStatus: () => true }],
      ['an authorizer that returns false', { getClusterStatus: clusterStatus, authorizeClusterStatus: () => false }],
      [
        'an authorizer that returns nothing',
        {
          getClusterStatus: clusterStatus,
          authorizeClusterStatus: (() => undefined) as unknown as ClusterStatusAuthorizer,
        },
      ],
      [
        'an authorizer that resolves to zero',
        {
          getClusterStatus: clusterStatus,
          authorizeClusterStatus: (async () => 0) as unknown as ClusterStatusAuthorizer,
        },
      ],
      [
        'an authorizer that resolves false late',
        {
          getClusterStatus: clusterStatus,
          authorizeClusterStatus: async () => {
            await new Promise(resolve => setTimeout(resolve, 20))
            return false
          },
        },
      ],
    ]

    expect([plain.status, errorCodeOf(plain.body)]).toEqual([404, 'NOT_FOUND'])
    for (const [label, options] of variants) {
      const refused = await clusterAnswer(options)
      expect([refused.status, refused.text], label).toEqual([plain.status, plain.text])
      expect(refused.headers.get('content-type'), label).toBe(plain.headers.get('content-type'))
    }
  })

  it('fails closed with 500 HOOK_ERROR when the authorizer throws or rejects, and leaks no address', async () => {
    const failures: Array<[label: string, authorizer: ClusterStatusAuthorizer]> = [
      [
        'a throw',
        () => {
          throw new Error(`token service at ${PRIMARY_ADDRESS} unreachable`)
        },
      ],
      ['a rejection', () => Promise.reject(new Error(`token service at ${REPLICA_ADDRESS} unreachable`))],
      ['a rejection with nothing', () => Promise.reject()],
    ]

    for (const [label, authorizeClusterStatus] of failures) {
      const answer = await clusterAnswer({ getClusterStatus: clusterStatus, authorizeClusterStatus })
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([500, 'HOOK_ERROR'])
      expect(answer.text, label).not.toContain(PRIMARY_ADDRESS)
      expect(answer.text, label).not.toContain(REPLICA_ADDRESS)
    }
  })

  it('calls the authenticate hook first, so a caller that it refuses never reaches the authorizer, and keeps serving while an authorizer hangs', async () => {
    const sirannon = trackedRegistry()
    await openOrders(sirannon, dir)
    const order: string[] = []
    const hung = gate()
    const served = await serveTyped(sirannon, {
      getClusterStatus: clusterStatus,
      authenticate: ctx => {
        order.push(`authenticate ${ctx.headers.authorization ?? 'none'}`)
        if (ctx.headers.authorization === undefined) throw new RequestDeniedError(401, 'NO_TOKEN', 'Present a token')
        return { operator: true }
      },
      authorizeClusterStatus: async ctx => {
        order.push(`authorize ${ctx.headers.authorization}`)
        if (ctx.headers['x-hang'] === 'yes') await hung.promise
        return true
      },
    })

    const refused = await sendRaw(served.base, 'GET', '/db/orders/cluster')
    const hanging = sendRaw(served.base, 'GET', '/db/orders/cluster', undefined, {
      Authorization: OPERATOR,
      'x-hang': 'yes',
    })
    const others = await Promise.all(
      Array.from({ length: 5 }, () =>
        sendRaw(served.base, 'GET', '/db/orders/cluster', undefined, { Authorization: OPERATOR }),
      ),
    )
    hung.open()
    const released = await hanging

    expect([refused.status, refused.body]).toEqual([401, { error: { code: 'NO_TOKEN', message: 'Present a token' } }])
    for (const answer of others) expect([answer.status, answer.body]).toEqual([200, EXPECTED_TOPOLOGY])
    expect([released.status, released.body]).toEqual([200, EXPECTED_TOPOLOGY])
    expect(order[0]).toBe('authenticate none')
    expect(order.filter(entry => entry.startsWith('authorize'))).toHaveLength(6)
    expect(order.indexOf('authorize none')).toBe(-1)
  })
})
