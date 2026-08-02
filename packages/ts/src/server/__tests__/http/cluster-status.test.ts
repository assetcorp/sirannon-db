import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { ClusterStatusInfo, RequestContext, ServerOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer | null = null

const driver = betterSqlite3()

function clusterStatus(databaseId: string): ClusterStatusInfo {
  return {
    databaseId,
    replicationGroupId: 'orders-group',
    role: 'primary',
    currentPrimary: { nodeId: 'node-a', endpoint: 'http://10.0.0.7:9876' },
    primaryTerm: 42n,
    readEndpoints: [{ nodeId: 'node-b', endpoint: 'http://10.0.0.8:9876', readConcerns: ['local'] }],
    health: 'degraded',
    healthReason: 'lagging',
  }
}

async function listen(options: ServerOptions): Promise<string> {
  server = createServer(sirannon, { acceptSql: true, port: 0, ...options })
  await server.listen()
  return `http://127.0.0.1:${server.listeningPort}`
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-cluster-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('orders', join(tempDir, 'orders.db'))
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
})

afterEach(async () => {
  await server?.close()
  server = null
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('GET /db/:id/cluster', () => {
  it('serves topology to a request the authorizer accepts', async () => {
    const baseUrl = await listen({ getClusterStatus: clusterStatus, authorizeClusterStatus: () => true })

    const response = await fetch(`${baseUrl}/db/orders/cluster`)
    const body = (await response.json()) as ClusterStatusInfo

    expect(response.status).toBe(200)
    expect(body.currentPrimary).toEqual({ nodeId: 'node-a', endpoint: 'http://10.0.0.7:9876' })
    expect(body.health).toBe('degraded')
    expect(body.healthReason).toBe('lagging')
  })

  it('hides topology from a server that configured no authorizer', async () => {
    const baseUrl = await listen({ getClusterStatus: clusterStatus })

    const response = await fetch(`${baseUrl}/db/orders/cluster`)
    const body = (await response.json()) as { error: { code: string; message: string } }

    expect(response.status).toBe(404)
    expect(body.error.code).toBe('NOT_FOUND')
    expect(JSON.stringify(body)).not.toContain('10.0.0.7')
  })

  it('answers a refused request exactly as it answers one to a server with no cluster', async () => {
    const gatedUrl = await listen({ getClusterStatus: clusterStatus, authorizeClusterStatus: () => false })
    const refused = await fetch(`${gatedUrl}/db/orders/cluster`)
    const refusedBody = await refused.text()
    await server?.close()
    server = null

    const plainUrl = await listen({})
    const absent = await fetch(`${plainUrl}/db/orders/cluster`)
    const absentBody = await absent.text()

    expect(refused.status).toBe(absent.status)
    expect(refusedBody).toBe(absentBody)
  })

  it('passes the request headers to the authorizer', async () => {
    const seen: RequestContext[] = []
    const baseUrl = await listen({
      getClusterStatus: clusterStatus,
      authorizeClusterStatus: ctx => {
        seen.push(ctx)
        return ctx.headers.authorization === 'Bearer operator-token'
      },
    })

    const denied = await fetch(`${baseUrl}/db/orders/cluster`, { headers: { Authorization: 'Bearer app-token' } })
    const allowed = await fetch(`${baseUrl}/db/orders/cluster`, { headers: { Authorization: 'Bearer operator-token' } })

    expect(denied.status).toBe(404)
    expect(allowed.status).toBe(200)
    expect(seen).toHaveLength(2)
    expect(seen[0].databaseId).toBe('orders')
    expect(seen[0].path).toBe('/db/orders/cluster')
  })

  it('awaits an asynchronous authorizer', async () => {
    const baseUrl = await listen({
      getClusterStatus: clusterStatus,
      authorizeClusterStatus: async () => {
        await new Promise(resolve => setTimeout(resolve, 10))
        return true
      },
    })

    const response = await fetch(`${baseUrl}/db/orders/cluster`)

    expect(response.status).toBe(200)
  })

  it('fails closed when the authorizer throws', async () => {
    const baseUrl = await listen({
      getClusterStatus: clusterStatus,
      authorizeClusterStatus: () => {
        throw new Error('token service unreachable')
      },
    })

    const response = await fetch(`${baseUrl}/db/orders/cluster`)
    const body = (await response.json()) as { error: { code: string } }

    expect(response.status).toBe(500)
    expect(body.error.code).toBe('HOOK_ERROR')
    expect(JSON.stringify(body)).not.toContain('10.0.0.7')
  })

  it('still runs the authenticate hook before the authorizer', async () => {
    let authorizerCalls = 0
    const baseUrl = await listen({
      getClusterStatus: clusterStatus,
      authorizeClusterStatus: () => {
        authorizerCalls++
        return true
      },
      authenticate: () => {
        throw new RequestDeniedError(401, 'UNAUTHORIZED', 'No credential')
      },
    })

    const response = await fetch(`${baseUrl}/db/orders/cluster`)
    const body = (await response.json()) as { error: { code: string } }

    expect(response.status).toBe(401)
    expect(body.error.code).toBe('UNAUTHORIZED')
    expect(authorizerCalls).toBe(0)
  })

  it('reports an unknown database only to an authorized request', async () => {
    const baseUrl = await listen({
      getClusterStatus: databaseId => (databaseId === 'orders' ? clusterStatus(databaseId) : null),
      authorizeClusterStatus: () => true,
    })

    const response = await fetch(`${baseUrl}/db/missing/cluster`)
    const body = (await response.json()) as { error: { code: string } }

    expect(response.status).toBe(404)
    expect(body.error.code).toBe('DATABASE_NOT_FOUND')
  })
})
