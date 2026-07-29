import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { AuthenticateHook } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

interface ApiResponse {
  rows: Record<string, unknown>[]
  error: { code: string; message: string }
}

interface Account {
  subject: string
}

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let hookServer: SirannonServer<Account> | undefined

const driver = betterSqlite3()

async function startHookServer(authenticate: AuthenticateHook<Account>): Promise<string> {
  hookServer = createServer<Account>(sirannon, { acceptSql: true, port: 0, authenticate })
  await hookServer.listen()
  return `http://127.0.0.1:${hookServer.listeningPort}`
}

function selectOne(url: string, headers: Record<string, string> = {}): Promise<Response> {
  return fetch(`${url}/db/test/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify({ sql: 'SELECT 1 AS val' }),
  })
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-authenticate-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

  server = createServer(sirannon, { acceptSql: true, port: 0 })
  await server.listen()
})

afterEach(async () => {
  if (hookServer) {
    await hookServer.close()
    hookServer = undefined
  }
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('authenticate', () => {
  it('denies with the status, code, and message the hook throws', async () => {
    const url = await startHookServer(() => {
      throw new RequestDeniedError(403, 'FORBIDDEN', 'Access denied')
    })
    const res = await selectOne(url)
    expect(res.status).toBe(403)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('FORBIDDEN')
    expect(body.error.message).toBe('Access denied')
  })

  it('allows the request when the hook returns an identity', async () => {
    const url = await startHookServer(({ headers }) => {
      if (headers.authorization !== 'Bearer valid-token') {
        throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Authentication required')
      }
      return { subject: 'alice' }
    })
    expect((await selectOne(url, { Authorization: 'Bearer valid-token' })).status).toBe(200)
    expect((await selectOne(url)).status).toBe(401)
  })

  it('allows the request when the hook returns nothing', async () => {
    const url = await startHookServer(() => undefined)
    expect((await selectOne(url)).status).toBe(200)
  })

  it('refuses the request when the hook returns a refusal instead of throwing', async () => {
    const url = await startHookServer((() => ({
      status: 401,
      code: 'UNAUTHORIZED',
      message: 'No token',
    })) as unknown as AuthenticateHook<Account>)

    const res = await selectOne(url)
    expect(res.status).toBe(500)
    const body = (await res.json()) as ApiResponse
    expect(body.error.code).toBe('HOOK_ERROR')
    expect(body.error.message).toMatch(/refuses a request by throwing/i)
  })

  it('does not run for health endpoints', async () => {
    const url = await startHookServer(() => {
      throw new RequestDeniedError(403, 'FORBIDDEN', 'Blocked')
    })
    expect((await fetch(`${url}/health`)).status).toBe(200)
    expect((await fetch(`${url}/health/ready`)).status).toBe(200)
  })

  it('awaits an async hook for both outcomes', async () => {
    const url = await startHookServer(async ({ headers }) => {
      await new Promise(resolve => setTimeout(resolve, 5))
      if (headers['x-api-key'] !== 'secret') {
        throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Bad key')
      }
      return { subject: 'service' }
    })
    expect((await selectOne(url)).status).toBe(401)
    expect((await selectOne(url, { 'X-Api-Key': 'secret' })).status).toBe(200)
  })

  it('answers 500 HOOK_ERROR when the hook throws an unexpected error', async () => {
    const url = await startHookServer(() => {
      throw new Error('hook crashed')
    })
    const res = await selectOne(url)
    expect(res.status).toBe(500)
    expect(((await res.json()) as ApiResponse).error.code).toBe('HOOK_ERROR')
  })

  it('populates context with method, path, databaseId, remoteAddress, and headers', async () => {
    let capturedCtx: Record<string, unknown> | undefined
    const url = await startHookServer(ctx => {
      capturedCtx = { ...ctx }
      return undefined
    })

    await selectOne(url, { 'X-Custom': 'test-value' })

    expect(capturedCtx?.method).toBe('post')
    expect(capturedCtx?.path).toBe('/db/test/query')
    expect(capturedCtx?.databaseId).toBe('test')
    expect(typeof capturedCtx?.remoteAddress).toBe('string')
    expect((capturedCtx?.headers as Record<string, string>)['x-custom']).toBe('test-value')
  })
})
