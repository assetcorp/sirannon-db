import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { SirannonError } from '../../../core/errors.js'
import type { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const SECRET_SEGMENT = 'operator-only-segment'

let dir: string
let secretDir: string

beforeEach(() => {
  dir = scratchDirectory('server-paths')
  secretDir = join(dir, SECRET_SEGMENT)
})

afterEach(releaseServedRegistries)

function autoOpening(driver: SQLiteDriver, databasePath: (id: string) => string): Sirannon {
  return trackedRegistry({ driver, lifecycle: { autoOpen: { resolver: id => ({ path: databasePath(id) }) } } })
}

async function operatorError(sirannon: Sirannon, id: string): Promise<SirannonError> {
  const failure = await sirannon.resolve(id).then(
    () => null,
    (err: unknown) => err,
  )
  if (!(failure instanceof SirannonError)) throw new Error(`Opening '${id}' did not fail with a SirannonError`)
  return failure
}

async function socketFailure(served: ServedRegistry, id: string): Promise<{ text: string; code: string | null }> {
  const socket = await RawSocket.open(`${served.wsBase}/db/${id}`)
  const frame = await socket.next()
  const closed = await socket.closed
  return {
    text: `${JSON.stringify(frame)} ${closed.reason}`,
    code: (frame as { error?: { code?: string } }).error?.code ?? null,
  }
}

function namesNoServerPath(text: string): boolean {
  return !text.includes(dir) && !text.includes(SECRET_SEGMENT) && !text.includes('.node') && !text.includes('.db')
}

describe('errors that name server files', () => {
  it('keeps the binding path of a missing row-counting build on the server and sends clients a fixed message', async () => {
    const missingBinding = join(secretDir, 'better_sqlite3.node')
    const sirannon = autoOpening(betterSqlite3({ rowCounts: true, sqliteBindingPath: missingBinding }), id =>
      join(dir, `${id}.db`),
    )
    const served = await serveRegistry(sirannon, { acceptSql: true })

    const local = await operatorError(sirannon, 'tenant-local')
    const overHttp = await postJson(served.base, '/db/tenant-http/query', { sql: 'SELECT 1' })
    const overSocket = await socketFailure(served, 'tenant-socket')

    expect(local.code).toBe('ROW_COUNTS_UNSUPPORTED')
    expect(local.message).toContain(missingBinding)
    expect([overHttp.status, errorCodeOf(overHttp.body)]).toEqual([500, 'ROW_COUNTS_UNSUPPORTED'])
    expect((overHttp.body as { error: { details?: unknown } }).error.details).toBeUndefined()
    expect(namesNoServerPath(overHttp.text)).toBe(true)
    expect(overSocket.code).toBe('ROW_COUNTS_UNSUPPORTED')
    expect(namesNoServerPath(overSocket.text)).toBe(true)
  })

  it('keeps the path of a database file that fails to open on the server and sends clients a fixed message', async () => {
    const sirannon = autoOpening(betterSqlite3(), id => join(secretDir, 'missing-directory', `${id}.db`))
    const served = await serveRegistry(sirannon, { acceptSql: true })

    const local = await operatorError(sirannon, 'tenant-local')
    const overHttp = await postJson(served.base, '/db/tenant-http/execute', { sql: 'CREATE TABLE t (id INTEGER)' })
    const overSocket = await socketFailure(served, 'tenant-socket')

    expect(local.code).toBe('DATABASE_OPEN_FAILED')
    expect(local.message).toContain(SECRET_SEGMENT)
    expect([overHttp.status, errorCodeOf(overHttp.body)]).toEqual([500, 'DATABASE_OPEN_FAILED'])
    expect(namesNoServerPath(overHttp.text)).toBe(true)
    expect(overSocket.code).toBe('DATABASE_OPEN_FAILED')
    expect(namesNoServerPath(overSocket.text)).toBe(true)
  })
})
