import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  type ExecuteResult,
  HookDeniedError,
  type Params,
  type QueryOptions,
  type RequestContext,
  type RequestDenial,
  type ServerExecutionTarget,
  Sirannon,
  type Transaction,
} from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { createServer } from '@delali/sirannon-db/server'
import {
  ACTIVITY_LIST_SQL,
  ALLOCATE_PRODUCT_SQL,
  DEFAULT_DEMO_TOKEN,
  DELETE_ACTIVITY_SQL,
  DELETE_PRODUCTS_SQL,
  INSERT_ACTIVITY_SQL,
  INSERT_PRODUCT_SQL,
  PRODUCT_LIST_SQL,
  RECEIVE_PRODUCT_SQL,
  RESET_SEQUENCE_SQL,
  SEED_PRODUCTS,
  toWebSocketAuthProtocol,
} from './lib/sql'

const HOST = process.env.HOST ?? '127.0.0.1'
const PORT = Number(process.env.PORT ?? 9876)
const DEMO_TOKEN = process.env.SIRANNON_DEMO_TOKEN ?? DEFAULT_DEMO_TOKEN
const WEBSOCKET_AUTH_PROTOCOL = toWebSocketAuthProtocol(DEMO_TOKEN)
const APP_ORIGINS = (process.env.APP_ORIGIN ?? 'http://localhost:3000')
  .split(',')
  .map(origin => origin.trim())
  .filter(origin => origin.length > 0)

const READ_SQL = new Set([normaliseSql(PRODUCT_LIST_SQL), normaliseSql(ACTIVITY_LIST_SQL)])

const WRITE_SQL = new Set([
  normaliseSql(ALLOCATE_PRODUCT_SQL),
  normaliseSql(RECEIVE_PRODUCT_SQL),
  normaliseSql(INSERT_PRODUCT_SQL),
  normaliseSql(INSERT_ACTIVITY_SQL),
  normaliseSql(DELETE_ACTIVITY_SQL),
  normaliseSql(DELETE_PRODUCTS_SQL),
  normaliseSql(RESET_SEQUENCE_SQL),
])

function normaliseSql(sql: string): string {
  return sql.trim().replace(/\s+/g, ' ')
}

function isNumberParam(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function isIntegerParam(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value)
}

function isStringParam(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 80
}

function assertAllowedQuery(sql: string, params?: Params): void {
  if (!READ_SQL.has(normaliseSql(sql)) || params !== undefined) {
    throw new HookDeniedError('demoSqlAllowlist', 'Query is not allowed by this demo')
  }
}

function assertAllowedExecute(sql: string, params?: Params): void {
  const normalised = normaliseSql(sql)

  if (!WRITE_SQL.has(normalised)) {
    throw new HookDeniedError('demoSqlAllowlist', 'Statement is not allowed by this demo')
  }

  if (!isAllowedParams(normalised, params)) {
    throw new HookDeniedError('demoSqlAllowlist', 'Statement parameters are not allowed by this demo')
  }
}

function isAllowedParams(sql: string, params?: Params): boolean {
  if (sql === normaliseSql(DELETE_ACTIVITY_SQL) || sql === normaliseSql(DELETE_PRODUCTS_SQL)) {
    return params === undefined
  }

  if (!Array.isArray(params)) {
    return false
  }

  if (sql === normaliseSql(ALLOCATE_PRODUCT_SQL)) {
    return params.length === 1 && isIntegerParam(params[0]) && params[0] > 0
  }

  if (sql === normaliseSql(RECEIVE_PRODUCT_SQL)) {
    return (
      params.length === 2 &&
      isIntegerParam(params[0]) &&
      params[0] > 0 &&
      params[0] <= 1_000 &&
      isIntegerParam(params[1]) &&
      params[1] > 0
    )
  }

  if (sql === normaliseSql(INSERT_PRODUCT_SQL)) {
    return (
      params.length === 3 &&
      isStringParam(params[0]) &&
      isNumberParam(params[1]) &&
      params[1] > 0 &&
      params[1] <= 100_000 &&
      isIntegerParam(params[2]) &&
      params[2] >= 0 &&
      params[2] <= 100_000
    )
  }

  if (sql === normaliseSql(INSERT_ACTIVITY_SQL)) {
    return (
      params.length === 3 &&
      isStringParam(params[0]) &&
      (params[1] === 'allocated' || params[1] === 'received' || params[1] === 'created') &&
      isIntegerParam(params[2]) &&
      params[2] >= 0 &&
      params[2] <= 100_000
    )
  }

  if (sql === normaliseSql(RESET_SEQUENCE_SQL)) {
    return params.length === 2 && params[0] === 'activity' && params[1] === 'products'
  }

  return false
}

function createGuardedTransaction(tx: Transaction): Transaction {
  const guarded = {
    query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
      assertAllowedQuery(sql, params)
      return tx.query<T>(sql, params)
    },
    execute(sql: string, params?: Params): Promise<ExecuteResult> {
      assertAllowedExecute(sql, params)
      return tx.execute(sql, params)
    },
    executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
      for (const params of paramsBatch) {
        assertAllowedExecute(sql, params)
      }
      return tx.executeBatch(sql, paramsBatch)
    },
    get lastInsertRowId(): number | bigint {
      return tx.lastInsertRowId
    },
  }

  return guarded as Transaction
}

function createGuardedTarget(target: ServerExecutionTarget): ServerExecutionTarget {
  return {
    query<T = Record<string, unknown>>(sql: string, params?: Params, options?: QueryOptions): Promise<T[]> {
      assertAllowedQuery(sql, params)
      return target.query<T>(sql, params, options)
    },
    execute(): Promise<ExecuteResult> {
      throw new HookDeniedError('demoSqlAllowlist', 'Single-statement execute is disabled by this demo')
    },
    transaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T> {
      return target.transaction(tx => fn(createGuardedTransaction(tx)), options)
    },
  }
}

function isAuthorizedHeader(value: string | undefined): boolean {
  return value === `Bearer ${DEMO_TOKEN}`
}

function getHeader(headers: Record<string, string>, name: string): string | undefined {
  const direct = headers[name] ?? headers[name.toLowerCase()]
  if (direct !== undefined) {
    return direct
  }

  const lowerName = name.toLowerCase()
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === lowerName) {
      return value
    }
  }

  return undefined
}

function isAllowedOrigin(value: string | undefined): boolean {
  return value !== undefined && APP_ORIGINS.includes(value)
}

function hasWebSocketAuthProtocol(value: string | undefined): boolean {
  if (value === undefined) {
    return false
  }

  return value.split(',').some(protocol => protocol.trim() === WEBSOCKET_AUTH_PROTOCOL)
}

function validateWebSocketUpgrade(ctx: RequestContext): RequestDenial | undefined {
  if (!isAllowedOrigin(getHeader(ctx.headers, 'origin'))) {
    return {
      status: 403,
      code: 'FORBIDDEN_ORIGIN',
      message: 'The demo data server rejects WebSocket upgrades from untrusted origins.',
    }
  }

  if (!hasWebSocketAuthProtocol(getHeader(ctx.headers, 'sec-websocket-protocol'))) {
    return {
      status: 401,
      code: 'UNAUTHORIZED',
      message: 'The demo data server requires the configured WebSocket auth protocol.',
    }
  }

  return undefined
}

const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-inventory-'))

const driver = betterSqlite3()
const sirannon = new Sirannon({
  driver,
  hooks: {
    onDatabaseOpen: [ctx => console.log(`[hook] Database opened: ${ctx.databaseId}`)],
    onDatabaseClose: [ctx => console.log(`[hook] Database closed: ${ctx.databaseId}`)],
  },
})

const db = await sirannon.open('main', join(tempDir, 'inventory.db'), {
  readPoolSize: 4,
  walMode: true,
  cdcPollInterval: 50,
})

await db.execute(`
  CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0
  )
`)

await db.execute(`
  CREATE TABLE IF NOT EXISTS activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    action TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  )
`)

await db.watch('products')
await db.watch('activity')

for (const product of SEED_PRODUCTS) {
  await db.execute(INSERT_PRODUCT_SQL, [product.name, product.price, product.stock])
}

console.log('Seed data inserted.')

const guardedDb = createGuardedTarget(db)
const server = createServer(sirannon, {
  host: HOST,
  port: PORT,
  cors: {
    origin: APP_ORIGINS,
    methods: ['GET', 'POST', 'OPTIONS'],
    headers: ['Content-Type', 'Authorization'],
  },
  onRequest: ctx => {
    const websocketUpgrade = ctx.method.toUpperCase() === 'GET' && ctx.path === '/db/main'
    if (websocketUpgrade) {
      return validateWebSocketUpgrade(ctx)
    }

    const authorization = getHeader(ctx.headers, 'authorization')
    if (isAuthorizedHeader(authorization)) {
      return undefined
    }

    return {
      status: 401,
      code: 'UNAUTHORIZED',
      message: 'The demo data server requires the configured bearer token.',
    }
  },
  resolveExecutionTarget: databaseId => {
    return databaseId === 'main' ? guardedDb : null
  },
})

await server.listen()
console.log(`Sirannon data server listening on ${HOST}:${PORT}`)
console.log(`  HTTP: http://localhost:${PORT}`)
console.log(`  WS:   ws://localhost:${PORT}`)

let isShuttingDown = false

const shutdown = async () => {
  if (isShuttingDown) {
    return
  }

  isShuttingDown = true
  let exitCode = 0

  try {
    await server.close()
  } catch (error) {
    exitCode = 1
    console.error('Failed to close server during shutdown.', error)
  }

  try {
    await sirannon.shutdown()
  } catch (error) {
    exitCode = 1
    console.error('Failed to shut down Sirannon during shutdown.', error)
  }

  try {
    rmSync(tempDir, { recursive: true, force: true })
  } catch (error) {
    exitCode = 1
    console.error('Failed to remove temporary directory during shutdown.', error)
  }

  process.exit(exitCode)
}

process.once('SIGTERM', shutdown)
process.once('SIGINT', shutdown)
