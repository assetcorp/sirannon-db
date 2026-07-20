import { parentPort } from 'node:worker_threads'
import type { GroupRunOutcome, SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { executeGroup } from '../query-executor.js'
import {
  serializeError,
  WORKER_CANCELLED_CODE,
  type WorkerCancel,
  type WorkerRequest,
  type WorkerResult,
} from './protocol.js'

const STATEMENT_CACHE_CAPACITY = 128

if (!parentPort) {
  throw new SirannonError('Writer worker started outside a worker thread', 'WRITER_WORKER_NO_PORT')
}
const port = parentPort

let connection: SQLiteConnection | null = null
const statements = new Map<string, SQLiteStatement>()

function requireConnection(): SQLiteConnection {
  if (!connection) {
    throw new SirannonError(
      'Writer worker received a command before the connection was opened',
      'WRITER_WORKER_NOT_OPEN',
    )
  }
  return connection
}

async function statementFor(sql: string): Promise<SQLiteStatement> {
  const conn = requireConnection()
  const cached = statements.get(sql)
  if (cached) {
    statements.delete(sql)
    statements.set(sql, cached)
    return cached
  }
  const stmt = await conn.prepare(sql)
  statements.set(sql, stmt)
  if (statements.size > STATEMENT_CACHE_CAPACITY) {
    const oldest = statements.keys().next().value
    if (oldest !== undefined) statements.delete(oldest)
  }
  return stmt
}

async function open(
  entry: { specifier: string; exportName?: string; config?: unknown },
  path: string,
  options: unknown,
) {
  const module: Record<string, unknown> = await import(entry.specifier)
  const factory = module[entry.exportName ?? 'default']
  if (typeof factory !== 'function') {
    throw new SirannonError(
      `Driver worker entry '${entry.specifier}' has no callable export '${entry.exportName ?? 'default'}'`,
      'WRITER_WORKER_BAD_ENTRY',
    )
  }
  const driver = (factory as (config?: unknown) => SQLiteDriver)(entry.config)
  connection = await driver.open(path, options as Parameters<SQLiteDriver['open']>[1])
}

async function dispatch(req: WorkerRequest): Promise<WorkerResult> {
  switch (req.kind) {
    case 'open':
      await open(req.entry, req.path, req.options)
      return undefined
    case 'exec':
      await requireConnection().exec(req.sql)
      return undefined
    case 'run':
      return (await statementFor(req.sql)).run(...req.params)
    case 'get':
      return (await statementFor(req.sql)).get(...req.params)
    case 'all':
      return (await statementFor(req.sql)).all(...req.params)
    case 'allRaw': {
      const stmt = await statementFor(req.sql)
      return stmt.allRaw ? stmt.allRaw(...req.params) : stmt.all(...req.params)
    }
    case 'runBatch': {
      const conn = requireConnection()
      if (conn.runBatch) return conn.runBatch(req.sql, req.paramsBatch)
      const stmt = await statementFor(req.sql)
      return Promise.all(req.paramsBatch.map(params => stmt.run(...params)))
    }
    case 'runBatchSummary': {
      const conn = requireConnection()
      if (conn.runBatchSummary) return conn.runBatchSummary(req.sql, req.paramsBatch)
      const stmt = await statementFor(req.sql)
      let changes = 0
      for (const params of req.paramsBatch) changes += (await stmt.run(...params)).changes
      return { rowsLoaded: req.paramsBatch.length, changes }
    }
    case 'runGroup': {
      const outcomes = await executeGroup(requireConnection(), req.units)
      return outcomes.map<GroupRunOutcome>(outcome =>
        outcome.ok ? { ok: true, results: outcome.values } : { ok: false, error: serializeError(outcome.error) },
      )
    }
    case 'close':
      if (connection) await connection.close()
      connection = null
      statements.clear()
      return undefined
  }
}

const cancelledIds = new Set<number>()
let latestDispatchedId = 0

async function handle(req: WorkerRequest): Promise<void> {
  latestDispatchedId = req.id
  if (cancelledIds.delete(req.id)) {
    port.postMessage({
      id: req.id,
      ok: false,
      error: {
        message: 'The caller abandoned this operation before the worker reached it, so it was not run',
        name: 'SirannonError',
        code: WORKER_CANCELLED_CODE,
      },
    })
    return
  }
  try {
    const value = await dispatch(req)
    port.postMessage({ id: req.id, ok: true, value })
  } catch (err) {
    port.postMessage({ id: req.id, ok: false, error: serializeError(err) })
  }
}

function settleDeliveredMessages(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve))
}

let tail: Promise<void> = Promise.resolve()
port.on('message', (msg: WorkerRequest | WorkerCancel) => {
  if (msg.kind === 'cancel') {
    if (msg.id > latestDispatchedId) cancelledIds.add(msg.id)
    return
  }
  tail = tail.then(settleDeliveredMessages).then(() => handle(msg))
})
