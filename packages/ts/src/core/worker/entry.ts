import { parentPort } from 'node:worker_threads'
import { addRowCounts, type RowCounts, rowCountsOf, withRowCounts } from '../driver/row-counts.js'
import type { GroupRunOutcome, SQLiteConnection, SQLiteDriver, SQLiteStatement } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { executeGroup } from '../group-executor.js'
import {
  serializeError,
  WORKER_CANCELLED_CODE,
  WORKER_COPY_STOPPED_CODE,
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

function missingCountedCall(call: string): SirannonError {
  return new SirannonError(
    `The driver rebuilt inside the writer worker opens statements without ${call}, so it reports no row counts`,
    'ROW_COUNTS_UNSUPPORTED',
  )
}

async function allCounted(sql: string, params: unknown[]): Promise<WorkerResult> {
  const stmt = await statementFor(sql)
  if (!stmt.allCounted) throw missingCountedCall('allCounted')
  return stmt.allCounted(...params)
}

async function getCounted(sql: string, params: unknown[]): Promise<WorkerResult> {
  const stmt = await statementFor(sql)
  if (!stmt.getCounted) throw missingCountedCall('getCounted')
  return stmt.getCounted(...params)
}

async function allRawCounted(sql: string, params: unknown[]): Promise<WorkerResult> {
  const stmt = await statementFor(sql)
  if (!stmt.allRaw) return allCounted(sql, params)
  if (!stmt.allRawCounted) throw missingCountedCall('allRawCounted')
  return stmt.allRawCounted(...params)
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

const runningCopies = new Map<number, { aborted: boolean }>()

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
    case 'allCounted':
      return allCounted(req.sql, req.params)
    case 'getCounted':
      return getCounted(req.sql, req.params)
    case 'allRawCounted':
      return allRawCounted(req.sql, req.params)
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
      let counts: RowCounts | undefined
      for (const params of req.paramsBatch) {
        const result = await stmt.run(...params)
        changes += result.changes
        counts = addRowCounts(counts, rowCountsOf(result))
      }
      return withRowCounts({ rowsLoaded: req.paramsBatch.length, changes }, counts)
    }
    case 'runGroup': {
      const outcomes = await executeGroup(requireConnection(), req.units)
      return outcomes.map<GroupRunOutcome>(outcome => {
        if (outcome.ok) return { ok: true, results: outcome.values }
        const error = serializeError(outcome.error)
        return outcome.statementRowCounts
          ? { ok: false, error, statementRowCounts: outcome.statementRowCounts }
          : { ok: false, error }
      })
    }
    case 'loadExtension': {
      const conn = requireConnection()
      if (!conn.loadExtension) {
        throw new SirannonError(
          'The driver rebuilt inside the writer worker opens connections without an extension loading call',
          'EXTENSION_ERROR',
        )
      }
      await conn.loadExtension(req.path)
      return undefined
    }
    case 'copyDatabase': {
      const conn = requireConnection()
      if (!conn.copyDatabase) {
        throw new SirannonError(
          'The driver rebuilt inside the writer worker opens connections without a stepped copy call',
          'BACKUP_UNSUPPORTED',
        )
      }
      const copy = { aborted: false }
      runningCopies.set(req.id, copy)
      try {
        return await conn.copyDatabase({
          destPath: req.destPath,
          pagesPerStep: req.pagesPerStep,
          onStep: step => {
            if (copy.aborted) {
              throw new SirannonError('The caller stopped this copy', WORKER_COPY_STOPPED_CODE)
            }
            port.postMessage({ id: req.id, kind: 'copyStep', step })
            return req.pagesPerStep
          },
        })
      } finally {
        runningCopies.delete(req.id)
      }
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

async function handle(req: WorkerRequest, queued = true): Promise<void> {
  if (queued) latestDispatchedId = req.id
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
    const copy = runningCopies.get(msg.id)
    if (copy) {
      copy.aborted = true
      return
    }
    if (msg.id > latestDispatchedId) cancelledIds.add(msg.id)
    return
  }
  if (msg.kind === 'copyDatabase') {
    void handle(msg, false)
    return
  }
  tail = tail.then(settleDeliveredMessages).then(() => handle(msg))
})
