import type {
  BatchSummary,
  Counted,
  DatabaseCopyStep,
  GroupRunOutcome,
  RunResult,
  SQLiteConnection,
  SQLiteStatement,
} from '../driver/types.js'
import { toWorkerGroupStatement, type WorkerRequestBody } from './protocol.js'

/**
 * Sends one request to the writer worker and resolves with the worker's reply.
 *
 * @internal
 */
export type WorkerHostRequest = (
  request: WorkerRequestBody,
  onStep?: (step: DatabaseCopyStep) => void,
) => Promise<unknown>

/**
 * The connection calls that the writer worker host implements itself, because they track the host's own state.
 *
 * @internal
 */
export type WorkerHostOwnCalls = Pick<SQLiteConnection, 'transaction' | 'close' | 'loadExtension'>

function countedCalls(
  request: WorkerHostRequest,
  sql: string,
): Pick<SQLiteStatement, 'allCounted' | 'getCounted' | 'allRawCounted'> {
  return {
    allCounted: <T = unknown>(...params: unknown[]) =>
      request({ kind: 'allCounted', sql, params }) as Promise<Counted<T[]>>,
    getCounted: <T = unknown>(...params: unknown[]) =>
      request({ kind: 'getCounted', sql, params }) as Promise<Counted<T | undefined>>,
    allRawCounted: <T = unknown>(...params: unknown[]) =>
      request({ kind: 'allRawCounted', sql, params }) as Promise<Counted<T[]>>,
  }
}

/**
 * Returns the connection whose calls the writer worker host sends to the worker thread.
 *
 * @param request - Sends one request to the worker.
 * @param countsRows - `true` when the worker's driver reports row counts, so that statements carry the counted calls.
 * @param ownCalls - The transaction, close, and extension calls that the host implements.
 * @returns The connection that Sirannon writes through.
 * @internal
 */
export function workerHostConnection(
  request: WorkerHostRequest,
  countsRows: boolean,
  ownCalls: WorkerHostOwnCalls,
): SQLiteConnection {
  return {
    exec: sql => request({ kind: 'exec', sql }) as Promise<void>,
    prepare: async sql => ({
      all: <T = unknown>(...params: unknown[]) => request({ kind: 'all', sql, params }) as Promise<T[]>,
      get: <T = unknown>(...params: unknown[]) => request({ kind: 'get', sql, params }) as Promise<T | undefined>,
      run: (...params: unknown[]) => request({ kind: 'run', sql, params }) as Promise<RunResult>,
      allRaw: <T = unknown>(...params: unknown[]) => request({ kind: 'allRaw', sql, params }) as Promise<T[]>,
      ...(countsRows ? countedCalls(request, sql) : {}),
    }),
    runBatch: (sql, paramsBatch) =>
      request({ kind: 'runBatch', sql, paramsBatch: paramsBatch as unknown[][] }) as Promise<RunResult[]>,
    runBatchSummary: (sql, paramsBatch) =>
      request({ kind: 'runBatchSummary', sql, paramsBatch: paramsBatch as unknown[][] }) as Promise<BatchSummary>,
    runGroup: (units, opening) =>
      request({
        kind: 'runGroup',
        units: units.map(unit => ({ statements: unit.statements.map(toWorkerGroupStatement) })),
        ...(opening === undefined ? {} : { opening: toWorkerGroupStatement(opening) }),
      }) as Promise<GroupRunOutcome[]>,
    copyRunsOffCallerThread: true,
    copyDatabase: copyRequest =>
      request(
        {
          kind: 'copyDatabase',
          destPath: copyRequest.destPath,
          pagesPerStep: copyRequest.pagesPerStep,
          ...(copyRequest.stallTimeoutMs === undefined ? {} : { stallTimeoutMs: copyRequest.stallTimeoutMs }),
        },
        copyRequest.onStep,
      ) as Promise<DatabaseCopyStep>,
    startChangeCapture: async () => {
      await request({ kind: 'startChangeCapture' })
    },
    takeCapturedChanges: () => request({ kind: 'takeCapturedChanges' }) as Promise<Uint8Array>,
    stopChangeCapture: async () => {
      await request({ kind: 'stopChangeCapture' })
    },
    ...ownCalls,
  }
}
