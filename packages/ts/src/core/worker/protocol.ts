import { rowCountsOf, withRowCounts } from '../driver/row-counts.js'
import type {
  BatchSummary,
  DatabaseCopyStep,
  DriverWorkerEntry,
  GroupRunOutcome,
  GroupRunStatement,
  OpenOptions,
  RunResult,
} from '../driver/types.js'
import { SirannonError } from '../errors.js'

export interface WorkerGroupStatement {
  sql: string
  params: unknown[]
  trusted?: boolean
}

/**
 * Converts a grouped statement into the form that the host posts to the writer worker, with its parameters copied.
 *
 * @param statement - The statement.
 * @returns The statement as the worker receives it.
 * @internal
 */
export function toWorkerGroupStatement(statement: GroupRunStatement): WorkerGroupStatement {
  return {
    sql: statement.sql,
    params: statement.params ? [...statement.params] : [],
    ...(statement.trusted === true ? { trusted: true } : {}),
  }
}

type WorkerRequestKinds =
  | { id: number; kind: 'open'; entry: DriverWorkerEntry; path: string; options: OpenOptions }
  | { id: number; kind: 'exec'; sql: string }
  | { id: number; kind: 'run'; sql: string; params: unknown[] }
  | { id: number; kind: 'get'; sql: string; params: unknown[] }
  | { id: number; kind: 'all'; sql: string; params: unknown[] }
  | { id: number; kind: 'allRaw'; sql: string; params: unknown[] }
  | { id: number; kind: 'allCounted'; sql: string; params: unknown[] }
  | { id: number; kind: 'getCounted'; sql: string; params: unknown[] }
  | { id: number; kind: 'allRawCounted'; sql: string; params: unknown[] }
  | { id: number; kind: 'runBatch'; sql: string; paramsBatch: unknown[][] }
  | { id: number; kind: 'runBatchSummary'; sql: string; paramsBatch: unknown[][] }
  | {
      id: number
      kind: 'runGroup'
      units: { statements: WorkerGroupStatement[] }[]
      opening?: WorkerGroupStatement
    }
  | { id: number; kind: 'loadExtension'; path: string }
  | { id: number; kind: 'copyDatabase'; destPath: string; pagesPerStep: number; stallTimeoutMs?: number }
  | { id: number; kind: 'close' }

export type WorkerRequest = WorkerRequestKinds & { begin?: true }

export type WorkerCancel = { kind: 'cancel'; id: number }

export const WORKER_CANCELLED_CODE = 'WRITER_WORKER_CANCELLED'

export const WORKER_BEGIN_FAILED_CODE = 'WRITER_WORKER_BEGIN_FAILED'

const KINDS_THAT_CARRY_BEGIN = new Set<WorkerRequestKinds['kind']>([
  'exec',
  'run',
  'get',
  'all',
  'allRaw',
  'allCounted',
  'getCounted',
  'allRawCounted',
  'runBatch',
  'runBatchSummary',
])

/**
 * Returns whether a request of this kind can carry the `BEGIN` of the transaction that the host has opened, which
 * the worker executes just before the request, so that the transaction costs no round trip of its own.
 *
 * @param kind - The request's kind.
 * @returns `true` for a statement request, and `false` for the requests that manage the worker or run a group.
 * @internal
 */
export function canCarryBegin(kind: WorkerRequestKinds['kind']): boolean {
  return KINDS_THAT_CARRY_BEGIN.has(kind)
}

/**
 * Returns whether the worker's error for a request that carried `BEGIN` means that the worker never began the
 * transaction, because `BEGIN` failed or because the worker dropped the request before it reached it.
 *
 * @param error - The error that the worker sent.
 * @returns `true` when the transaction never began.
 * @internal
 */
export function leftTransactionUnbegun(error: SerializedError): boolean {
  return error.code === WORKER_BEGIN_FAILED_CODE || error.code === WORKER_CANCELLED_CODE
}

type DistributiveOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never

export type WorkerRequestBody = DistributiveOmit<WorkerRequest, 'id'>

export type WorkerResult = undefined | RunResult | RunResult[] | BatchSummary | GroupRunOutcome[] | unknown[] | unknown

export interface SerializedError {
  message: string
  name?: string
  code?: string
  sql?: string
  rowsRead?: number
  rowsWritten?: number
  sirannonError?: true
}

export type WorkerCopyStep = { id: number; kind: 'copyStep'; step: DatabaseCopyStep }

export type WorkerResponse =
  | { id: number; ok: true; value: WorkerResult }
  | { id: number; ok: false; error: SerializedError }
  | WorkerCopyStep

export const WORKER_COPY_STOPPED_CODE = 'WRITER_WORKER_COPY_STOPPED'

export function serializeError(err: unknown): SerializedError {
  if (err instanceof Error) {
    const { code, sql } = err as { code?: unknown; sql?: unknown }
    return withRowCounts(
      {
        message: err.message,
        name: err.name,
        ...(typeof code === 'string' ? { code } : {}),
        ...(typeof sql === 'string' ? { sql } : {}),
        ...(err instanceof SirannonError ? { sirannonError: true as const } : {}),
      },
      rowCountsOf(err),
    )
  }
  return { message: String(err) }
}

export function deserializeError(error: SerializedError): Error {
  const err =
    error.sirannonError === true && error.code !== undefined
      ? new SirannonError(error.message, error.code)
      : Object.assign(new Error(error.message), error.code === undefined ? {} : { code: error.code })
  if (error.name) err.name = error.name
  return withRowCounts(err, rowCountsOf(error))
}
