import { rowCountsOf, withRowCounts } from '../driver/row-counts.js'
import type {
  BatchSummary,
  DatabaseCopyStep,
  DriverWorkerEntry,
  GroupRunOutcome,
  OpenOptions,
  RunResult,
} from '../driver/types.js'
import { SirannonError } from '../errors.js'

export type WorkerRequest =
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
  | { id: number; kind: 'runGroup'; units: { statements: { sql: string; params: unknown[]; trusted?: boolean }[] }[] }
  | { id: number; kind: 'loadExtension'; path: string }
  | { id: number; kind: 'copyDatabase'; destPath: string; pagesPerStep: number; stallTimeoutMs?: number }
  | { id: number; kind: 'close' }

export type WorkerCancel = { kind: 'cancel'; id: number }

export const WORKER_CANCELLED_CODE = 'WRITER_WORKER_CANCELLED'

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
