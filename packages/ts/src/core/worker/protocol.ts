import type { BatchSummary, DriverWorkerEntry, GroupRunOutcome, OpenOptions, RunResult } from '../driver/types.js'

export type WorkerRequest =
  | { id: number; kind: 'open'; entry: DriverWorkerEntry; path: string; options: OpenOptions }
  | { id: number; kind: 'exec'; sql: string }
  | { id: number; kind: 'run'; sql: string; params: unknown[] }
  | { id: number; kind: 'get'; sql: string; params: unknown[] }
  | { id: number; kind: 'all'; sql: string; params: unknown[] }
  | { id: number; kind: 'allRaw'; sql: string; params: unknown[] }
  | { id: number; kind: 'runBatch'; sql: string; paramsBatch: unknown[][] }
  | { id: number; kind: 'runBatchSummary'; sql: string; paramsBatch: unknown[][] }
  | { id: number; kind: 'runGroup'; batch: { sql: string; params: unknown[] }[] }
  | { id: number; kind: 'close' }

type DistributiveOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never

export type WorkerRequestBody = DistributiveOmit<WorkerRequest, 'id'>

export type WorkerResult = undefined | RunResult | RunResult[] | BatchSummary | GroupRunOutcome[] | unknown[] | unknown

export interface SerializedError {
  message: string
  name?: string
  code?: string
}

export type WorkerResponse =
  | { id: number; ok: true; value: WorkerResult }
  | { id: number; ok: false; error: SerializedError }

export function serializeError(err: unknown): SerializedError {
  if (err instanceof Error) {
    const code = (err as { code?: unknown }).code
    return {
      message: err.message,
      name: err.name,
      ...(typeof code === 'string' ? { code } : {}),
    }
  }
  return { message: String(err) }
}

export function deserializeError(error: SerializedError): Error {
  const err = new Error(error.message)
  if (error.name) err.name = error.name
  if (error.code) (err as { code?: string }).code = error.code
  return err
}
