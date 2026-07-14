import { SirannonError } from '../errors.js'
import type { WriterWorkerOptions } from '../types.js'
import type { WorkerHostOptions } from './host.js'

export const DEFAULT_MAX_PENDING_WRITES = 1024
export const DEFAULT_RETRY_AFTER_MS = 1_000

export interface ResolvedWriterWorker {
  enabled: boolean
  maxPendingWrites: number
  retryAfterMs: number
  host: WorkerHostOptions
}

function requireCount(value: number, name: string, min: number): number {
  if (!Number.isInteger(value) || value < min) {
    throw new SirannonError(`writerWorker.${name} must be an integer >= ${min}`, 'INVALID_WRITER_WORKER')
  }
  return value
}

export function resolveWriterWorkerConfig(option: boolean | WriterWorkerOptions | undefined): ResolvedWriterWorker {
  if (!option) {
    return { enabled: false, maxPendingWrites: 0, retryAfterMs: DEFAULT_RETRY_AFTER_MS, host: {} }
  }

  const opts = option === true ? {} : option
  const host: WorkerHostOptions = {}
  if (opts.writeTimeoutMs !== undefined) host.writeTimeoutMs = requireCount(opts.writeTimeoutMs, 'writeTimeoutMs', 0)
  if (opts.maxRestarts !== undefined) host.maxRestarts = requireCount(opts.maxRestarts, 'maxRestarts', 0)

  const maxPendingWrites =
    opts.maxPendingWrites === undefined
      ? DEFAULT_MAX_PENDING_WRITES
      : requireCount(opts.maxPendingWrites, 'maxPendingWrites', 1)

  return { enabled: true, maxPendingWrites, retryAfterMs: DEFAULT_RETRY_AFTER_MS, host }
}
