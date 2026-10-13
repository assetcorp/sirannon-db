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

function isOptionsObject(value: unknown): value is WriterWorkerOptions {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/**
 * Returns the writer-worker settings for one database, with every limit that the caller left out set to its default.
 *
 * @param option - The `writerWorker` value from the open call or the registry: `true`, `false`, or an options object. `null` and `undefined` leave the worker off.
 * @returns Whether the worker is on, the limit on writes in flight, the retry hint for a refused write, and the deadline and restart settings for the worker host.
 * @throws {@link SirannonError} with code `INVALID_WRITER_WORKER` when the value is neither a boolean nor an options object, or when a limit is out of range.
 * @internal
 */
export function resolveWriterWorkerConfig(option: boolean | WriterWorkerOptions | undefined): ResolvedWriterWorker {
  if (option === undefined || option === null || option === false) {
    return { enabled: false, maxPendingWrites: 0, retryAfterMs: DEFAULT_RETRY_AFTER_MS, host: {} }
  }
  if (option !== true && !isOptionsObject(option)) {
    throw new SirannonError('writerWorker must be true, false, or an options object', 'INVALID_WRITER_WORKER')
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
