import type { DatabaseCopyStep } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import type { WorkerRequestBody } from './protocol.js'

export interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: NodeJS.Timeout | null
  graceTimer: NodeJS.Timeout | null
  deadlineMs: number
  cancellable: boolean
  kind: WorkerRequestBody['kind']
  onStep?: (step: DatabaseCopyStep) => void
}

export function unresponsiveError(waitedMs: number, kind?: WorkerRequestBody['kind']): SirannonError {
  if (kind === 'copyDatabase') {
    return new SirannonError(
      `The writer worker moved no page of the copy for ${waitedMs}ms, so the copy was stopped and nothing it wrote is usable`,
      'BACKUP_STALLED',
    )
  }
  return new SirannonError(
    `Writer worker did not respond within ${waitedMs}ms; the operation's outcome is unknown`,
    'WRITER_WORKER_TIMEOUT',
  )
}

export function copyStallDeadline(request: WorkerRequestBody): number | undefined {
  return request.kind === 'copyDatabase' ? request.stallTimeoutMs : undefined
}

export function clearPendingTimers(entry: PendingRequest): void {
  if (entry.timer) clearTimeout(entry.timer)
  if (entry.graceTimer) clearTimeout(entry.graceTimer)
}
