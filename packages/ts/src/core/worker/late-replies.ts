import { type RowCounts, rowCountsOf, totalRowCounts } from '../driver/row-counts.js'
import type { GroupRunOutcome } from '../driver/types.js'
import { withLateGroupOutcomes, withLateRowCounts } from '../metrics/late-row-counts.js'
import type { WorkerRequestBody } from './protocol.js'

/**
 * Holds the writer worker's answer to a request that the host stopped waiting for: the value, or the error in the
 * form that the worker sent it.
 *
 * @internal
 */
export type LateReply = { ok: true; value: unknown } | { ok: false; error: unknown }

const KINDS_WITHOUT_ROW_COUNTS = new Set<WorkerRequestBody['kind']>(['open', 'close', 'copyDatabase', 'loadExtension'])

function statementCounts(reply: LateReply): (RowCounts | undefined)[] {
  if (!reply.ok) return [rowCountsOf(reply.error)]
  return [Array.isArray(reply.value) ? totalRowCounts(reply.value) : rowCountsOf(reply.value)]
}

function groupOutcomes(reply: LateReply): readonly GroupRunOutcome[] {
  return reply.ok && Array.isArray(reply.value) ? (reply.value as GroupRunOutcome[]) : []
}

/**
 * Keeps, for each request that timed out on the host, a promise of the answer that the writer worker will still send,
 * so that Sirannon can report the row counts of work that SQLite finished after the caller received
 * `WRITER_WORKER_TIMEOUT`.
 *
 * @internal
 */
export class LateReplies {
  private readonly waiting = new Map<number, (reply: LateReply | null) => void>()

  /**
   * Starts waiting for the answer to a request that timed out, and records on the caller's error what that answer
   * will report.
   *
   * @param id - The request's ID.
   * @param kind - The request's kind.
   * @param error - The timeout error that the caller receives.
   * @returns `error` itself.
   */
  expect<E extends object>(id: number, kind: WorkerRequestBody['kind'], error: E): E {
    if (KINDS_WITHOUT_ROW_COUNTS.has(kind)) return error
    const reply = new Promise<LateReply | null>(resolve => this.waiting.set(id, resolve))
    if (kind === 'runGroup') {
      return withLateGroupOutcomes(
        error,
        reply.then(answer => (answer === null ? null : groupOutcomes(answer))),
      )
    }
    return withLateRowCounts(
      error,
      reply.then(answer => (answer === null ? null : statementCounts(answer))),
    )
  }

  /**
   * Hands the writer worker's answer to whoever waits for it.
   *
   * @param id - The request's ID.
   * @param reply - The answer.
   * @returns `true` when the host was waiting for this answer.
   */
  settle(id: number, reply: LateReply): boolean {
    const resolve = this.waiting.get(id)
    if (resolve === undefined) return false
    this.waiting.delete(id)
    resolve(reply)
    return true
  }

  /**
   * Gives up every answer that the host still waits for, once the writer worker that owed them has exited,
   * crashed, or closed.
   */
  abandonAll(): void {
    const waiting = [...this.waiting.values()]
    this.waiting.clear()
    for (const resolve of waiting) resolve(null)
  }
}
