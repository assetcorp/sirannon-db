import { invokeCallerCallback } from '../caller-callbacks.js'
import type { ChangeDispatchObserver } from '../cdc/subscription.js'
import { addRowCounts, type RowCounts, rowCountsOf } from '../driver/row-counts.js'
import type { CDCMetrics, ConnectionMetrics, MetricsConfig, QueryMetrics } from '../types.js'
import { type LateRowCounts, lateRowCountsOf } from './late-row-counts.js'

export type QueryOutcome = Pick<QueryMetrics, 'rowsReturned' | 'changes' | 'rowsRead' | 'rowsWritten'>

export type QueryOutcomeMeasure<T> = (result: T) => QueryOutcome

export type QueryFailureMeasure = (error: unknown) => QueryOutcome

/**
 * Returns the outcome of a statement whose caller stopped waiting for it, from the outcome known when the caller
 * received its error and the row counts that the writer worker reported afterwards, or `null` when it reported none.
 *
 * @internal
 */
export type LateOutcomeMeasure = (
  known: QueryOutcome,
  counts: readonly (RowCounts | undefined)[] | null,
) => QueryOutcome

/**
 * Returns the known outcome with the late row counts added to its own, or the known outcome unchanged when the late
 * counts never arrived.
 *
 * @param known - The outcome known when the caller received its error.
 * @param counts - The row counts that the writer worker reported afterwards, or `null`.
 * @returns The outcome to report.
 * @internal
 */
export function settleLateOutcome(
  known: QueryOutcome,
  counts: readonly (RowCounts | undefined)[] | null,
): QueryOutcome {
  if (counts === null) return known
  let total = rowCountsOf(known)
  for (const statement of counts) total = addRowCounts(total, statement)
  const { rowsRead: _read, rowsWritten: _written, ...rest } = known
  return total === undefined ? rest : { ...rest, ...total }
}

/**
 * Times each query and passes query timings, connection events, and change-capture events to the metrics callbacks that the caller configures.
 *
 * @internal
 */
export class MetricsCollector {
  private config: MetricsConfig

  constructor(config?: MetricsConfig) {
    this.config = config ?? {}
  }

  get tracksQueries(): boolean {
    return this.config.onQueryComplete !== undefined
  }

  async trackQuery<T>(
    fn: () => Promise<T>,
    context: Omit<QueryMetrics, 'durationMs' | 'error'>,
    measure?: QueryOutcomeMeasure<T>,
    measureFailure?: QueryFailureMeasure,
    measureLate: LateOutcomeMeasure = settleLateOutcome,
  ): Promise<T> {
    if (!this.config.onQueryComplete) {
      return fn()
    }

    const start = performance.now()
    let failed = false
    let outcome: QueryOutcome = {}
    let late: LateRowCounts | undefined
    try {
      const result = await fn()
      if (measure) outcome = measure(result)
      return result
    } catch (err) {
      failed = true
      if (measureFailure) outcome = measureFailure(err)
      late = lateRowCountsOf(err)
      throw err
    } finally {
      if (late === undefined) {
        this.reportQuery(context, outcome, performance.now() - start, failed)
      } else {
        const known = outcome
        void late.then(counts => this.reportQuery(context, measureLate(known, counts), performance.now() - start, true))
      }
    }
  }

  reportQuery(
    context: Omit<QueryMetrics, 'durationMs' | 'error'>,
    outcome: QueryOutcome,
    durationMs: number,
    failed: boolean,
  ): void {
    invokeCallerCallback(() => this.config.onQueryComplete?.({ ...context, ...outcome, durationMs, error: failed }))
  }

  trackConnection(metrics: ConnectionMetrics): void {
    invokeCallerCallback(() =>
      metrics.event === 'open' ? this.config.onConnectionOpen?.(metrics) : this.config.onConnectionClose?.(metrics),
    )
  }

  trackCDCEvent(metrics: CDCMetrics): void {
    invokeCallerCallback(() => this.config.onCDCEvent?.(metrics))
  }

  observeDispatch(databaseId: string): ChangeDispatchObserver | undefined {
    if (!this.config.onCDCEvent) return undefined
    return (event, subscriberCount) => {
      this.trackCDCEvent({ databaseId, table: event.table, operation: event.type, subscriberCount })
    }
  }

  get active(): boolean {
    return !!(
      this.config.onQueryComplete ||
      this.config.onConnectionOpen ||
      this.config.onConnectionClose ||
      this.config.onCDCEvent
    )
  }
}
