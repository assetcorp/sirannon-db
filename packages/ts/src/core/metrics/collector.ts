import type { ChangeDispatchObserver } from '../cdc/subscription.js'
import type { CDCMetrics, ConnectionMetrics, MetricsConfig, QueryMetrics } from '../types.js'

export type QueryOutcome = Pick<QueryMetrics, 'rowsReturned' | 'changes' | 'rowsRead' | 'rowsWritten'>

export type QueryOutcomeMeasure<T> = (result: T) => QueryOutcome

export type QueryFailureMeasure = (error: unknown) => QueryOutcome

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
  ): Promise<T> {
    if (!this.config.onQueryComplete) {
      return fn()
    }

    const start = performance.now()
    let failed = false
    let outcome: QueryOutcome = {}
    try {
      const result = await fn()
      if (measure) outcome = measure(result)
      return result
    } catch (err) {
      failed = true
      if (measureFailure) outcome = measureFailure(err)
      throw err
    } finally {
      this.reportQuery(context, outcome, performance.now() - start, failed)
    }
  }

  reportQuery(
    context: Omit<QueryMetrics, 'durationMs' | 'error'>,
    outcome: QueryOutcome,
    durationMs: number,
    failed: boolean,
  ): void {
    try {
      this.config.onQueryComplete?.({
        ...context,
        ...outcome,
        durationMs,
        error: failed,
      })
    } catch {
      void 0
    }
  }

  trackConnection(metrics: ConnectionMetrics): void {
    try {
      if (metrics.event === 'open') {
        this.config.onConnectionOpen?.(metrics)
      } else {
        this.config.onConnectionClose?.(metrics)
      }
    } catch {
      void 0
    }
  }

  trackCDCEvent(metrics: CDCMetrics): void {
    try {
      this.config.onCDCEvent?.(metrics)
    } catch {
      void 0
    }
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
