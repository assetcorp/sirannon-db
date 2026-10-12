import {
  fireAfterQueryHooks,
  fireBeforeQueryHooks,
  STATEMENT_SUCCEEDED,
  type StatementOutcome,
} from './hooks/query-hooks.js'
import type { HookRegistry } from './hooks/registry.js'
import type { MetricsCollector, QueryFailureMeasure, QueryOutcome, QueryOutcomeMeasure } from './metrics/collector.js'
import { failedStatementOutcome, leavesOutcomeUnknown, ROW_COUNTS_UNKNOWN } from './metrics/outcomes.js'
import type { StatementMeter } from './transaction.js'
import type { ExecuteResult, Params, QueryMetrics, QueryOptions } from './types.js'

interface ObservedStatement {
  sql: string
  params?: Params
}

export interface TransactionOutcomes {
  succeeded(results: readonly ExecuteResult[]): readonly QueryOutcome[]
  failed(): readonly QueryOutcome[]
}

type QueryContext = Omit<QueryMetrics, 'durationMs' | 'error'>

export class DatabaseObserver implements StatementMeter {
  constructor(
    private readonly databaseId: string,
    private readonly localHooks: HookRegistry,
    private readonly parentHooks: HookRegistry | null,
    private readonly metrics: MetricsCollector | null,
    private readonly countsRows: boolean,
  ) {}

  get observesQueries(): boolean {
    return (
      this.metrics !== null ||
      this.localHooks.has('beforeQuery') ||
      this.localHooks.has('afterQuery') ||
      (this.parentHooks?.has('beforeQuery') ?? false) ||
      (this.parentHooks?.has('afterQuery') ?? false)
    )
  }

  get meter(): StatementMeter | null {
    return this.metrics?.tracksQueries ? this : null
  }

  async withQueryHooks<T>(
    sql: string,
    params: Params | undefined,
    options: QueryOptions | undefined,
    op: () => Promise<T>,
  ): Promise<T> {
    this.fireBefore(sql, params, options)
    const start = performance.now()
    try {
      const result = await op()
      this.fireAfter(sql, params, performance.now() - start, STATEMENT_SUCCEEDED)
      return result
    } catch (error) {
      this.fireAfter(sql, params, performance.now() - start, { failed: true, error })
      throw error
    }
  }

  track<T>(
    sql: string,
    op: () => Promise<T>,
    measure?: QueryOutcomeMeasure<T>,
    measureFailure: QueryFailureMeasure = failedStatementOutcome,
    source?: 'live',
  ): Promise<T> {
    if (!this.metrics) return op()
    return this.metrics.trackQuery(
      op,
      this.context(sql, source),
      result => (measure ? measure(result) : {}),
      error => this.completeFailure(measureFailure(error)),
    )
  }

  async withTransactionHooks(
    statements: readonly ObservedStatement[],
    op: () => Promise<ExecuteResult[]>,
    outcomes: TransactionOutcomes,
  ): Promise<ExecuteResult[]> {
    for (const statement of statements) this.fireBefore(statement.sql, statement.params)
    const start = performance.now()
    try {
      const results = await this.trackEach(statements, op, outcomes)
      this.fireAfterEach(statements, performance.now() - start, STATEMENT_SUCCEEDED)
      return results
    } catch (error) {
      this.fireAfterEach(statements, performance.now() - start, { failed: true, error })
      throw error
    }
  }

  private context(sql: string, source?: 'live'): QueryContext {
    return source ? { databaseId: this.databaseId, sql, source } : { databaseId: this.databaseId, sql }
  }

  private completeFailure(outcome: QueryOutcome): QueryOutcome {
    if (!this.countsRows || outcome === ROW_COUNTS_UNKNOWN || outcome.rowsRead !== undefined) return outcome
    return { ...outcome, rowsRead: 0, rowsWritten: 0 }
  }

  private fireAfterEach(statements: readonly ObservedStatement[], durationMs: number, outcome: StatementOutcome): void {
    for (const statement of statements) this.fireAfter(statement.sql, statement.params, durationMs, outcome)
  }

  private async trackEach(
    statements: readonly ObservedStatement[],
    op: () => Promise<ExecuteResult[]>,
    outcomes: TransactionOutcomes,
  ): Promise<ExecuteResult[]> {
    const metrics = this.metrics
    if (!metrics?.tracksQueries) return op()
    const start = performance.now()
    let results: ExecuteResult[]
    try {
      results = await op()
    } catch (error) {
      const unreported = leavesOutcomeUnknown(error) ? ROW_COUNTS_UNKNOWN : {}
      this.reportEach(metrics, statements, outcomes.failed(), performance.now() - start, unreported)
      throw error
    }
    this.reportEach(metrics, statements, outcomes.succeeded(results), performance.now() - start, null)
    return results
  }

  private reportEach(
    metrics: MetricsCollector,
    statements: readonly ObservedStatement[],
    perStatement: readonly QueryOutcome[],
    durationMs: number,
    failedWithout: QueryOutcome | null,
  ): void {
    const failed = failedWithout !== null
    for (let i = 0; i < statements.length; i++) {
      const outcome = perStatement[i] ?? failedWithout ?? {}
      metrics.reportQuery(
        this.context(statements[i].sql),
        failed ? this.completeFailure(outcome) : outcome,
        durationMs,
        failed,
      )
    }
  }

  private fireBefore(sql: string, params?: Params, options?: QueryOptions): void {
    fireBeforeQueryHooks(this.parentHooks, this.localHooks, this.databaseId, sql, params, options)
  }

  private fireAfter(sql: string, params: Params | undefined, durationMs: number, outcome: StatementOutcome): void {
    fireAfterQueryHooks(this.parentHooks, this.localHooks, this.databaseId, sql, params, durationMs, outcome)
  }
}
