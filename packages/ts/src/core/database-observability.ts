import {
  fireAfterQueryHooks,
  fireBeforeQueryHooks,
  STATEMENT_SUCCEEDED,
  type StatementOutcome,
} from './hooks/query-hooks.js'
import type { HookRegistry } from './hooks/registry.js'
import type { MetricsCollector, QueryOutcomeMeasure } from './metrics/collector.js'
import type { ExecuteResult, Params, QueryOptions } from './types.js'

interface ObservedStatement {
  sql: string
  params?: Params
}

export class DatabaseObserver {
  constructor(
    private readonly databaseId: string,
    private readonly localHooks: HookRegistry,
    private readonly parentHooks: HookRegistry | null,
    private readonly metrics: MetricsCollector | null,
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

  track<T>(sql: string, op: () => Promise<T>, measure?: QueryOutcomeMeasure<T>): Promise<T> {
    if (!this.metrics) return op()
    return this.metrics.trackQuery(op, { databaseId: this.databaseId, sql }, measure)
  }

  async withTransactionHooks(
    statements: readonly ObservedStatement[],
    op: () => Promise<ExecuteResult[]>,
  ): Promise<ExecuteResult[]> {
    for (const statement of statements) this.fireBefore(statement.sql, statement.params)
    const start = performance.now()
    try {
      const results = await this.trackEach(statements, op)
      this.fireAfterEach(statements, performance.now() - start, STATEMENT_SUCCEEDED)
      return results
    } catch (error) {
      this.fireAfterEach(statements, performance.now() - start, { failed: true, error })
      throw error
    }
  }

  private fireAfterEach(statements: readonly ObservedStatement[], durationMs: number, outcome: StatementOutcome): void {
    for (const statement of statements) this.fireAfter(statement.sql, statement.params, durationMs, outcome)
  }

  private trackEach(
    statements: readonly ObservedStatement[],
    op: () => Promise<ExecuteResult[]>,
  ): Promise<ExecuteResult[]> {
    const metrics = this.metrics
    if (!metrics) return op()
    let run = op
    statements.forEach((statement, index) => {
      const inner = run
      run = () =>
        metrics.trackQuery(inner, { databaseId: this.databaseId, sql: statement.sql }, results => ({
          changes: results[index]?.changes,
        }))
    })
    return run()
  }

  private fireBefore(sql: string, params?: Params, options?: QueryOptions): void {
    fireBeforeQueryHooks(this.parentHooks, this.localHooks, this.databaseId, sql, params, options)
  }

  private fireAfter(sql: string, params: Params | undefined, durationMs: number, outcome: StatementOutcome): void {
    fireAfterQueryHooks(this.parentHooks, this.localHooks, this.databaseId, sql, params, durationMs, outcome)
  }
}
