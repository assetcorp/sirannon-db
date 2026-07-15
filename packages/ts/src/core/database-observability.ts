import { fireAfterQueryHooks, fireBeforeQueryHooks } from './hooks/query-hooks.js'
import type { HookRegistry } from './hooks/registry.js'
import type { MetricsCollector } from './metrics/collector.js'
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

  /** False lets a caller skip the timing calls on a path where nothing would read them. */
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
      return await op()
    } finally {
      this.fireAfter(sql, params, performance.now() - start)
    }
  }

  track<T>(sql: string, op: () => Promise<T>): Promise<T> {
    if (!this.metrics) return op()
    return this.metrics.trackQuery(op, { databaseId: this.databaseId, sql })
  }

  /**
   * Reports each statement separately, since a hook filtering on SQL has no other
   * way to see them. They share the transaction's duration because the group runs
   * them back to back under one commit and no statement has a duration of its own,
   * which is already what a grouped {@link Database.execute} reports.
   */
  async withTransactionHooks(
    statements: readonly ObservedStatement[],
    op: () => Promise<ExecuteResult[]>,
  ): Promise<ExecuteResult[]> {
    for (const statement of statements) this.fireBefore(statement.sql, statement.params)
    const start = performance.now()
    try {
      return await this.trackEach(statements, op)
    } finally {
      const durationMs = performance.now() - start
      for (const statement of statements) this.fireAfter(statement.sql, statement.params, durationMs)
    }
  }

  private trackEach(
    statements: readonly ObservedStatement[],
    op: () => Promise<ExecuteResult[]>,
  ): Promise<ExecuteResult[]> {
    const metrics = this.metrics
    if (!metrics) return op()
    let run = op
    for (const statement of statements) {
      const inner = run
      const sql = statement.sql
      run = () => metrics.trackQuery(inner, { databaseId: this.databaseId, sql })
    }
    return run()
  }

  private fireBefore(sql: string, params?: Params, options?: QueryOptions): void {
    fireBeforeQueryHooks(this.parentHooks, this.localHooks, this.databaseId, sql, params, options)
  }

  private fireAfter(sql: string, params: Params | undefined, durationMs: number): void {
    fireAfterQueryHooks(this.parentHooks, this.localHooks, this.databaseId, sql, params, durationMs)
  }
}
