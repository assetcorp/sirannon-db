import type { SQLiteConnection } from './driver/types.js'
import type { QueryFailureMeasure, QueryOutcomeMeasure } from './metrics/collector.js'
import { batchWriteOutcome, failedStatementOutcome, readOutcome, writeOutcome } from './metrics/outcomes.js'
import { execute, executeBatch, queryMetered, withoutRowCounts } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'
import { runWriterTransaction } from './writer-transaction.js'

/**
 * Receives each statement that a transaction executes, so that Sirannon can report its metrics.
 *
 * @internal
 */
export interface StatementMeter {
  /** Executes one statement through `op` and reports the outcome that `measure` or `measureFailure` returns. */
  track<T>(
    sql: string,
    op: () => Promise<T>,
    measure: QueryOutcomeMeasure<T>,
    measureFailure: QueryFailureMeasure,
  ): Promise<T>
}

/**
 * Executes statements inside one transaction; Sirannon passes it to the function that you give {@link Database.transaction}.
 *
 * @public
 */
export class Transaction {
  private _lastInsertRowId: number | bigint = 0
  private meter: StatementMeter | null = null

  constructor(private readonly conn: SQLiteConnection) {}

  /**
   * Executes a read inside this transaction.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @returns The rows that the statement returns.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
    const read = () => queryMetered<T>(this.conn, sql, params)
    const metered = this.meter ? await this.meter.track(sql, read, readOutcome, failedStatementOutcome) : await read()
    return metered.value
  }

  /**
   * Executes one write inside this transaction.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @returns The number of rows that the statement changed, and the row id of the last inserted row.
   */
  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    const write = () => execute(this.conn, sql, params)
    const result = this.meter ? await this.meter.track(sql, write, writeOutcome, failedStatementOutcome) : await write()
    this._lastInsertRowId = result.lastInsertRowId
    return withoutRowCounts(result)
  }

  /**
   * Executes one statement once for each parameter set, inside this transaction.
   *
   * @param sql - The statement to execute for each parameter set.
   * @param paramsBatch - One parameter set for each execution.
   * @returns One result per parameter set, in order.
   */
  async executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
    const write = () => executeBatch(this.conn, sql, paramsBatch)
    const results = this.meter
      ? await this.meter.track(sql, write, batchWriteOutcome, failedStatementOutcome)
      : await write()
    if (results.length > 0) {
      this._lastInsertRowId = results[results.length - 1].lastInsertRowId
    }
    return results.map(withoutRowCounts)
  }

  /**
   * The row id that SQLite assigned to the last row that this transaction inserted.
   */
  get lastInsertRowId(): number | bigint {
    return this._lastInsertRowId
  }

  /**
   * Reports every statement that this transaction executes to `meter`, and returns this transaction.
   *
   * @param meter - The receiver of each statement's metrics, or `null` to report nothing.
   * @returns This transaction.
   * @internal
   */
  withMeter(meter: StatementMeter | null): this {
    this.meter = meter
    return this
  }

  /** @internal */
  static async run<T>(
    conn: SQLiteConnection,
    fn: (tx: Transaction) => Promise<T>,
    meter: StatementMeter | null = null,
  ): Promise<T> {
    return runWriterTransaction(conn, async txConn => {
      const tx = new Transaction(txConn).withMeter(meter)
      return fn(tx)
    })
  }
}
