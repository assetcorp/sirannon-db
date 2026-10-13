import type { SQLiteConnection } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { QueryFailureMeasure, QueryOutcomeMeasure } from './metrics/collector.js'
import { batchWriteOutcome, failedStatementOutcome, readOutcome, writeOutcome } from './metrics/outcomes.js'
import { execute, executeBatch, queryMetered, withoutRowCounts } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'
import { runWriterTransaction, type WriterTransactionRunner } from './writer-transaction.js'

/**
 * Receives each statement that a transaction executes, so that Sirannon can fire its query hooks and report its metrics.
 *
 * @internal
 */
export interface StatementMeter {
  /** Executes one statement through `op`, with `params` as its bound values, and reports the outcome that `measure` or `measureFailure` returns. */
  track<T>(
    sql: string,
    op: () => Promise<T>,
    measure: QueryOutcomeMeasure<T>,
    measureFailure: QueryFailureMeasure,
    params?: Params,
  ): Promise<T>
}

function transactionClosed(): SirannonError {
  return new SirannonError(
    'This transaction has ended, so it executes no further statements; run them in a new transaction',
    'TRANSACTION_CLOSED',
  )
}

/**
 * Executes statements inside one transaction; Sirannon passes it to the function that you give {@link Database.transaction}.
 *
 * Once that function returns or throws, the transaction refuses every further call with `TRANSACTION_CLOSED`.
 *
 * @public
 */
export class Transaction {
  private _lastInsertRowId: number | bigint = 0
  private meter: StatementMeter | null = null
  private ended = false

  constructor(private readonly conn: SQLiteConnection) {}

  /**
   * Executes a read inside this transaction.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @returns The rows that the statement returns.
   * @throws A `SirannonError` with code `TRANSACTION_CLOSED` once the transaction has ended.
   */
  async query<T = Record<string, unknown>>(sql: string, params?: Params): Promise<T[]> {
    if (this.ended) throw transactionClosed()
    const read = () => queryMetered<T>(this.conn, sql, params)
    const metered = this.meter
      ? await this.meter.track(sql, read, readOutcome, failedStatementOutcome, params)
      : await read()
    return metered.value
  }

  /**
   * Executes one write inside this transaction.
   *
   * @param sql - The statement to execute.
   * @param params - The values to bind to the statement, named or positional.
   * @returns The number of rows that the statement changed, and the row id of the last inserted row.
   * @throws A `SirannonError` with code `TRANSACTION_CLOSED` once the transaction has ended.
   */
  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    if (this.ended) throw transactionClosed()
    const write = () => execute(this.conn, sql, params)
    const result = this.meter
      ? await this.meter.track(sql, write, writeOutcome, failedStatementOutcome, params)
      : await write()
    this._lastInsertRowId = result.lastInsertRowId
    return withoutRowCounts(result)
  }

  /**
   * Executes one statement once for each parameter set, inside this transaction.
   *
   * @param sql - The statement to execute for each parameter set.
   * @param paramsBatch - One parameter set for each execution.
   * @returns One result per parameter set, in order.
   * @throws A `SirannonError` with code `TRANSACTION_CLOSED` once the transaction has ended.
   */
  async executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
    if (this.ended) throw transactionClosed()
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
   * @param meter - The receiver of each statement, or `null` to report nothing.
   * @returns This transaction.
   * @internal
   */
  withMeter(meter: StatementMeter | null): this {
    this.meter = meter
    return this
  }

  /**
   * Ends this transaction, so that a handle that the caller's function leaks refuses every later call with `TRANSACTION_CLOSED`.
   *
   * @internal
   */
  end(): void {
    this.ended = true
  }

  /** @internal */
  static async run<T>(
    conn: SQLiteConnection,
    fn: (tx: Transaction) => Promise<T>,
    meter: StatementMeter | null = null,
    runInTransaction: WriterTransactionRunner = runWriterTransaction,
  ): Promise<T> {
    return runInTransaction(conn, async txConn => {
      const tx = new Transaction(txConn).withMeter(meter)
      try {
        return await fn(tx)
      } finally {
        tx.end()
      }
    })
  }
}
