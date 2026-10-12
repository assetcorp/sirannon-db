import type { TransactionOutcomes } from '../database-observability.js'
import { addRowCounts, type RowCounts, rowCountsOf } from '../driver/row-counts.js'
import type { StatementMeter } from '../transaction.js'
import type { QueryFailureMeasure, QueryOutcome, QueryOutcomeMeasure } from './collector.js'
import { ROW_COUNTS_UNKNOWN } from './outcomes.js'

function rowCountsOnly(outcome: QueryOutcome): QueryOutcome {
  if (outcome === ROW_COUNTS_UNKNOWN) return outcome
  return outcome.rowsRead === undefined ? {} : { rowsRead: outcome.rowsRead, rowsWritten: outcome.rowsWritten }
}

/**
 * Records the outcome of each statement that one transaction executes, so that Sirannon can report every statement once the transaction commits or rolls back.
 *
 * @internal
 */
export class StatementRecorder implements StatementMeter {
  /** The outcome of each statement that the transaction executed, in order. */
  readonly outcomes: QueryOutcome[] = []

  /**
   * Executes one statement through `op` and records the outcome that `measure` or `measureFailure` returns.
   *
   * @param _sql - The statement, which the recorder keeps no copy of.
   * @param op - The call that executes the statement.
   * @param measure - Returns the outcome of a statement that succeeds.
   * @param measureFailure - Returns the outcome of a statement that throws.
   * @returns The value that `op` resolves to.
   */
  async track<T>(
    _sql: string,
    op: () => Promise<T>,
    measure: QueryOutcomeMeasure<T>,
    measureFailure: QueryFailureMeasure,
  ): Promise<T> {
    try {
      const result = await op()
      this.outcomes.push(measure(result))
      return result
    } catch (err) {
      this.outcomes.push(measureFailure(err))
      throw err
    }
  }

  /**
   * Returns the outcomes to report for each statement, keeping only the row counts when the transaction fails.
   *
   * @returns The outcomes for a transaction that commits and for one that fails.
   */
  transactionOutcomes(): TransactionOutcomes {
    return { succeeded: () => this.outcomes, failed: () => this.outcomes.map(rowCountsOnly) }
  }

  /**
   * Returns the summed row counts of every recorded statement, for a statement that executed once for each parameter set and failed.
   *
   * @param error - The error that the transaction raised, whose own counts Sirannon reports when the recorder holds no statement.
   * @param measureFailure - Returns the outcome of an error that no recorded statement raised.
   * @returns The total counts, or the unknown outcome when any statement's outcome is unknown.
   */
  failedTotal(error: unknown, measureFailure: QueryFailureMeasure): QueryOutcome {
    if (this.outcomes.length === 0) return measureFailure(error)
    let total: RowCounts | undefined
    for (const outcome of this.outcomes) {
      if (outcome === ROW_COUNTS_UNKNOWN) return ROW_COUNTS_UNKNOWN
      total = addRowCounts(total, rowCountsOf(outcome))
    }
    return total ?? {}
  }
}
