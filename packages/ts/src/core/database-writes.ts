import { runBulkLoad } from './bulk-load.js'
import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseCdcController } from './database-cdc.js'
import type { DatabaseObserver, TransactionOutcomes } from './database-observability.js'
import { type RowCounts, rowCountsOf } from './driver/row-counts.js'
import type { SQLiteConnection, SynchronousLevel } from './driver/types.js'
import { canGroupTransaction, type FailedWriteRowCounts, type GroupCommitter } from './group-committer.js'
import type { QueryFailureMeasure, QueryOutcome, QueryOutcomeMeasure } from './metrics/collector.js'
import { batchWriteOutcome, failedStatementOutcome, ROW_COUNTS_UNKNOWN, writeOutcome } from './metrics/outcomes.js'
import { executeBatch, executeBatchSummary, withoutRowCounts } from './query-executor.js'
import type { StatementMeter, Transaction } from './transaction.js'
import type { BulkLoadOptions, BulkLoadResult, ExecuteResult, Params, QueryOptions } from './types.js'
import type { WriteGate } from './worker/gate.js'
import type { WriterLock } from './writer-lock.js'
import { runWriterTransaction } from './writer-transaction.js'

export interface DatabaseWriteDeps {
  pool: ConnectionPool
  writeGate: WriteGate
  writerLock: WriterLock
  groupCommitter: GroupCommitter
  cdc: DatabaseCdcController
  observer: DatabaseObserver
  synchronous: SynchronousLevel
  walMode: boolean
  capturesChangeLog: boolean
}

function rowCountsOnly(outcome: QueryOutcome): QueryOutcome {
  if (outcome === ROW_COUNTS_UNKNOWN) return outcome
  return outcome.rowsRead === undefined ? {} : { rowsRead: outcome.rowsRead, rowsWritten: outcome.rowsWritten }
}

function groupedOutcomes(failed: FailedWriteRowCounts): TransactionOutcomes {
  return {
    succeeded: results => results.map(writeOutcome),
    failed: () => failed.statements?.map((counts: RowCounts) => ({ ...counts })) ?? [],
  }
}

class StatementRecorder implements StatementMeter {
  readonly outcomes: QueryOutcome[] = []

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

  transactionOutcomes(): TransactionOutcomes {
    return { succeeded: () => this.outcomes, failed: () => this.outcomes.map(rowCountsOnly) }
  }
}

export class DatabaseWriteController {
  constructor(private readonly deps: DatabaseWriteDeps) {}

  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    const { observer, writeGate, writerLock, groupCommitter } = this.deps
    const failed: FailedWriteRowCounts = {}
    return observer
      .withQueryHooks(sql, params, options, () =>
        writeGate.run(() =>
          observer.track(
            sql,
            () =>
              writerLock.isHeld()
                ? groupCommitter.runUngrouped(sql, params)
                : groupCommitter.submit(sql, params, failed),
            writeOutcome,
            error => failed.statements?.[0] ?? failedStatementOutcome(error),
          ),
        ),
      )
      .then(withoutRowCounts)
  }

  executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    const { observer, writeGate, writerLock, pool } = this.deps
    return observer
      .withQueryHooks(sql, undefined, options, () =>
        writeGate.run(() =>
          writerLock.run(() =>
            this.runInTransaction(
              pool.acquireWriter(),
              sql,
              txConn => executeBatch(txConn, sql, paramsBatch),
              batchWriteOutcome,
            ),
          ),
        ),
      )
      .then(results => results.map(withoutRowCounts))
  }

  bulkLoad(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult> {
    const { observer, writeGate, writerLock, pool, synchronous, walMode, capturesChangeLog } = this.deps
    return observer.withQueryHooks(sql, undefined, undefined, () =>
      writeGate.run(() =>
        writerLock.run(async () => {
          const writer = pool.acquireWriter()
          const summary = await runBulkLoad({
            writer,
            configuredSynchronous: synchronous,
            walMode,
            durability: options?.durability,
            checkpoint: options?.checkpoint ?? true,
            capturesChangeLog,
            loadRows: () =>
              this.runInTransaction(writer, sql, txConn => executeBatchSummary(txConn, sql, paramsBatch), writeOutcome),
          })
          return { rowsLoaded: summary.rowsLoaded, changes: summary.changes }
        }),
      ),
    )
  }

  executeTransaction(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    const { observer, writeGate, groupCommitter } = this.deps
    const owned = statements.map(statement => ({ sql: statement.sql, params: statement.params }))
    if (canGroupTransaction(owned)) {
      const failed: FailedWriteRowCounts = {}
      const run = () => writeGate.run(() => groupCommitter.submitTransaction(owned, failed))
      const results = observer.observesQueries
        ? observer.withTransactionHooks(owned, run, groupedOutcomes(failed))
        : run()
      return results.then(written => written.map(withoutRowCounts))
    }

    const recorder = new StatementRecorder()
    const run = () => this.runStatementsAlone(owned, recorder)
    if (!observer.observesQueries) return run()
    return observer.withTransactionHooks(owned, run, recorder.transactionOutcomes())
  }

  transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.runTransaction(fn, this.deps.observer.meter)
  }

  drain(): Promise<void> {
    return this.deps.groupCommitter.drain()
  }

  private runTransaction<T>(fn: (tx: Transaction) => Promise<T>, meter: StatementMeter | null): Promise<T> {
    const { writeGate, writerLock, pool, cdc } = this.deps
    return writeGate.run(() => writerLock.run(() => cdc.runTransaction(pool.acquireWriter(), fn, meter)))
  }

  private runStatementsAlone(
    statements: readonly { sql: string; params?: Params }[],
    recorder: StatementRecorder,
  ): Promise<ExecuteResult[]> {
    return this.runTransaction(async tx => {
      const results: ExecuteResult[] = new Array(statements.length)
      for (let i = 0; i < statements.length; i++) {
        results[i] = await tx.execute(statements[i].sql, statements[i].params)
      }
      return results
    }, recorder)
  }

  private async runInTransaction<T>(
    writer: SQLiteConnection,
    sql: string,
    run: (txConn: SQLiteConnection) => Promise<T>,
    measure: QueryOutcomeMeasure<T>,
  ): Promise<T> {
    const { observer, cdc } = this.deps
    let countsBeforeCommit: RowCounts | undefined
    const result = await observer.track(
      sql,
      () =>
        runWriterTransaction(writer, async txConn => {
          const value = await run(txConn)
          countsBeforeCommit = rowCountsOf(measure(value))
          await cdc.applyStamps(txConn)
          return value
        }),
      measure,
      error => countsBeforeCommit ?? failedStatementOutcome(error),
    )
    await applyDdlSideEffectsIfRelevant(cdc.changeTracker, writer, sql)
    return result
  }
}
