import { runBulkLoad } from './bulk-load.js'
import { executeAllowingColumnDrop } from './cdc/column-drop.js'
import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseCdcController } from './database-cdc.js'
import type { DatabaseObserver, TransactionOutcomes } from './database-observability.js'
import { type RowCounts, rowCountsOf } from './driver/row-counts.js'
import type { SQLiteConnection, SynchronousLevel } from './driver/types.js'
import { SirannonError } from './errors.js'
import { canGroupTransaction, type FailedWriteRowCounts, type GroupCommitter } from './group-committer.js'
import type { QueryOutcomeMeasure } from './metrics/collector.js'
import { withoutLateRowCounts } from './metrics/late-row-counts.js'
import { batchWriteOutcome, failedStatementOutcome, writeOutcome } from './metrics/outcomes.js'
import { StatementRecorder } from './metrics/statement-recorder.js'
import { executeBatch, executeBatchSummary, withoutRowCounts } from './query-executor.js'
import type { StatementMeter, Transaction } from './transaction.js'
import type { BulkLoadOptions, BulkLoadResult, ExecuteResult, Params, QueryOptions } from './types.js'
import type { WriteGate } from './worker/gate.js'
import type { WriterLock } from './writer-lock.js'
import { runInSavepoint, runWriterTransaction, type WriterTransactionRunner } from './writer-transaction.js'

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

function bulkLoadInsideTransaction(): SirannonError {
  return new SirannonError(
    'bulkLoad commits in a transaction of its own at a relaxed synchronous level, so a call inside another transaction fails; call executeBatch on that transaction to write the rows',
    'BULK_LOAD_IN_TRANSACTION',
  )
}

function rethrowWithoutLateRowCounts(error: unknown): never {
  throw withoutLateRowCounts(error)
}

function groupedOutcomes(failed: FailedWriteRowCounts): TransactionOutcomes {
  return {
    succeeded: results => results.map(writeOutcome),
    failed: () => failed.statements?.map((counts: RowCounts) => ({ ...counts })) ?? [],
    failedLate: counts => counts.map(statement => (statement === undefined ? {} : { ...statement })),
  }
}

export class DatabaseWriteController {
  constructor(private readonly deps: DatabaseWriteDeps) {}

  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    const { observer, writeGate, writerLock, groupCommitter } = this.deps
    const failed: FailedWriteRowCounts = {}
    return observer
      .withQueryHooks(
        sql,
        params,
        options,
        () =>
          this.refusedCaller() ??
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
    const runInTransaction = this.transactionRunnerForCaller()
    return observer
      .withQueryHooks(
        sql,
        undefined,
        options,
        () =>
          this.refusedCaller() ??
          writeGate.run(() =>
            writerLock.run(() =>
              this.runInTransaction(
                pool.acquireWriter(),
                sql,
                txConn =>
                  executeAllowingColumnDrop(this.deps.cdc.changeTracker, txConn, sql, () =>
                    executeBatch(txConn, sql, paramsBatch),
                  ),
                batchWriteOutcome,
                runInTransaction,
              ),
            ),
          ),
      )
      .then(results => results.map(withoutRowCounts))
  }

  bulkLoad(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult> {
    const { observer, writeGate, writerLock, pool, synchronous, walMode, capturesChangeLog } = this.deps
    return observer.withQueryHooks(sql, undefined, undefined, () => {
      if (writerLock.isHeld()) return Promise.reject(bulkLoadInsideTransaction())
      const refused = this.refusedCaller()
      if (refused !== null) return refused
      return writeGate.run(() =>
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
              this.runInTransaction(
                writer,
                sql,
                txConn =>
                  executeAllowingColumnDrop(this.deps.cdc.changeTracker, txConn, sql, () =>
                    executeBatchSummary(txConn, sql, paramsBatch),
                  ),
                writeOutcome,
              ),
            restoreBeforeNextWrite: restore => writerLock.repairBeforeNextHold(restore),
          })
          return { rowsLoaded: summary.rowsLoaded, changes: summary.changes }
        }),
      )
    })
  }

  executeTransaction(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    const { observer, writeGate, writerLock, groupCommitter } = this.deps
    const refused = this.refusedCaller()
    if (refused !== null) return refused
    const owned = statements.map(statement => ({ sql: statement.sql, params: statement.params }))
    if (canGroupTransaction(owned) && !writerLock.isHeld()) {
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
    return this.refusedCaller() ?? this.runTransaction(fn, this.deps.observer.meter, true)
  }

  drain(): Promise<void> {
    return this.deps.groupCommitter.drain()
  }

  private refusedCaller(): Promise<never> | null {
    const refusal = this.deps.writerLock.callerRefusal()
    return refusal === null ? null : Promise.reject(refusal)
  }

  private runTransaction<T>(
    fn: (tx: Transaction) => Promise<T>,
    meter: StatementMeter | null,
    callsApplication = false,
  ): Promise<T> {
    const { writeGate, writerLock, pool, cdc } = this.deps
    const runInTransaction = this.transactionRunnerForCaller()
    const hold = () => cdc.runTransaction(pool.acquireWriter(), fn, meter, runInTransaction)
    return writeGate.run(() => (callsApplication ? writerLock.runCallback(hold) : writerLock.run(hold)))
  }

  private transactionRunnerForCaller(): WriterTransactionRunner {
    return this.deps.writerLock.isHeld() ? runInSavepoint : runWriterTransaction
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
    runInTransaction: WriterTransactionRunner = runWriterTransaction,
  ): Promise<T> {
    const { observer, cdc } = this.deps
    let countsBeforeCommit: RowCounts | undefined
    const result = await observer.track(
      sql,
      () =>
        runInTransaction(writer, async txConn => {
          const opening = cdc.openStamps(txConn, runInTransaction)
          if (opening !== null) await opening.catch(rethrowWithoutLateRowCounts)
          const value = await run(txConn)
          countsBeforeCommit = rowCountsOf(measure(value))
          const stamping = cdc.applyStamps(txConn, runInTransaction)
          if (stamping !== null) await stamping.catch(rethrowWithoutLateRowCounts)
          return value
        }),
      measure,
      error => countsBeforeCommit ?? failedStatementOutcome(error),
    )
    await applyDdlSideEffectsIfRelevant(cdc.changeTracker, writer, sql)
    return result
  }
}
