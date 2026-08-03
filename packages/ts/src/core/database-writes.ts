import { runBulkLoad } from './bulk-load.js'
import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import type { ConnectionPool } from './connection-pool.js'
import type { DatabaseCdcController } from './database-cdc.js'
import type { DatabaseObserver } from './database-observability.js'
import type { SQLiteConnection, SynchronousLevel } from './driver/types.js'
import { canGroupTransaction, type GroupCommitter } from './group-committer.js'
import { executeBatch, executeBatchSummary } from './query-executor.js'
import type { Transaction } from './transaction.js'
import type { BulkLoadOptions, BulkLoadResult, ExecuteResult, Params, QueryOptions } from './types.js'
import type { WriteGate } from './worker/gate.js'
import type { WriterLock } from './writer-lock.js'

export interface DatabaseWriteDeps {
  pool: ConnectionPool
  writeGate: WriteGate
  writerLock: WriterLock
  groupCommitter: GroupCommitter
  cdc: DatabaseCdcController
  observer: DatabaseObserver
  synchronous: SynchronousLevel
  walMode: boolean
}

export class DatabaseWriteController {
  constructor(private readonly deps: DatabaseWriteDeps) {}

  execute(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    const { observer, writeGate, writerLock, groupCommitter } = this.deps
    return observer.withQueryHooks(sql, params, options, () =>
      writeGate.run(() =>
        observer.track(sql, () =>
          writerLock.isHeld() ? groupCommitter.runUngrouped(sql, params) : groupCommitter.submit(sql, params),
        ),
      ),
    )
  }

  executeBatch(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    const { observer, writeGate, writerLock, pool } = this.deps
    return observer.withQueryHooks(sql, undefined, options, () =>
      writeGate.run(() =>
        writerLock.run(() =>
          this.runInTransaction(pool.acquireWriter(), sql, txConn => executeBatch(txConn, sql, paramsBatch)),
        ),
      ),
    )
  }

  bulkLoad(sql: string, paramsBatch: Params[], options?: BulkLoadOptions): Promise<BulkLoadResult> {
    const { observer, writeGate, writerLock, pool, synchronous, walMode } = this.deps
    return observer.withQueryHooks(sql, undefined, undefined, () =>
      writeGate.run(() =>
        writerLock.run(() => {
          const writer = pool.acquireWriter()
          return runBulkLoad({
            writer,
            configuredSynchronous: synchronous,
            walMode,
            durability: options?.durability,
            checkpoint: options?.checkpoint ?? true,
            loadRows: () => this.runInTransaction(writer, sql, txConn => executeBatchSummary(txConn, sql, paramsBatch)),
          })
        }),
      ),
    )
  }

  executeTransaction(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    const { observer, writeGate, groupCommitter } = this.deps
    const owned = statements.map(statement => ({ sql: statement.sql, params: statement.params }))
    const run = canGroupTransaction(owned)
      ? () => writeGate.run(() => groupCommitter.submitTransaction(owned))
      : () => this.runStatementsAlone(owned)

    if (!observer.observesQueries) return run()
    return observer.withTransactionHooks(owned, run)
  }

  transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    const { writeGate, writerLock, pool, cdc } = this.deps
    return writeGate.run(() => writerLock.run(() => cdc.runTransaction(pool.acquireWriter(), fn)))
  }

  drain(): Promise<void> {
    return this.deps.groupCommitter.drain()
  }

  private runStatementsAlone(statements: readonly { sql: string; params?: Params }[]): Promise<ExecuteResult[]> {
    return this.transaction(async tx => {
      const results: ExecuteResult[] = new Array(statements.length)
      for (let i = 0; i < statements.length; i++) {
        results[i] = await tx.execute(statements[i].sql, statements[i].params)
      }
      return results
    })
  }

  private async runInTransaction<T>(
    writer: SQLiteConnection,
    sql: string,
    run: (txConn: SQLiteConnection) => Promise<T>,
  ): Promise<T> {
    const { observer, cdc } = this.deps
    const result = await observer.track(sql, () =>
      writer.transaction(async txConn => {
        const value = await run(txConn)
        await cdc.applyStamps(txConn)
        return value
      }),
    )
    await applyDdlSideEffectsIfRelevant(cdc.changeTracker, writer, sql)
    return result
  }
}
