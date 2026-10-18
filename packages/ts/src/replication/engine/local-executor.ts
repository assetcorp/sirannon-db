import { randomUUID } from 'node:crypto'
import { type RowCounts, rowCountsOf } from '../../core/driver/row-counts.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import { batchWriteOutcome, failedStatementOutcome, writeOutcome } from '../../core/metrics/outcomes.js'
import { StatementRecorder } from '../../core/metrics/statement-recorder.js'
import { execute, withoutRowCounts } from '../../core/query-executor.js'
import { insertDdlChange } from '../../core/system-catalog/index.js'
import type { StatementMeter, Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions, WriteConcern } from '../../core/types.js'
import { runWriterTransaction } from '../../core/writer-transaction.js'
import { ReplicationError } from '../errors.js'
import type { ForwardedTransactionResult } from '../types.js'
import { extractDroppedTable, SAFE_SQL_PREFIX_RE } from './constants.js'
import { resolveWriteConcern, waitForWriteConcern } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { ReplicationTransaction, type ReplicationTransactionHooks } from './replication-transaction.js'
import { replicatedSchemaChangeOf } from './schema-statement.js'
import { refreshTriggersAfterDdl } from './trigger-refresh.js'

/**
 * Executes a write on this node and stamps the changes that it makes into the replication log inside the same SQLite
 * transaction.
 *
 * @internal
 */
export class LocalExecutor {
  /**
   * Makes each `executeTransactionLocally` call wait until the previous one finishes.
   *
   * The engine writes through the single connection `engine.writerConn`,
   * which SQLite limits to one open transaction. Without this queue, two
   * concurrent `engine.transaction(fn)` calls would both send `BEGIN` on that
   * connection, so SQLite would reject the second with 'cannot start a
   * transaction within a transaction'. The queue catches each rejection so
   * that a later transaction still starts after an earlier one fails, while
   * the caller that started the failed transaction still receives its error.
   */
  private transactionQueue: Promise<unknown> = Promise.resolve()

  constructor(private readonly engine: ReplicationEngine) {}

  executeLocally(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    return this.engine.database.observer.withQueryHooks(sql, params, options, () =>
      this.executeOneLocally(sql, params, options),
    )
  }

  private async executeOneLocally(sql: string, params?: Params, options?: QueryOptions): Promise<ExecuteResult> {
    const engine = this.engine
    const schemaChange = replicatedSchemaChangeOf(sql)
    const isDdl = schemaChange !== null
    const txId = randomUUID()
    const droppedTable = isDdl ? extractDroppedTable(schemaChange) : null

    let countsBeforeCommit: RowCounts | undefined
    const result = await engine.database.observer.track(
      sql,
      () =>
        runWriterTransaction(engine.writerConn, async tx => {
          const seqBefore = await engine.log.getLocalSeq()

          const r = await execute(tx, sql, params)
          countsBeforeCommit = rowCountsOf(r)

          if (isDdl) {
            await insertDdlChange(tx, CHANGES_TABLE, {
              ddlStatement: schemaChange,
              nodeId: engine.nodeId,
              txId,
              hlc: engine.hlc.now(),
            })
          } else {
            await engine.log.stampChanges(tx, seqBefore, txId)
            await engine.log.updateColumnVersions(tx, seqBefore)
          }

          return r
        }),
      writeOutcome,
      error => countsBeforeCommit ?? failedStatementOutcome(error),
    )

    const newSeq = await engine.log.getLocalSeq()
    if (newSeq > engine.lastLocalSeq) {
      engine.lastLocalSeq = newSeq
    }

    if (isDdl) {
      if (droppedTable !== null && engine.tracker) {
        await engine.tracker.pruneDroppedTables(engine.writerConn, [droppedTable])
      }
      await refreshTriggersAfterDdl(engine)
    }

    const writeConcern = resolveWriteConcern(engine, options?.writeConcern)
    if (writeConcern) {
      await waitForWriteConcern(engine, newSeq, writeConcern)
    }

    return withoutRowCounts(result)
  }

  executeBatchLocally(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    const observer = this.engine.database.observer
    return observer.withQueryHooks(sql, undefined, options, async () => {
      const recorder = new StatementRecorder()
      const { results, newSeq } = await observer.track(
        sql,
        () =>
          this.executeInOneTransaction(
            paramsBatch.map(params => ({ sql, params })),
            recorder,
          ),
        written => batchWriteOutcome(written.results),
        error => recorder.failedTotal(error, failedStatementOutcome),
      )
      await this.waitForWriteConcernOf(newSeq, options?.writeConcern)
      return results.map(withoutRowCounts)
    })
  }

  async executeForwardedLocally(
    statements: Array<{ sql: string; params?: Params }>,
    statedWriteConcern?: WriteConcern,
  ): Promise<ForwardedTransactionResult> {
    const hook = this.engine.config.onBeforeForwardedQuery

    for (const { sql } of statements) {
      if (!SAFE_SQL_PREFIX_RE.test(sql)) {
        throw new ReplicationError('Forwarded statement rejected: only DML and safe DDL are allowed')
      }
    }

    if (hook) {
      for (const { sql, params } of statements) {
        hook(sql, params)
      }
    }

    const results = await this.executeStatementsLocally(statements, statedWriteConcern)
    return {
      results: results.map(r => ({
        changes: r.changes,
        lastInsertRowId: typeof r.lastInsertRowId === 'bigint' ? r.lastInsertRowId.toString() : r.lastInsertRowId,
      })),
      requestId: randomUUID(),
    }
  }

  async executeStatementsLocally(
    statements: Array<{ sql: string; params?: Params }>,
    statedWriteConcern?: WriteConcern,
  ): Promise<ExecuteResult[]> {
    const recorder = new StatementRecorder()
    let newSeq = 0n
    const results = await this.engine.database.observer.withTransactionHooks(
      statements,
      async () => {
        const written = await this.executeInOneTransaction(statements, recorder)
        newSeq = written.newSeq
        return written.results
      },
      recorder.transactionOutcomes(),
    )
    await this.waitForWriteConcernOf(newSeq, statedWriteConcern)
    return results.map(withoutRowCounts)
  }

  private async waitForWriteConcernOf(seq: bigint, stated: WriteConcern | undefined): Promise<void> {
    const writeConcern = resolveWriteConcern(this.engine, stated)
    if (writeConcern) {
      await waitForWriteConcern(this.engine, seq, writeConcern)
    }
  }

  private async executeInOneTransaction(
    statements: Array<{ sql: string; params?: Params }>,
    meter: StatementMeter,
  ): Promise<{ results: ExecuteResult[]; newSeq: bigint }> {
    const engine = this.engine
    const results: ExecuteResult[] = []
    const txId = randomUUID()
    let sawDdl = false
    const droppedTables: string[] = []

    await runWriterTransaction(engine.writerConn, async tx => {
      const seqBefore = await engine.log.getLocalSeq()

      for (const { sql, params } of statements) {
        const schemaChange = replicatedSchemaChangeOf(sql)

        results.push(await meter.track(sql, () => execute(tx, sql, params), writeOutcome, failedStatementOutcome))

        if (schemaChange !== null) {
          sawDdl = true
          const droppedTable = extractDroppedTable(schemaChange)
          if (droppedTable !== null) {
            droppedTables.push(droppedTable)
          }
          await insertDdlChange(tx, CHANGES_TABLE, {
            ddlStatement: schemaChange,
            nodeId: engine.nodeId,
            txId,
            hlc: engine.hlc.now(),
          })
          if (engine.tracker) {
            await engine.tracker.refreshAllTriggersUsingConnection(tx)
          }
        }
      }

      await engine.log.stampChanges(tx, seqBefore, txId)
      await engine.log.updateColumnVersions(tx, seqBefore)
    })

    const newSeq = await engine.log.getLocalSeq()
    if (newSeq > engine.lastLocalSeq) {
      engine.lastLocalSeq = newSeq
    }

    if (sawDdl) {
      if (droppedTables.length > 0 && engine.tracker) {
        await engine.tracker.pruneDroppedTables(engine.writerConn, droppedTables)
      }
      await refreshTriggersAfterDdl(engine)
    }

    return { results, newSeq }
  }

  async executeTransactionLocally<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T> {
    const ticket = this.transactionQueue.then(
      () => this.runTransaction(fn, options),
      () => this.runTransaction(fn, options),
    )
    this.transactionQueue = ticket.catch(() => undefined)
    return ticket
  }

  private async runTransaction<T>(fn: (tx: Transaction) => Promise<T>, options?: QueryOptions): Promise<T> {
    const engine = this.engine
    const txId = randomUUID()

    const hooks: ReplicationTransactionHooks = {
      sawDdl: false,
      droppedTables: [],
      onDdl: () => {
        throw new ReplicationError('Internal error: DDL hook invoked outside an active transaction')
      },
    }

    const userResult = await runWriterTransaction(engine.writerConn, async tx => {
      const seqBefore = await engine.log.getLocalSeq()

      hooks.onDdl = async (sql: string) => {
        const droppedTable = extractDroppedTable(sql)
        if (droppedTable !== null) {
          hooks.droppedTables.push(droppedTable)
        }
        await insertDdlChange(tx, CHANGES_TABLE, {
          ddlStatement: sql,
          nodeId: engine.nodeId,
          txId,
          hlc: engine.hlc.now(),
        })
        if (engine.tracker) {
          await engine.tracker.refreshAllTriggersUsingConnection(tx)
        }
      }

      const replicationTx = new ReplicationTransaction(tx, hooks).withMeter(engine.database.observer.meter)
      let result: T
      try {
        result = await fn(replicationTx)
      } finally {
        replicationTx.end()
      }

      await engine.log.stampChanges(tx, seqBefore, txId)
      await engine.log.updateColumnVersions(tx, seqBefore)

      return result
    })

    const newSeq = await engine.log.getLocalSeq()
    if (newSeq > engine.lastLocalSeq) {
      engine.lastLocalSeq = newSeq
    }

    if (hooks.sawDdl) {
      if (hooks.droppedTables.length > 0 && engine.tracker) {
        await engine.tracker.pruneDroppedTables(engine.writerConn, hooks.droppedTables)
      }
      await refreshTriggersAfterDdl(engine)
    }

    const writeConcern = resolveWriteConcern(engine, options?.writeConcern)
    if (writeConcern) {
      await waitForWriteConcern(engine, newSeq, writeConcern)
    }

    return userResult
  }
}
