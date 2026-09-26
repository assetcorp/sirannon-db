import { randomUUID } from 'node:crypto'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import { insertDdlChange } from '../../core/system-catalog/index.js'
import type { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params, QueryOptions, WriteConcern } from '../../core/types.js'
import { runWriterTransaction } from '../../core/writer-transaction.js'
import { ReplicationError } from '../errors.js'
import type { ForwardedTransactionResult } from '../types.js'
import { DDL_PREFIX_RE, extractDroppedTable, SAFE_SQL_PREFIX_RE } from './constants.js'
import { resolveWriteConcern, waitForWriteConcern } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { ReplicationTransaction, type ReplicationTransactionHooks } from './replication-transaction.js'
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

  async executeLocally(sql: string, params?: Params, options?: QueryOptions) {
    const engine = this.engine
    const isDdl = DDL_PREFIX_RE.test(sql)
    if (isDdl && sql.includes(';')) {
      throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
    }
    const txId = randomUUID()
    const droppedTable = isDdl ? extractDroppedTable(sql) : null

    const result = await runWriterTransaction(engine.writerConn, async tx => {
      const seqBefore = await engine.log.getLocalSeq()

      const bindValues = params ? (Array.isArray(params) ? params : [params]) : []
      const stmt = await tx.prepare(sql)
      const r = await stmt.run(...bindValues)

      if (isDdl) {
        await insertDdlChange(tx, CHANGES_TABLE, {
          ddlStatement: sql,
          nodeId: engine.nodeId,
          txId,
          hlc: engine.hlc.now(),
        })
      } else {
        await engine.log.stampChanges(tx, seqBefore, txId)
        await engine.log.updateColumnVersions(tx, seqBefore)
      }

      return { changes: r.changes, lastInsertRowId: r.lastInsertRowId }
    })

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

    return result
  }

  async executeBatchLocally(sql: string, paramsBatch: Params[], options?: QueryOptions): Promise<ExecuteResult[]> {
    const { results, newSeq } = await this.executeInOneTransaction(paramsBatch.map(params => ({ sql, params })))
    await this.waitForWriteConcernOf(newSeq, options?.writeConcern)
    return results
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

    const { results, newSeq } = await this.executeInOneTransaction(statements)
    await this.waitForWriteConcernOf(newSeq, statedWriteConcern)
    return {
      results: results.map(r => ({
        changes: r.changes,
        lastInsertRowId: typeof r.lastInsertRowId === 'bigint' ? r.lastInsertRowId.toString() : r.lastInsertRowId,
      })),
      requestId: randomUUID(),
    }
  }

  private async waitForWriteConcernOf(seq: bigint, stated: WriteConcern | undefined): Promise<void> {
    const writeConcern = resolveWriteConcern(this.engine, stated)
    if (writeConcern) {
      await waitForWriteConcern(this.engine, seq, writeConcern)
    }
  }

  private async executeInOneTransaction(
    statements: Array<{ sql: string; params?: Params }>,
  ): Promise<{ results: ExecuteResult[]; newSeq: bigint }> {
    const engine = this.engine
    const results: ExecuteResult[] = []
    const txId = randomUUID()
    let sawDdl = false
    const droppedTables: string[] = []

    await runWriterTransaction(engine.writerConn, async tx => {
      const seqBefore = await engine.log.getLocalSeq()

      for (const { sql, params } of statements) {
        const isDdl = DDL_PREFIX_RE.test(sql)
        if (isDdl && sql.includes(';')) {
          throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
        }

        const bindValues = params ? (Array.isArray(params) ? params : [params]) : []
        const stmt = await tx.prepare(sql)
        const r = await stmt.run(...bindValues)
        results.push({ changes: r.changes, lastInsertRowId: r.lastInsertRowId })

        if (isDdl) {
          sawDdl = true
          const droppedTable = extractDroppedTable(sql)
          if (droppedTable !== null) {
            droppedTables.push(droppedTable)
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

      const replicationTx = new ReplicationTransaction(tx, hooks)
      const result = await fn(replicationTx)

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
