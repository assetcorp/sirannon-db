import { randomUUID } from 'node:crypto'
import type { Transaction } from '../../core/transaction.js'
import type { Params, QueryOptions } from '../../core/types.js'
import { ReplicationError } from '../errors.js'
import type { ForwardedTransactionResult } from '../types.js'
import { DDL_PREFIX_RE, extractDroppedTable, SAFE_SQL_PREFIX_RE } from './constants.js'
import type { ReplicationEngine } from './engine.js'
import { ReplicationTransaction, type ReplicationTransactionHooks } from './replication-transaction.js'

export class LocalExecutor {
  /**
   * Serialises every `executeTransactionLocally` call against itself.
   *
   * Reason: SQLite only allows one active transaction per connection; the
   * writer pool exposes a single writer connection (see `ConnectionPool`).
   * Two parallel `engine.transaction(fn)` callers therefore both reach
   * `await conn.exec('BEGIN')` on the same connection, and the second
   * `BEGIN` errors with "cannot start a transaction within a transaction".
   * Chaining onto this promise turns the second caller into a strict
   * follow-on without changing the public API. A rejection is swallowed at
   * the chain level so a failed transaction never poisons the queue; the
   * original error still surfaces to the caller that initiated it.
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

    const result = await engine.writerConn.transaction(async tx => {
      const seqBefore = await engine.log.getLocalSeq()

      const bindValues = params ? (Array.isArray(params) ? params : [params]) : []
      const stmt = await tx.prepare(sql)
      const r = await stmt.run(...bindValues)

      if (isDdl) {
        const ddlStmt = await tx.prepare(
          `INSERT INTO "_sirannon_changes" (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
           VALUES ('__ddl__', 'DDL', '', ?, ?, ?, ?)`,
        )
        const hlcVal = engine.hlc.now()
        await ddlStmt.run(JSON.stringify({ ddlStatement: sql }), engine.nodeId, txId, hlcVal)
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
      await engine.refreshTriggersAfterDdl()
    }

    if (options?.writeConcern) {
      await engine.waitForWriteConcern(newSeq, options.writeConcern)
    }

    return result
  }

  async executeForwardedLocally(
    statements: Array<{ sql: string; params?: Params }>,
  ): Promise<ForwardedTransactionResult> {
    const engine = this.engine
    const requestId = randomUUID()
    const results: Array<{ changes: number; lastInsertRowId: number | string }> = []
    const txId = randomUUID()
    const hook = engine.config.onBeforeForwardedQuery

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

    let sawDdl = false
    const droppedTables: string[] = []

    await engine.writerConn.transaction(async tx => {
      const seqBefore = await engine.log.getLocalSeq()

      for (const { sql, params } of statements) {
        const isDdl = DDL_PREFIX_RE.test(sql)
        if (isDdl && sql.includes(';')) {
          throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
        }

        const bindValues = params ? (Array.isArray(params) ? params : [params]) : []
        const stmt = await tx.prepare(sql)
        const r = await stmt.run(...bindValues)
        results.push({
          changes: r.changes,
          lastInsertRowId: typeof r.lastInsertRowId === 'bigint' ? r.lastInsertRowId.toString() : r.lastInsertRowId,
        })

        if (isDdl) {
          sawDdl = true
          const droppedTable = extractDroppedTable(sql)
          if (droppedTable !== null) {
            droppedTables.push(droppedTable)
          }
          const ddlStmt = await tx.prepare(
            `INSERT INTO "_sirannon_changes" (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
             VALUES ('__ddl__', 'DDL', '', ?, ?, ?, ?)`,
          )
          const hlcVal = engine.hlc.now()
          await ddlStmt.run(JSON.stringify({ ddlStatement: sql }), engine.nodeId, txId, hlcVal)
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
      await engine.refreshTriggersAfterDdl()
    }

    return { results, requestId }
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

    const userResult = await engine.writerConn.transaction(async tx => {
      const seqBefore = await engine.log.getLocalSeq()

      hooks.onDdl = async (sql: string) => {
        const droppedTable = extractDroppedTable(sql)
        if (droppedTable !== null) {
          hooks.droppedTables.push(droppedTable)
        }
        const ddlStmt = await tx.prepare(
          `INSERT INTO "_sirannon_changes" (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
           VALUES ('__ddl__', 'DDL', '', ?, ?, ?, ?)`,
        )
        const hlcVal = engine.hlc.now()
        await ddlStmt.run(JSON.stringify({ ddlStatement: sql }), engine.nodeId, txId, hlcVal)
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
      await engine.refreshTriggersAfterDdl()
    }

    if (options?.writeConcern) {
      await engine.waitForWriteConcern(newSeq, options.writeConcern)
    }

    return userResult
  }
}
