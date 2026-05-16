import { randomUUID } from 'node:crypto'
import type { Params, QueryOptions } from '../../core/types.js'
import { ReplicationError } from '../errors.js'
import type { ForwardedTransactionResult } from '../types.js'
import { DDL_PREFIX_RE, SAFE_SQL_PREFIX_RE } from './constants.js'
import type { ReplicationEngine } from './engine.js'

export class LocalExecutor {
  constructor(private readonly engine: ReplicationEngine) {}

  async executeLocally(sql: string, params?: Params, options?: QueryOptions) {
    const engine = this.engine
    const isDdl = DDL_PREFIX_RE.test(sql)
    if (isDdl && sql.includes(';')) {
      throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
    }
    const txId = randomUUID()

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
          const ddlStmt = await tx.prepare(
            `INSERT INTO "_sirannon_changes" (table_name, operation, row_id, new_data, node_id, tx_id, hlc)
             VALUES ('__ddl__', 'DDL', '', ?, ?, ?, ?)`,
          )
          const hlcVal = engine.hlc.now()
          await ddlStmt.run(JSON.stringify({ ddlStatement: sql }), engine.nodeId, txId, hlcVal)
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
      await engine.refreshTriggersAfterDdl()
    }

    return { results, requestId }
  }
}
