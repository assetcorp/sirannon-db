import type { SQLiteConnection } from '../../core/driver/types.js'
import { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params } from '../../core/types.js'
import { ReplicationError } from '../errors.js'
import { DDL_PREFIX_RE } from './constants.js'

/**
 * Per-statement hook surface exposed to a replication-aware transaction.
 *
 * `onDdl` is invoked synchronously after a DDL statement has been applied
 * inside the transaction, so the engine can record a synthetic `__ddl__`
 * CDC row on the same transactional connection (the row must be visible
 * for `stampChanges`/`updateColumnVersions` and must roll back atomically
 * if the user callback later throws).
 *
 * `droppedTables` accumulates the table names of every `DROP TABLE`
 * statement executed inside the transaction. The list is read by the
 * executor after the outer transaction commits to prune watched-tracker
 * entries that point at tables SQLite has just removed; if the
 * transaction rolls back the list is discarded by the executor without
 * touching the tracker.
 */
export interface ReplicationTransactionHooks {
  sawDdl: boolean
  droppedTables: string[]
  onDdl(sql: string): Promise<void>
}

/**
 * `Transaction` subclass that intercepts writes performed inside
 * `ReplicationEngine.transaction(fn)`.
 *
 * Behaviour relative to the core `Transaction`:
 * - `query` and `executeBatch` are inherited unchanged.
 * - `execute` adds DDL guardrails: statements with embedded semicolons are
 *   rejected, and any DDL statement that completes successfully triggers a
 *   replication-side bookkeeping hook so the engine can emit the synthetic
 *   `__ddl__` CDC row inside the same transaction.
 *
 * Inheriting `Transaction` means the public surface (including the private
 * `_lastInsertRowId` field that TypeScript checks structurally) stays
 * compatible with user callbacks typed as `(tx: Transaction) => Promise<T>`.
 */
export class ReplicationTransaction extends Transaction {
  constructor(
    txConn: SQLiteConnection,
    private readonly hooks: ReplicationTransactionHooks,
  ) {
    super(txConn)
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    const isDdl = DDL_PREFIX_RE.test(sql)
    if (isDdl && sql.includes(';')) {
      throw new ReplicationError('DDL statements containing semicolons are not allowed for replication safety')
    }

    const result = await super.execute(sql, params)

    if (isDdl) {
      this.hooks.sawDdl = true
      await this.hooks.onDdl(sql)
    }

    return result
  }

  async executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
    if (DDL_PREFIX_RE.test(sql)) {
      throw new ReplicationError('DDL statements are not allowed via executeBatch inside a replication transaction')
    }
    return super.executeBatch(sql, paramsBatch)
  }
}
