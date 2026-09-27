import type { SQLiteConnection } from '../../core/driver/types.js'
import { Transaction } from '../../core/transaction.js'
import type { ExecuteResult, Params } from '../../core/types.js'
import { ReplicationError } from '../errors.js'
import { DDL_PREFIX_RE } from './constants.js'

/**
 * Holds the DDL callback and the DDL record that {@link ReplicationTransaction} shares with `LocalExecutor` for one
 * transaction.
 *
 * After each DDL statement succeeds, `execute` sets `sawDdl` and awaits
 * `onDdl`, which writes a synthetic `__ddl__` CDC row on the transaction's own
 * connection. `onDdl` writes that row inside the open transaction, so that
 * `stampChanges` and `updateColumnVersions` include it and SQLite rolls it back
 * if the caller's callback throws.
 *
 * `droppedTables` lists every table that a `DROP TABLE` statement in the
 * transaction removes. The executor prunes those tables from the change
 * tracker only after the transaction commits.
 */
export interface ReplicationTransactionHooks {
  sawDdl: boolean
  droppedTables: string[]
  onDdl(sql: string): Promise<void>
}

/**
 * Gives the callback of `ReplicationEngine.transaction(fn)` a {@link Transaction} that records DDL for replication.
 *
 * `query` behaves as it does on the base class. `execute` throws a
 * `ReplicationError` for a DDL statement that contains a semicolon, and after a
 * DDL statement succeeds it awaits the `onDdl` hook, so that the executor
 * writes the synthetic `__ddl__` CDC row inside the same transaction.
 * `executeBatch` throws a `ReplicationError` for any DDL statement.
 *
 * Because the class extends `Transaction`, it keeps the private
 * `_lastInsertRowId` field that TypeScript compares structurally, so a
 * callback typed as `(tx: Transaction) => Promise<T>` accepts it.
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
