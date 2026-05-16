import type { SQLiteConnection } from '../driver/types.js'
import { Transaction } from '../transaction.js'
import type { ExecuteResult, Params } from '../types.js'
import type { ChangeTracker } from './change-tracker.js'
import { extractDroppedTable, isCdcRelevantDdl } from './ddl-handler.js'

/**
 * Bookkeeping surface populated as DDL statements execute inside a
 * `Database.transaction(fn)` callback. The owning database reads
 * `droppedTables` after the outer transaction commits so it can prune the
 * `ChangeTracker` watched map for every `DROP TABLE` that ran inside the
 * transaction. If the transaction rolls back the database discards this
 * state without touching the tracker.
 */
export interface CdcTransactionState {
  sawDdl: boolean
  droppedTables: string[]
}

/**
 * `Transaction` subclass that the core `Database.transaction(fn)` hands to
 * the user callback whenever the database has an active `ChangeTracker`.
 *
 * Behaviour relative to the base `Transaction`:
 * - `query` and `executeBatch` are inherited unchanged.
 * - `execute` runs the underlying statement first, then for any DDL whose
 *   prefix matches `isCdcRelevantDdl` it refreshes CDC triggers on the
 *   same transactional connection so subsequent DML inside the
 *   transaction picks up the new column list. Dropped table names are
 *   appended to `state.droppedTables` for post-commit pruning.
 *
 * The refresh runs only when the tracker has at least one watched table,
 * so the zero-watched case incurs only the regex test on `execute`.
 *
 * Failure semantics: if the user's DDL throws (e.g. SQLite reports a
 * constraint violation), the override never observes a "succeeded" DDL and
 * never updates state. If the trigger refresh throws after a successful
 * DDL, the error propagates out of `execute`; the caller's transaction
 * rolls back and the on-disk schema reverts together with the trigger
 * changes (since both happen on the same `BEGIN`).
 */
export class CdcAwareTransaction extends Transaction {
  private readonly txConn: SQLiteConnection

  constructor(
    txConn: SQLiteConnection,
    private readonly tracker: ChangeTracker,
    private readonly state: CdcTransactionState,
  ) {
    super(txConn)
    this.txConn = txConn
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResult> {
    const isDdl = isCdcRelevantDdl(sql)
    const result = await super.execute(sql, params)

    if (!isDdl) {
      return result
    }

    this.state.sawDdl = true
    const dropped = extractDroppedTable(sql)
    if (dropped !== null) {
      this.state.droppedTables.push(dropped)
    }

    if (this.tracker.watchedTables.size > 0) {
      await this.tracker.refreshAllTriggersUsingConnection(this.txConn)
    }

    return result
  }
}
