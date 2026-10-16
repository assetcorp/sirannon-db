import type { SQLiteConnection } from '../driver/types.js'
import { Transaction } from '../transaction.js'
import type { ExecuteResult, Params } from '../types.js'
import type { ChangeTracker } from './change-tracker.js'
import { executeAllowingColumnDrop } from './column-drop.js'
import { extractDroppedTable, isCdcRelevantDdl } from './ddl-handler.js'

export interface CdcTransactionState {
  sawDdl: boolean
  droppedTables: string[]
}

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
    this.assertOpen()
    if (!isCdcRelevantDdl(sql)) {
      return await super.execute(sql, params)
    }

    const result = await executeAllowingColumnDrop(this.tracker, this.txConn, sql, () => super.execute(sql, params))
    await this.recordDdl(sql)
    return result
  }

  async executeBatch(sql: string, paramsBatch: Params[]): Promise<ExecuteResult[]> {
    this.assertOpen()
    if (!isCdcRelevantDdl(sql)) {
      return await super.executeBatch(sql, paramsBatch)
    }

    const results = await executeAllowingColumnDrop(this.tracker, this.txConn, sql, () =>
      super.executeBatch(sql, paramsBatch),
    )
    await this.recordDdl(sql)
    return results
  }

  private async recordDdl(sql: string): Promise<void> {
    this.state.sawDdl = true
    const dropped = extractDroppedTable(sql)
    if (dropped !== null) {
      this.state.droppedTables.push(dropped)
    }

    if (this.tracker.watchedTables.size > 0) {
      await this.tracker.refreshAllTriggersUsingConnection(this.txConn)
    }
  }
}
