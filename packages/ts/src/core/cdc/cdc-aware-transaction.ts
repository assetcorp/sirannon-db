import type { SQLiteConnection } from '../driver/types.js'
import { Transaction } from '../transaction.js'
import type { ExecuteResult, Params } from '../types.js'
import type { ChangeTracker } from './change-tracker.js'
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
