import type { SQLiteConnection } from '../driver/types.js'
import { tablePkColumns } from '../system-catalog/index.js'

export class PkResolver {
  private readonly cache = new Map<string, string[]>()

  constructor(private readonly conn: SQLiteConnection) {}

  async forTable(table: string): Promise<string[]> {
    const cached = this.cache.get(table)
    if (cached) return cached

    const pkCols = await this.forTableOnConnection(this.conn, table)
    this.cache.set(table, pkCols)
    return pkCols
  }

  async forTableOnConnection(conn: SQLiteConnection, table: string): Promise<string[]> {
    const pkCols = await tablePkColumns(conn, table)
    if (pkCols.length === 0) {
      pkCols.push('rowid')
    }
    return pkCols
  }
}
