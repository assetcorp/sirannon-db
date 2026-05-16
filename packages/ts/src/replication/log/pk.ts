import type { SQLiteConnection } from '../../core/driver/types.js'
import type { ColumnInfoRow } from './internal-types.js'

export class PkResolver {
  private readonly cache = new Map<string, string[]>()

  constructor(private readonly conn: SQLiteConnection) {}

  async forTable(table: string): Promise<string[]> {
    const cached = this.cache.get(table)
    if (cached) return cached

    const stmt = await this.conn.prepare(`PRAGMA table_info("${table}")`)
    const info = (await stmt.all()) as ColumnInfoRow[]
    const pkCols = info
      .filter(col => col.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map(col => col.name)

    if (pkCols.length === 0) {
      pkCols.push('rowid')
    }

    this.cache.set(table, pkCols)
    return pkCols
  }

  async forTableOnConnection(conn: SQLiteConnection, table: string): Promise<string[]> {
    const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
    const info = (await stmt.all()) as ColumnInfoRow[]
    const pkCols = info
      .filter(col => col.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map(col => col.name)
    if (pkCols.length === 0) {
      pkCols.push('rowid')
    }
    return pkCols
  }
}
