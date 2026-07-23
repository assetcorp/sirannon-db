import type { SQLiteConnection } from '../driver/types.js'
import type { ColumnInfo } from './types.js'

export async function tableColumnNames(conn: SQLiteConnection, table: string): Promise<string[]> {
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  const info = (await stmt.all()) as ColumnInfo[]
  return info.map(col => col.name)
}

export async function tablePkColumns(conn: SQLiteConnection, table: string): Promise<string[]> {
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  const info = (await stmt.all()) as ColumnInfo[]
  return info
    .filter(col => col.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map(col => col.name)
}
