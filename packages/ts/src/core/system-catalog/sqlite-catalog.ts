import type { SQLiteConnection } from '../driver/types.js'

export interface TableInfoRow {
  name: string
  pk: number
}

export interface SchemaObjectRow {
  type: string
  name: string
  tbl_name: string
  sql: string
}

export async function tableExists(conn: SQLiteConnection, table: string): Promise<boolean> {
  const stmt = await conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
  return Boolean(await stmt.get(table))
}

export async function tableInfoRows(conn: SQLiteConnection, table: string): Promise<TableInfoRow[]> {
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  return (await stmt.all()) as TableInfoRow[]
}

export async function tableColumnNames(conn: SQLiteConnection, table: string): Promise<string[]> {
  return (await tableInfoRows(conn, table)).map(col => col.name)
}

export async function tablePkColumns(conn: SQLiteConnection, table: string): Promise<string[]> {
  return (await tableInfoRows(conn, table))
    .filter(col => col.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map(col => col.name)
}

export async function countTableRows(conn: SQLiteConnection, table: string): Promise<number> {
  const stmt = await conn.prepare(`SELECT COUNT(*) AS cnt FROM "${table}"`)
  const row = (await stmt.get()) as { cnt: number | bigint } | undefined
  return Number(row?.cnt ?? 0)
}

export async function referencedTables(conn: SQLiteConnection, table: string): Promise<string[]> {
  const stmt = await conn.prepare(`PRAGMA foreign_key_list("${table}")`)
  const rows = (await stmt.all()) as Array<{ table: string }>
  return rows.map(row => row.table)
}

export async function setForeignKeysEnabled(conn: SQLiteConnection, enabled: boolean): Promise<void> {
  await conn.exec(enabled ? 'PRAGMA foreign_keys = ON' : 'PRAGMA foreign_keys = OFF')
}

export async function userSchemaObjects(conn: SQLiteConnection, excludePrefix: string): Promise<SchemaObjectRow[]> {
  const stmt = await conn.prepare(
    `SELECT type, name, tbl_name, sql FROM sqlite_master
     WHERE type IN ('table', 'index') AND name NOT LIKE ? AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL`,
  )
  return (await stmt.all(`${excludePrefix}%`)) as SchemaObjectRow[]
}

export async function userTableNames(conn: SQLiteConnection, excludePrefix: string): Promise<string[]> {
  const stmt = await conn.prepare(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE ? AND name NOT LIKE 'sqlite_%'",
  )
  const rows = (await stmt.all(`${excludePrefix}%`)) as Array<{ name: string }>
  return rows.map(row => row.name)
}
