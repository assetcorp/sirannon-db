import type { SQLiteConnection } from '../driver/types.js'

export interface TableInfoRow {
  name: string
  type: string
  pk: number
}

export interface SchemaObjectRow {
  type: string
  name: string
  tbl_name: string
  sql: string
}

export async function selectTableExists(conn: SQLiteConnection, table: string): Promise<boolean> {
  const stmt = await conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
  return Boolean(await stmt.get(table))
}

export async function tableInfoRows(conn: SQLiteConnection, table: string): Promise<TableInfoRow[]> {
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  return (await stmt.all()) as TableInfoRow[]
}

export interface TableColumnRow extends TableInfoRow {
  hidden: number
}

export async function tableColumnsWithGenerated(conn: SQLiteConnection, table: string): Promise<TableColumnRow[]> {
  const stmt = await conn.prepare(`PRAGMA table_xinfo("${table}")`)
  return ((await stmt.all()) as TableColumnRow[]).map(row => ({ ...row, hidden: Number(row.hidden) }))
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

export async function selectTableSql(conn: SQLiteConnection, table: string): Promise<string | null> {
  const stmt = await conn.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?")
  const row = (await stmt.get(table)) as { sql?: string | null } | undefined
  return row?.sql ?? null
}

/**
 * Returns the statement that SQLite stores for a trigger, which SQLite rebuilds from the original `CREATE TRIGGER` without its `IF NOT EXISTS`.
 *
 * @param conn - The connection to read the schema through.
 * @param trigger - The name of the trigger.
 * @returns The stored statement, or `null` when no trigger has that name.
 */
export async function selectTriggerSql(conn: SQLiteConnection, trigger: string): Promise<string | null> {
  const stmt = await conn.prepare("SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?")
  const row = (await stmt.get(trigger)) as { sql?: string | null } | undefined
  return row?.sql ?? null
}

export async function selectCountTableRows(conn: SQLiteConnection, table: string): Promise<number> {
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

export async function selectUserSchemaObjects(
  conn: SQLiteConnection,
  excludePrefix: string,
): Promise<SchemaObjectRow[]> {
  const stmt = await conn.prepare(
    `SELECT type, name, tbl_name, sql FROM sqlite_master
     WHERE type IN ('table', 'index') AND name NOT LIKE ? AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL`,
  )
  return (await stmt.all(`${excludePrefix}%`)) as SchemaObjectRow[]
}

export async function selectUserTableNames(conn: SQLiteConnection, excludePrefix: string): Promise<string[]> {
  const stmt = await conn.prepare(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE ? AND name NOT LIKE 'sqlite_%'",
  )
  const rows = (await stmt.all(`${excludePrefix}%`)) as Array<{ name: string }>
  return rows.map(row => row.name)
}

/**
 * Returns the name that the schema stores for an ordinary table whose name matches `table` in any ASCII letter case, or `null` when no ordinary table matches, so a view or a virtual table yields `null`.
 *
 * @param conn - The connection whose `main` schema holds the table.
 * @param table - The table name that the caller passed, in any letter case.
 * @returns The stored table name, or `null` when the schema holds no ordinary table of that name.
 * @internal
 */
export async function selectOrdinaryTableName(conn: SQLiteConnection, table: string): Promise<string | null> {
  const stmt = await conn.prepare(
    `SELECT name FROM sqlite_master
     WHERE type = 'table' AND name = ? COLLATE NOCASE AND sql NOT LIKE 'CREATE VIRTUAL TABLE%'`,
  )
  const row = (await stmt.get(table)) as { name?: unknown } | undefined
  return typeof row?.name === 'string' ? row.name : null
}

export interface IndexListRow {
  name: string
  unique: number
  origin: string
  partial: number
}

/**
 * Returns the `CREATE INDEX` statement that SQLite stores for an index.
 *
 * @param conn - The connection whose schema holds the index.
 * @param index - The name of the index.
 * @returns The stored statement, or `null` for an index that a `UNIQUE` or `PRIMARY KEY` constraint created, since SQLite stores none for it.
 * @internal
 */
export async function selectIndexSql(conn: SQLiteConnection, index: string): Promise<string | null> {
  const stmt = await conn.prepare("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?")
  const row = (await stmt.get(index)) as { sql?: string | null } | undefined
  return row?.sql ?? null
}

/**
 * Returns every index on a table, in the order that `PRAGMA index_list` reports them, with whether each one is unique and whether a `CREATE INDEX` statement (`c`), a `UNIQUE` constraint (`u`), or the primary key (`pk`) created it.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns One row per index, with `unique` set to 1 for a unique index and 0 otherwise.
 * @internal
 */
export async function tableIndexList(conn: SQLiteConnection, table: string): Promise<IndexListRow[]> {
  const stmt = await conn.prepare('SELECT name, "unique", origin, partial FROM pragma_index_list(?) ORDER BY seq')
  const rows = (await stmt.all(table)) as Array<{
    name: string
    unique: number | bigint
    origin: string
    partial: number | bigint
  }>
  return rows.map(row => ({
    name: row.name,
    unique: Number(row.unique),
    origin: row.origin,
    partial: Number(row.partial),
  }))
}

export interface IndexColumnRow {
  cid: number
  name: string | null
  collation: string
  key: number
}

/**
 * Returns every column of an index in key order, as `PRAGMA index_xinfo` reports them, including the collation that the index compares each column with.
 *
 * @param conn - The connection whose schema holds the index.
 * @param index - The name of the index.
 * @returns One row per index column, where `key` is 1 for a key column and 0 for an auxiliary column, `cid` is -1 for the rowid, and `cid` is -2 with a `null` name for an expression.
 * @internal
 */
export async function indexColumns(conn: SQLiteConnection, index: string): Promise<IndexColumnRow[]> {
  const stmt = await conn.prepare('SELECT cid, name, coll, "key" FROM pragma_index_xinfo(?) ORDER BY seqno')
  const rows = (await stmt.all(index)) as Array<{
    cid: number | bigint
    name: string | null
    coll: string
    key: number | bigint
  }>
  return rows.map(row => ({ cid: Number(row.cid), name: row.name, collation: row.coll, key: Number(row.key) }))
}
