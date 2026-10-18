import type { SQLiteConnection } from '../driver/types.js'

/**
 * Holds one column of a table as `PRAGMA table_info` reports it.
 *
 * @internal
 */
export interface TableInfoRow {
  name: string
  type: string
  pk: number
}

/**
 * Holds one table or index from `sqlite_master`, with the statement that SQLite stores for it.
 *
 * @internal
 */
export interface SchemaObjectRow {
  type: string
  name: string
  tbl_name: string
  sql: string
}

/**
 * The values of the `hidden` column of `PRAGMA table_xinfo`: an ordinary column, a hidden column of a virtual table,
 * a virtual generated column, and a stored generated column.
 *
 * @internal
 */
export const TABLE_XINFO_HIDDEN = {
  ordinary: 0,
  virtualTableColumn: 1,
  generatedVirtual: 2,
  generatedStored: 3,
} as const

/**
 * Returns whether the schema holds a table of a given name.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns `true` when a table of that name exists.
 * @internal
 */
export async function selectTableExists(conn: SQLiteConnection, table: string): Promise<boolean> {
  const stmt = await conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
  return Boolean(await stmt.get(table))
}

/**
 * Returns every column of a table that `PRAGMA table_info` reports, which leaves out generated columns.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns One row per column, in declaration order, or no rows when the table does not exist.
 * @internal
 */
export async function tableInfoRows(conn: SQLiteConnection, table: string): Promise<TableInfoRow[]> {
  const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  return (await stmt.all()) as TableInfoRow[]
}

/**
 * Holds one column of a table as `PRAGMA table_xinfo` reports it, with the `hidden` value that
 * {@link TABLE_XINFO_HIDDEN} names.
 *
 * @internal
 */
export interface TableColumnRow extends TableInfoRow {
  hidden: number
}

/**
 * Returns every column of a table, generated columns included, as `PRAGMA table_xinfo` reports them.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns One row per column, in declaration order, each with `hidden` as a number.
 * @internal
 */
export async function tableColumnsWithGenerated(conn: SQLiteConnection, table: string): Promise<TableColumnRow[]> {
  const stmt = await conn.prepare(`PRAGMA table_xinfo("${table}")`)
  return ((await stmt.all()) as TableColumnRow[]).map(row => ({ ...row, hidden: Number(row.hidden) }))
}

/**
 * Returns the names of a table's columns that `PRAGMA table_info` reports.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns The column names, in declaration order.
 * @internal
 */
export async function tableColumnNames(conn: SQLiteConnection, table: string): Promise<string[]> {
  return (await tableInfoRows(conn, table)).map(col => col.name)
}

/**
 * Returns the primary-key columns among a table's columns, in the order that the key lists them.
 *
 * @param columns - The columns that `PRAGMA table_info` or `PRAGMA table_xinfo` reports.
 * @returns The names of the key columns in key order, or no names when the table declares no primary key.
 * @internal
 */
export function primaryKeyColumns(columns: readonly TableInfoRow[]): string[] {
  return columns
    .filter(column => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map(column => column.name)
}

/**
 * Returns the primary-key columns of a table, in the order that the key lists them.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns The names of the key columns in key order, or no names when the table declares no primary key.
 * @internal
 */
export async function tablePkColumns(conn: SQLiteConnection, table: string): Promise<string[]> {
  return primaryKeyColumns(await tableInfoRows(conn, table))
}

/**
 * Returns the `CREATE TABLE` statement that SQLite stores for a table.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns The stored statement, or `null` when no table has that name.
 * @internal
 */
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

/**
 * Returns the number of rows in a table.
 *
 * @param conn - The connection to read the table through.
 * @param table - The stored name of the table.
 * @returns The row count.
 * @internal
 */
export async function selectCountTableRows(conn: SQLiteConnection, table: string): Promise<number> {
  const stmt = await conn.prepare(`SELECT COUNT(*) AS cnt FROM "${table}"`)
  const row = (await stmt.get()) as { cnt: number | bigint } | undefined
  return Number(row?.cnt ?? 0)
}

/**
 * Returns the tables that a table's foreign keys refer to.
 *
 * @param conn - The connection whose schema holds the table.
 * @param table - The stored name of the table.
 * @returns One table name per foreign-key column, so a table that two columns refer to appears twice.
 * @internal
 */
export async function referencedTables(conn: SQLiteConnection, table: string): Promise<string[]> {
  const stmt = await conn.prepare(`PRAGMA foreign_key_list("${table}")`)
  const rows = (await stmt.all()) as Array<{ table: string }>
  return rows.map(row => row.table)
}

/**
 * Turns foreign-key enforcement on or off for a connection, which SQLite applies only outside a transaction.
 *
 * @param conn - The connection to change.
 * @param enabled - `true` to enforce foreign keys.
 * @returns A promise that settles once SQLite has applied the setting.
 * @internal
 */
export async function setForeignKeysEnabled(conn: SQLiteConnection, enabled: boolean): Promise<void> {
  await conn.exec(enabled ? 'PRAGMA foreign_keys = ON' : 'PRAGMA foreign_keys = OFF')
}

/**
 * Returns every table and index that the application created, with the statement that SQLite stores for each.
 *
 * @param conn - The connection whose schema to read.
 * @param excludePrefix - The name prefix of the tables and indexes to leave out, such as Sirannon's own.
 * @returns One row per table or index, leaving out SQLite's own objects and every index that a constraint created.
 * @internal
 */
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

/**
 * Returns the names of every table that the application created.
 *
 * @param conn - The connection whose schema to read.
 * @param excludePrefix - The name prefix of the tables to leave out, such as Sirannon's own.
 * @returns The table names, leaving out SQLite's own tables.
 * @internal
 */
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

/**
 * Holds one index of a table as `PRAGMA index_list` reports it.
 *
 * @internal
 */
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

/**
 * Holds one column of an index as `PRAGMA index_xinfo` reports it.
 *
 * @internal
 */
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
