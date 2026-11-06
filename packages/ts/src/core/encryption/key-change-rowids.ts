import type { SQLiteConnection } from '../driver/types.js'
import { escId, ROWID_ALIASES } from '../sql-identifiers.js'
import { TABLE_XINFO_HIDDEN } from '../system-catalog/sqlite-catalog.js'
import { EncryptionError } from './errors.js'

const PRIMARY_KEY_INDEX_ORIGIN = 'pk'

interface TableColumn {
  name: string
  pk: unknown
  hidden: unknown
}

function quote(name: string): string {
  return `"${escId(name)}"`
}

async function readCount(conn: SQLiteConnection, sql: string, ...params: unknown[]): Promise<number> {
  const row = (await (await conn.prepare(sql)).get(...params)) as Record<string, unknown> | undefined
  const value = row === undefined ? undefined : Object.values(row)[0]
  const count = typeof value === 'bigint' ? Number(value) : value
  if (typeof count !== 'number' || !Number.isSafeInteger(count)) {
    throw new EncryptionError(`SQLite returned no count for '${sql}'`, 'ENCRYPTION_CHANGE_ABORTED')
  }
  return count
}

async function rowidTables(conn: SQLiteConnection): Promise<string[]> {
  const rows = (await (
    await conn.prepare(
      "SELECT name FROM pragma_table_list WHERE schema = 'main' AND type = 'table' AND wr = 0 AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'",
    )
  ).all()) as { name: string }[]
  return rows.map(row => row.name)
}

async function rowidIsPrimaryKey(
  conn: SQLiteConnection,
  table: string,
  columns: readonly TableColumn[],
): Promise<boolean> {
  if (!columns.some(column => Number(column.pk) > 0)) return false
  const primaryKeyIndexes = await readCount(
    conn,
    "SELECT count(*) FROM pragma_index_list(?, 'main') WHERE origin = ?",
    table,
    PRIMARY_KEY_INDEX_ORIGIN,
  )
  return primaryKeyIndexes === 0
}

/**
 * Returns a name through which a statement can read a rowid table's rowid: the first of `rowid`, `oid`, and `_rowid_`
 * that no column of the table takes.
 *
 * @param table - The table, as the error names it.
 * @param columns - The table's columns.
 * @returns The free name.
 * @throws An `EncryptionError` with `ENCRYPTION_CHANGE_ABORTED` when the table has a column under each of the three
 * names.
 * @internal
 */
export function freeRowidAlias(table: string, columns: readonly { name: string }[]): string {
  const taken = new Set(columns.map(column => column.name.toLowerCase()))
  const name = ROWID_ALIASES.find(candidate => !taken.has(candidate))
  if (name === undefined) {
    throw new EncryptionError(
      `The table ${table} has columns named rowid, _rowid_, and oid, so the key change cannot address its rows`,
      'ENCRYPTION_CHANGE_ABORTED',
    )
  }
  return name
}

async function copyWithRowids(
  conn: SQLiteConnection,
  table: string,
  columns: readonly TableColumn[],
  targetSchema: string,
): Promise<void> {
  const rowid = freeRowidAlias(table, columns)
  const stored = columns
    .filter(column => {
      const hidden = Number(column.hidden)
      return hidden !== TABLE_XINFO_HIDDEN.generatedStored && hidden !== TABLE_XINFO_HIDDEN.generatedVirtual
    })
    .map(column => quote(column.name))
    .join(', ')
  const rows = await readCount(conn, `SELECT count(*) FROM main.${quote(table)}`)
  const changesBefore = await readCount(conn, 'SELECT total_changes()')
  await conn.exec(`DELETE FROM ${targetSchema}.${quote(table)}`)
  await conn.exec(
    `INSERT INTO ${targetSchema}.${quote(table)} (${rowid}, ${stored}) SELECT ${rowid}, ${stored} FROM main.${quote(table)}`,
  )
  const changes = (await readCount(conn, 'SELECT total_changes()')) - changesBefore
  if (changes !== rows * 2) {
    throw new EncryptionError(
      `Copying the ${rows} rows of the table ${table} with their rowids changed ${changes} rows, so the key change stopped`,
      'ENCRYPTION_CHANGE_ABORTED',
    )
  }
}

/**
 * Gives every row of the new file the rowid that it holds in the original, for each rowid table whose rowid is not an
 * INTEGER PRIMARY KEY, because SQLCipher's `sqlcipher_export` numbers the rows of such a table afresh when the table
 * has no index.
 *
 * @param snapshot - The snapshot connection, with foreign keys off, inside the transaction that holds the copy.
 * @param targetSchema - The schema under which the snapshot connection attaches the new file.
 * @throws An `EncryptionError` with `ENCRYPTION_CHANGE_ABORTED` when a table has columns named rowid, _rowid_, and oid,
 * or when the copy of a table changes a number of rows other than twice the rows that it holds.
 * @internal
 */
export async function copyRowidsAfterSqlcipherExport(snapshot: SQLiteConnection, targetSchema: string): Promise<void> {
  for (const table of await rowidTables(snapshot)) {
    const columns = (await (
      await snapshot.prepare("SELECT name, pk, hidden FROM pragma_table_xinfo(?, 'main')")
    ).all(table)) as TableColumn[]
    if (await rowidIsPrimaryKey(snapshot, table, columns)) continue
    await copyWithRowids(snapshot, table, columns, targetSchema)
  }
}
