import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { assertSafeIdentifier } from './columns.js'

const SAFE_DECLARED_TYPE = /^[A-Za-z0-9_ ()',.+-]*$/

/**
 * Describes one column of the watched table that a live query's probe table copies: its name, its declared type,
 * its collation, and the expression of a generated column.
 *
 * @internal
 */
export interface LiveProbeColumn {
  name: string
  type: string
  collation: string | null
  generatedAs: string | null
}

/**
 * Returns the columns that hold a stored value, leaving out every generated column.
 *
 * @param columns - The columns of the watched table.
 * @returns The stored columns, in their original order.
 * @internal
 */
export function storedLiveProbeColumns(columns: readonly LiveProbeColumn[]): LiveProbeColumn[] {
  return columns.filter(column => column.generatedAs === null)
}

/**
 * Creates the temporary table into which a live query copies changed rows to test them against its filter, with the
 * same columns, types, collations, and generated expressions as the watched table, where the table is missing.
 *
 * @param conn - The connection that evaluates the live query.
 * @param probeTable - The temporary table's name, which has to be a safe identifier.
 * @param columns - The columns of the watched table.
 * @returns A promise that settles once the table exists.
 * @throws {@link SirannonError} with code `CDC_ERROR` when `columns` is empty, and with code `INTERNAL_SCHEMA_ERROR`
 * when a declared type holds a character outside the letters, digits, spaces, and `_()',.+-`.
 * @internal
 */
export async function ensureLiveProbeTable(
  conn: SQLiteConnection,
  probeTable: string,
  columns: readonly LiveProbeColumn[],
): Promise<void> {
  assertSafeIdentifier(probeTable)
  if (columns.length === 0) {
    throw new SirannonError(`Cannot stage rows for '${probeTable}': the table declares no columns`, 'CDC_ERROR')
  }

  const definitions = columns.map(column => {
    assertSafeIdentifier(column.name)
    if (!SAFE_DECLARED_TYPE.test(column.type)) {
      throw new SirannonError(`Unsafe column type for internal schema: ${column.type}`, 'INTERNAL_SCHEMA_ERROR')
    }
    if (column.collation !== null) assertSafeIdentifier(column.collation)

    const type = column.type.trim().length === 0 ? '' : ` ${column.type.trim()}`
    const collate = column.collation === null ? '' : ` COLLATE "${column.collation}"`
    const generated = column.generatedAs === null ? '' : ` GENERATED ALWAYS AS (${column.generatedAs}) VIRTUAL`
    return `"${column.name}"${type}${collate}${generated}`
  })

  await conn.exec(`CREATE TEMP TABLE IF NOT EXISTS "${probeTable}" (${definitions.join(', ')})`)
}

/**
 * Drops a live query's temporary probe table, where it exists.
 *
 * @param conn - The connection that evaluates the live query.
 * @param probeTable - The temporary table's name, which has to be a safe identifier.
 * @returns A promise that settles once the table is gone.
 * @internal
 */
export async function dropLiveProbeTable(conn: SQLiteConnection, probeTable: string): Promise<void> {
  assertSafeIdentifier(probeTable)
  await conn.exec(`DROP TABLE IF EXISTS temp."${probeTable}"`)
}

/**
 * Returns a prepared statement that copies one changed row into a live query's temporary probe table.
 *
 * @param conn - The connection that evaluates the live query.
 * @param probeTable - The temporary table's name, which has to be a safe identifier.
 * @param columns - The columns of the watched table.
 * @returns The statement, which binds the row's rowid, then the value of each stored column in the order that
 * {@link storedLiveProbeColumns} returns them.
 * @internal
 */
export async function prepareInsertLiveProbeRow(
  conn: SQLiteConnection,
  probeTable: string,
  columns: readonly LiveProbeColumn[],
): Promise<SQLiteStatement> {
  assertSafeIdentifier(probeTable)
  const stored = storedLiveProbeColumns(columns)
  const names = stored.map(column => {
    assertSafeIdentifier(column.name)
    return `"${column.name}"`
  })
  const placeholders = new Array(stored.length + 1).fill('?').join(', ')
  return conn.prepare(`INSERT INTO temp."${probeTable}" (rowid, ${names.join(', ')}) VALUES (${placeholders})`)
}

/**
 * Returns a prepared statement that empties a live query's temporary probe table.
 *
 * @param conn - The connection that evaluates the live query.
 * @param probeTable - The temporary table's name, which has to be a safe identifier.
 * @returns The statement, which binds nothing.
 * @internal
 */
export async function prepareDeleteLiveProbeRows(conn: SQLiteConnection, probeTable: string): Promise<SQLiteStatement> {
  assertSafeIdentifier(probeTable)
  return conn.prepare(`DELETE FROM temp."${probeTable}"`)
}

/**
 * Returns the statement that selects the rows of a live query's temporary probe table which pass the query's filter,
 * each with its rowid and the query's projection.
 *
 * @param probeTable - The temporary table's name, which has to be a safe identifier.
 * @param keyColumn - The name under which the statement returns each rowid, which has to be a safe identifier.
 * @param projection - The live query's select list.
 * @param where - The live query's filter, or `null` when it has none.
 * @param sourceName - The name of the watched table in the live query, which the statement gives the probe table so
 * that the projection and the filter resolve against it.
 * @returns The statement text.
 * @internal
 */
export function selectLiveProbeMatchesSql(
  probeTable: string,
  keyColumn: string,
  projection: string,
  where: string | null,
  sourceName: string,
): string {
  assertSafeIdentifier(probeTable)
  assertSafeIdentifier(keyColumn)
  const filter = where === null ? '' : `\nWHERE (${where}\n)`
  return `SELECT rowid AS "${keyColumn}"\n, ${projection}\nFROM temp."${probeTable}" AS "${sourceName}"${filter}`
}
