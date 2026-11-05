import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { KEY_CHANGE_CAPTURE_PREFIX } from '../internal-tables.js'
import { escId } from '../sql-identifiers.js'

/**
 * The temporary tables through which a key change records the key of every row that a write touches on one table,
 * with the slot names that hold the key's columns.
 *
 * @internal
 */
export interface KeyChangeCaptureTables {
  readonly log: string
  readonly touched: string
  readonly slots: readonly string[]
}

/**
 * Returns the names of the temporary tables that capture the writes to the table at a position in the key change's
 * list of tables, for a key of a given number of columns.
 *
 * @param position - The table's position in the key change's list.
 * @param keyColumns - The number of columns in the table's key.
 * @returns The table names and the key slots.
 * @internal
 */
export function keyChangeCaptureTables(position: number, keyColumns: number): KeyChangeCaptureTables {
  return {
    log: `${KEY_CHANGE_CAPTURE_PREFIX}${position}_log`,
    touched: `${KEY_CHANGE_CAPTURE_PREFIX}${position}_touched`,
    slots: Array.from({ length: keyColumns }, (_, n) => `k${n}`),
  }
}

/**
 * Returns the name of the temporary trigger that records one kind of write to the table at a position in the key
 * change's list of tables.
 *
 * @param position - The table's position in the key change's list.
 * @param kind - The kind of write that the trigger records.
 * @returns The trigger name.
 * @internal
 */
export function keyChangeCaptureTriggerName(position: number, kind: 'insert' | 'update' | 'delete'): string {
  return `${KEY_CHANGE_CAPTURE_PREFIX}${position}_${kind}`
}

/**
 * Creates the temporary table into which the capture triggers write the key of each row that a write touches.
 *
 * @param conn - The original's writer.
 * @param tables - The capture tables of one captured table.
 * @returns A promise that settles once the table exists.
 * @internal
 */
export async function ensureKeyChangeLogTable(conn: SQLiteConnection, tables: KeyChangeCaptureTables): Promise<void> {
  await conn.exec(`CREATE TEMP TABLE "${escId(tables.log)}" (${tables.slots.join(', ')})`)
}

/**
 * Returns each distinct key in the log, with every slot as its type name under `t_<slot>` and its value under
 * `v_<slot>`, where an integer value comes back as text so that no driver narrows it.
 *
 * @param conn - The original's writer.
 * @param tables - The capture tables of one captured table.
 * @returns The distinct keys.
 * @internal
 */
export async function selectDistinctKeyChangeLogKeys(
  conn: SQLiteConnection,
  tables: KeyChangeCaptureTables,
): Promise<Record<string, unknown>[]> {
  const columns = tables.slots
    .map(
      slot =>
        `typeof(${slot}) AS t_${slot}, CASE WHEN typeof(${slot}) = 'integer' THEN CAST(${slot} AS TEXT) ELSE ${slot} END AS v_${slot}`,
    )
    .join(', ')
  const stmt = await conn.prepare(`SELECT DISTINCT ${columns} FROM temp."${escId(tables.log)}"`)
  return (await stmt.all()) as Record<string, unknown>[]
}

/**
 * Empties the log of one captured table.
 *
 * @param conn - The original's writer.
 * @param tables - The capture tables of one captured table.
 * @returns A promise that settles once the log is empty.
 * @internal
 */
export async function deleteKeyChangeLogKeys(conn: SQLiteConnection, tables: KeyChangeCaptureTables): Promise<void> {
  await conn.exec(`DELETE FROM temp."${escId(tables.log)}"`)
}

/**
 * Drops the log of one captured table, where it exists.
 *
 * @param conn - The original's writer.
 * @param tables - The capture tables of one captured table.
 * @returns A promise that settles once the log is gone.
 * @internal
 */
export async function dropKeyChangeLogTable(conn: SQLiteConnection, tables: KeyChangeCaptureTables): Promise<void> {
  await conn.exec(`DROP TABLE IF EXISTS temp."${escId(tables.log)}"`)
}

/**
 * Creates, where it is missing, and empties the temporary table on the new file's connection that holds the keys of
 * one round, so that the round can copy those rows from the original.
 *
 * @param conn - The connection to the new file.
 * @param tables - The capture tables of one captured table.
 * @returns A promise that settles once the table exists and is empty.
 * @internal
 */
export async function ensureEmptyKeyChangeTouchedTable(
  conn: SQLiteConnection,
  tables: KeyChangeCaptureTables,
): Promise<void> {
  await conn.exec(`CREATE TEMP TABLE IF NOT EXISTS "${escId(tables.touched)}" (${tables.slots.join(', ')})`)
  await conn.exec(`DELETE FROM temp."${escId(tables.touched)}"`)
}

/**
 * Returns a prepared statement that stores one key of a round, which binds, for every slot in order, the type name,
 * then the value twice, as {@link selectDistinctKeyChangeLogKeys} returns them.
 *
 * @param conn - The connection to the new file.
 * @param tables - The capture tables of one captured table.
 * @returns The statement.
 * @internal
 */
export function prepareInsertKeyChangeTouchedKey(
  conn: SQLiteConnection,
  tables: KeyChangeCaptureTables,
): Promise<SQLiteStatement> {
  const values = tables.slots.map(() => "CASE WHEN ? = 'integer' THEN CAST(? AS INTEGER) ELSE ? END").join(', ')
  return conn.prepare(`INSERT INTO temp."${escId(tables.touched)}" VALUES (${values})`)
}

/**
 * Returns the statement that selects every key of a round, for use as the right side of an `IN` condition.
 *
 * @param tables - The capture tables of one captured table.
 * @returns The statement text.
 * @internal
 */
export function selectKeyChangeTouchedKeysSql(tables: KeyChangeCaptureTables): string {
  return `SELECT ${tables.slots.join(', ')} FROM temp."${escId(tables.touched)}"`
}
