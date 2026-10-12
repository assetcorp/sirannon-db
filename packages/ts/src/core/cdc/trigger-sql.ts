import type { SQLiteConnection } from '../driver/types.js'
import { CDC_TRIGGER_PREFIX } from '../internal-tables.js'
import { selectTriggerSql } from '../system-catalog/index.js'
import { SAFE_INT_BOUND_TEXT } from './encoding.js'

const FULL_PRECISION_REAL_FORMAT = '%!.17g'
const POSITIVE_INFINITY_LITERAL = '9e999'

interface CdcTriggerDefinition {
  name: string
  body: string
}

/**
 * Drops a table's change-capture triggers, skipping each one that does not exist.
 *
 * @param conn - The writer connection on which to drop the triggers.
 * @param table - The table whose triggers to drop.
 */
export async function dropCdcTriggers(conn: SQLiteConnection, table: string): Promise<void> {
  await conn.exec(`DROP TRIGGER IF EXISTS "${CDC_TRIGGER_PREFIX}${table}_insert"`)
  await conn.exec(`DROP TRIGGER IF EXISTS "${CDC_TRIGGER_PREFIX}${table}_update"`)
  await conn.exec(`DROP TRIGGER IF EXISTS "${CDC_TRIGGER_PREFIX}${table}_delete"`)
}

/**
 * Creates the insert, update, and delete triggers that record each write to a table in the change log, and leaves in place each trigger that already exists under the same name.
 *
 * @param conn - The writer connection on which to create the triggers.
 * @param changesTable - The change log that the triggers write to.
 * @param table - The watched table.
 * @param columns - The columns that each recorded row image holds, in table order.
 * @param pkColumns - The primary-key columns that form each change's row id, which is the rowid when the list is empty.
 */
export async function installCdcTriggers(
  conn: SQLiteConnection,
  changesTable: string,
  table: string,
  columns: string[],
  pkColumns: string[],
): Promise<void> {
  for (const trigger of cdcTriggerDefinitions(changesTable, table, columns, pkColumns)) {
    await conn.exec(`CREATE TRIGGER IF NOT EXISTS ${trigger.body}`)
  }
}

/**
 * Returns whether the database holds all three change-capture triggers for a table exactly as {@link installCdcTriggers} would create them now, so that triggers left by an earlier session with other columns or older SQL can be replaced.
 *
 * @param conn - The connection to read the schema through.
 * @param changesTable - The change log that the triggers write to.
 * @param table - The watched table.
 * @param columns - The columns that each recorded row image holds, in table order.
 * @param pkColumns - The primary-key columns that form each change's row id.
 * @returns `true` when every trigger exists with the expected statement.
 */
export async function cdcTriggersMatch(
  conn: SQLiteConnection,
  changesTable: string,
  table: string,
  columns: string[],
  pkColumns: string[],
): Promise<boolean> {
  for (const trigger of cdcTriggerDefinitions(changesTable, table, columns, pkColumns)) {
    const stored = await selectTriggerSql(conn, trigger.name)
    if (stored === null || stored.trim() !== `CREATE TRIGGER ${trigger.body}`) {
      return false
    }
  }
  return true
}

function cdcTriggerDefinitions(
  changesTable: string,
  table: string,
  columns: string[],
  pkColumns: string[],
): CdcTriggerDefinition[] {
  const newJson = buildJsonObject(columns, 'NEW')
  const oldJson = buildJsonObject(columns, 'OLD')
  const newPk = buildPkRef(pkColumns, 'NEW')
  const oldPk = buildPkRef(pkColumns, 'OLD')

  const replCols = ', node_id, tx_id, hlc'
  const replVals = ", '', '', ''"

  const insertName = `${CDC_TRIGGER_PREFIX}${table}_insert`
  const updateName = `${CDC_TRIGGER_PREFIX}${table}_update`
  const deleteName = `${CDC_TRIGGER_PREFIX}${table}_delete`

  return [
    {
      name: insertName,
      body: `"${insertName}"
			AFTER INSERT ON "${table}"
			BEGIN
				INSERT INTO "${changesTable}" (table_name, operation, row_id, new_data${replCols})
				VALUES ('${table}', 'INSERT', ${newPk}, ${newJson}${replVals});
			END`,
    },
    {
      name: updateName,
      body: `"${updateName}"
			AFTER UPDATE ON "${table}"
			BEGIN
				INSERT INTO "${changesTable}" (table_name, operation, row_id, old_data, new_data${replCols})
				VALUES ('${table}', 'UPDATE', ${newPk}, ${oldJson}, ${newJson}${replVals});
			END`,
    },
    {
      name: deleteName,
      body: `"${deleteName}"
			AFTER DELETE ON "${table}"
			BEGIN
				INSERT INTO "${changesTable}" (table_name, operation, row_id, old_data${replCols})
				VALUES ('${table}', 'DELETE', ${oldPk}, ${oldJson}${replVals});
			END`,
    },
  ]
}

function buildPkRef(pkColumns: string[], ref: 'NEW' | 'OLD'): string {
  if (pkColumns.length === 0) {
    return `${ref}.rowid`
  }
  if (pkColumns.length === 1) {
    return `${ref}."${escId(pkColumns[0])}"`
  }
  return pkColumns.map(col => `${ref}."${escId(col)}"`).join(" || '-' || ")
}

function buildJsonObject(columns: string[], ref: 'NEW' | 'OLD'): string {
  const pairs = columns
    .map(col => {
      const ident = `${ref}."${escId(col)}"`
      return (
        `'${escStr(col)}', ` +
        `CASE typeof(${ident}) ` +
        `WHEN 'blob' THEN json(json_object('__sirannon_blob', hex(${ident}))) ` +
        `WHEN 'integer' THEN ` +
        `CASE WHEN ${ident} > ${SAFE_INT_BOUND_TEXT} OR ${ident} < -${SAFE_INT_BOUND_TEXT} ` +
        `THEN json(json_object('__sirannon_int', printf('%d', ${ident}))) ` +
        `ELSE ${ident} END ` +
        `WHEN 'real' THEN ` +
        `CASE WHEN ${ident} = ${POSITIVE_INFINITY_LITERAL} THEN json('${POSITIVE_INFINITY_LITERAL}') ` +
        `WHEN ${ident} = -${POSITIVE_INFINITY_LITERAL} THEN json('-${POSITIVE_INFINITY_LITERAL}') ` +
        `ELSE json(printf('${FULL_PRECISION_REAL_FORMAT}', ${ident})) END ` +
        `ELSE ${ident} END`
      )
    })
    .join(', ')
  return `json_object(${pairs})`
}

function escId(name: string): string {
  return name.replace(/"/g, '""')
}

function escStr(name: string): string {
  return name.replace(/'/g, "''")
}
