import type { SQLiteConnection } from '../driver/types.js'
import { CDC_TRIGGER_PREFIX, REPLACED_ROWS_TABLE } from '../internal-tables.js'
import { selectTriggerSql } from '../system-catalog/index.js'
import type { CaptureShape } from './capture-shape.js'
import {
  type ConflictSql,
  conflictSql,
  escId,
  keyChangedCondition,
  keyUpdateColumnList,
  pkRef,
  rowKey,
} from './conflict-sql.js'
import { SAFE_INT_BOUND_TEXT } from './encoding.js'

const FULL_PRECISION_REAL_FORMAT = '%!.17g'
const POSITIVE_INFINITY_LITERAL = '9e999'
const TRIGGER_SUFFIXES = ['insert', 'update', 'delete', 'insert_conflicts', 'update_conflicts'] as const
const STAMP_COLUMNS = ', node_id, tx_id, hlc'
const STAMP_VALUES = ", '', '', ''"
const STAGED = `"${REPLACED_ROWS_TABLE}"`

interface CdcTriggerDefinition {
  name: string
  body: string
}

interface TriggerContext {
  changesTable: string
  table: string
  shape: CaptureShape
  conflicts: ConflictSql
}

/**
 * Drops a table's change-capture triggers, skipping each one that does not exist.
 *
 * @param conn - The writer connection on which to drop the triggers.
 * @param table - The table whose triggers to drop.
 */
export async function dropCdcTriggers(conn: SQLiteConnection, table: string): Promise<void> {
  for (const suffix of TRIGGER_SUFFIXES) {
    await conn.exec(`DROP TRIGGER IF EXISTS "${CDC_TRIGGER_PREFIX}${table}_${suffix}"`)
  }
}

/**
 * Creates the triggers that record each write to a table in the change log, including each row that a `REPLACE` conflict resolution deletes, and leaves in place each trigger that already exists under the same name.
 *
 * @param conn - The writer connection on which to create the triggers.
 * @param changesTable - The change log that the triggers write to.
 * @param table - The watched table.
 * @param shape - The columns, row identity, and unique keys of the table.
 */
export async function installCdcTriggers(
  conn: SQLiteConnection,
  changesTable: string,
  table: string,
  shape: CaptureShape,
): Promise<void> {
  for (const trigger of cdcTriggerDefinitions(changesTable, table, shape)) {
    await conn.exec(`CREATE TRIGGER IF NOT EXISTS ${trigger.body}`)
  }
}

/**
 * Returns whether the database holds every change-capture trigger for a table exactly as {@link installCdcTriggers} would create it now, so that triggers left by an earlier session with other columns or older SQL can be replaced.
 *
 * @param conn - The connection to read the schema through.
 * @param changesTable - The change log that the triggers write to.
 * @param table - The watched table.
 * @param shape - The columns, row identity, and unique keys of the table.
 * @returns `true` when every trigger exists with the expected statement.
 */
export async function cdcTriggersMatch(
  conn: SQLiteConnection,
  changesTable: string,
  table: string,
  shape: CaptureShape,
): Promise<boolean> {
  for (const trigger of cdcTriggerDefinitions(changesTable, table, shape)) {
    const stored = await selectTriggerSql(conn, trigger.name)
    if (stored === null || stored.trim() !== `CREATE TRIGGER ${trigger.body}`) {
      return false
    }
  }
  return true
}

function cdcTriggerDefinitions(changesTable: string, table: string, shape: CaptureShape): CdcTriggerDefinition[] {
  const context: TriggerContext = { changesTable, table, shape, conflicts: conflictSql(table, shape) }
  const name = (suffix: (typeof TRIGGER_SUFFIXES)[number]) => `${CDC_TRIGGER_PREFIX}${table}_${suffix}`
  const changes = `INSERT INTO "${changesTable}"`
  const newJson = buildJsonObject(shape.columns, 'NEW')
  const oldJson = buildJsonObject(shape.columns, 'OLD')
  const newPk = pkRef(shape.pkColumns, 'NEW')
  const oldPk = pkRef(shape.pkColumns, 'OLD')
  const source = `"${table}"`
  const otherThanOld = ` AND ${rowKey(shape, source)} IS NOT ${rowKey(shape, 'OLD')}`
  const keyChanged = `(${keyChangedCondition(table, shape)})`

  return [
    {
      name: name('insert_conflicts'),
      body: triggerBody(
        name('insert_conflicts'),
        `BEFORE INSERT ON ${source}\nWHEN ${anyConflict(context, '')}`,
        stageReplacedRows(context, ''),
      ),
    },
    {
      name: name('update_conflicts'),
      body: triggerBody(
        name('update_conflicts'),
        `BEFORE UPDATE OF ${keyUpdateColumnList(table, shape)} ON ${source}\nWHEN ${keyChanged} AND (${anyConflict(context, otherThanOld)})`,
        stageReplacedRows(context, otherThanOld),
      ),
    },
    {
      name: name('insert'),
      body: triggerBody(name('insert'), `AFTER INSERT ON ${source}`, [
        ...logReplacedRows(context, ''),
        `${changes} (table_name, operation, row_id, new_data${STAMP_COLUMNS})
VALUES ('${table}', 'INSERT', ${newPk}, ${newJson}${STAMP_VALUES});`,
      ]),
    },
    {
      name: name('update'),
      body: triggerBody(name('update'), `AFTER UPDATE ON ${source}`, [
        `DELETE FROM ${STAGED} WHERE table_name = '${table}' AND row_key = ${rowKey(shape, 'OLD')};`,
        ...logReplacedRows(context, `${keyChanged} AND `),
        ...updateEvents(changes, table, shape, { newJson, oldJson, newPk }),
      ]),
    },
    {
      name: name('delete'),
      body: triggerBody(name('delete'), `AFTER DELETE ON ${source}`, [
        `${changes} (table_name, operation, row_id, old_data${STAMP_COLUMNS})
VALUES ('${table}', 'DELETE', ${oldPk}, ${oldJson}${STAMP_VALUES});`,
        `DELETE FROM ${STAGED} WHERE table_name = '${table}' AND row_key = ${rowKey(shape, 'OLD')};`,
      ]),
    },
  ]
}

function triggerBody(name: string, timing: string, statements: readonly string[]): string {
  return `"${name}"\n${timing}\nBEGIN\n${statements.join('\n')}\nEND`
}

function anyConflict(context: TriggerContext, extra: string): string {
  return context.conflicts.matches
    .map(match => `EXISTS (SELECT 1 FROM "${context.table}" WHERE ${match}${extra})`)
    .join(' OR ')
}

function stageReplacedRows(context: TriggerContext, extra: string): string[] {
  const { table, shape } = context
  const source = `"${table}"`
  const token = context.conflicts.token ?? "''"
  return context.conflicts.matches.map(
    match => `INSERT OR REPLACE INTO ${STAGED} (table_name, row_key, match_key, row_id, old_data)
SELECT '${table}', ${rowKey(shape, source)}, ${token}, ${pkRef(shape.pkColumns, source)}, ${buildJsonObject(shape.columns, source)}
FROM ${source} WHERE ${match}${extra};`,
  )
}

function logReplacedRows(context: TriggerContext, guard: string): string[] {
  const { changesTable, table, shape } = context
  const source = `"${table}"`
  const newKey = rowKey(shape, 'NEW')
  const token = context.conflicts.token
  const ofWrite = (ref: string) =>
    token === null
      ? `${ref}.table_name = '${table}' AND ${ref}.match_key = '' AND ${ref}.row_key = ${newKey}`
      : `${ref}.table_name = '${table}' AND ${ref}.match_key = ${token}`
  const removed =
    token === null
      ? ''
      : `\nAND (staged.row_key = ${newKey} OR NOT EXISTS (SELECT 1 FROM ${source} WHERE ${rowKey(shape, source)} = staged.row_key))`
  return [
    `INSERT INTO "${changesTable}" (table_name, operation, row_id, old_data${STAMP_COLUMNS})
SELECT '${table}', 'DELETE', staged.row_id, staged.old_data${STAMP_VALUES}
FROM ${STAGED} AS staged
WHERE ${guard}${ofWrite('staged')}${removed};`,
    `DELETE FROM ${STAGED} WHERE ${guard}${ofWrite(STAGED)};`,
  ]
}

function updateEvents(
  changes: string,
  table: string,
  shape: CaptureShape,
  images: { newJson: string; oldJson: string; newPk: string },
): string[] {
  const { newJson, oldJson, newPk } = images
  if (shape.pkColumns.length > 0) {
    return [
      `${changes} (table_name, operation, row_id, old_data, new_data${STAMP_COLUMNS})
VALUES ('${table}', 'UPDATE', ${newPk}, ${oldJson}, ${newJson}${STAMP_VALUES});`,
    ]
  }
  return [
    `${changes} (table_name, operation, row_id, old_data, new_data${STAMP_COLUMNS})
SELECT '${table}', 'UPDATE', NEW.rowid, ${oldJson}, ${newJson}${STAMP_VALUES} WHERE OLD.rowid = NEW.rowid;`,
    `${changes} (table_name, operation, row_id, old_data${STAMP_COLUMNS})
SELECT '${table}', 'DELETE', OLD.rowid, ${oldJson}${STAMP_VALUES} WHERE OLD.rowid <> NEW.rowid;`,
    `${changes} (table_name, operation, row_id, new_data${STAMP_COLUMNS})
SELECT '${table}', 'INSERT', NEW.rowid, ${newJson}${STAMP_VALUES} WHERE OLD.rowid <> NEW.rowid;`,
  ]
}

function buildJsonObject(columns: string[], ref: string): string {
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

function escStr(name: string): string {
  return name.replace(/'/g, "''")
}
