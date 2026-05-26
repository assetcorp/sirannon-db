import type { SQLiteConnection } from '../driver/types.js'
import { SAFE_INT_BOUND_TEXT } from './encoding.js'

/**
 * Helpers that compile the CDC trigger SQL for a watched table and
 * install/drop those triggers on a supplied connection.
 *
 * The trigger SQL bakes the watched table's column list into a static
 * `json_object(...)` expression for `new_data`/`old_data`. SQLite recompiles
 * triggers when their target schema changes, but the column list inside the
 * trigger body is fixed at create time — re-installation is the only way to
 * pick up a new column after `ALTER TABLE ... ADD COLUMN`.
 *
 * These functions accept a bare connection and perform no transaction
 * management of their own so that callers inside an active `BEGIN`/`COMMIT`
 * can re-install triggers in place without nesting another transaction.
 */
export async function dropCdcTriggers(conn: SQLiteConnection, table: string): Promise<void> {
  await conn.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_insert"`)
  await conn.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_update"`)
  await conn.exec(`DROP TRIGGER IF EXISTS "_sirannon_trg_${table}_delete"`)
}

export async function installCdcTriggers(
  conn: SQLiteConnection,
  changesTable: string,
  table: string,
  columns: string[],
  pkColumns: string[],
  replication: boolean,
): Promise<void> {
  const newJson = buildJsonObject(columns, 'NEW')
  const oldJson = buildJsonObject(columns, 'OLD')
  const newPk = buildPkRef(pkColumns, 'NEW')
  const oldPk = buildPkRef(pkColumns, 'OLD')

  const replCols = replication ? ', node_id, tx_id, hlc' : ''
  const replVals = replication ? ", '', '', ''" : ''

  await conn.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_insert"
			AFTER INSERT ON "${table}"
			BEGIN
				INSERT INTO "${changesTable}" (table_name, operation, row_id, new_data${replCols})
				VALUES ('${table}', 'INSERT', ${newPk}, ${newJson}${replVals});
			END
		`)

  await conn.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_update"
			AFTER UPDATE ON "${table}"
			BEGIN
				INSERT INTO "${changesTable}" (table_name, operation, row_id, old_data, new_data${replCols})
				VALUES ('${table}', 'UPDATE', ${newPk}, ${oldJson}, ${newJson}${replVals});
			END
		`)

  await conn.exec(`
			CREATE TRIGGER IF NOT EXISTS "_sirannon_trg_${table}_delete"
			AFTER DELETE ON "${table}"
			BEGIN
				INSERT INTO "${changesTable}" (table_name, operation, row_id, old_data${replCols})
				VALUES ('${table}', 'DELETE', ${oldPk}, ${oldJson}${replVals});
			END
		`)
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
