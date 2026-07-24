import type { SQLiteConnection } from '../driver/types.js'
import { COLUMN_VERSIONS_TABLE } from '../internal-tables.js'
import type { PkResolver } from './pk.js'
import { findRowByPk } from './row-lookup.js'
import type { ReplicationChange } from './types.js'
import { IDENTIFIER_RE, validateIdentifier } from './validators.js'

export class RowWriter {
  constructor(
    private readonly pkResolver: PkResolver,
    private readonly changesTable: string,
  ) {}

  async findExistingRow(tx: SQLiteConnection, change: ReplicationChange): Promise<Record<string, unknown> | undefined> {
    if (!IDENTIFIER_RE.test(change.table)) return undefined

    const pkColumns = await this.pkResolver.forTable(change.table)

    const result = await findRowByPk(tx, change.table, pkColumns, change.newData ?? change.oldData ?? {})
    if (result) return result

    if (change.operation === 'update' && change.oldData) {
      return findRowByPk(tx, change.table, pkColumns, change.oldData)
    }

    return undefined
  }

  async getLocalHlcForRow(tx: SQLiteConnection, table: string, rowId: string): Promise<string | null> {
    const stmt = await tx.prepare(
      `SELECT MAX(hlc) as max_hlc FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`,
    )
    const row = (await stmt.get(table, rowId)) as { max_hlc: string | null } | undefined
    if (row?.max_hlc) return row.max_hlc

    const logStmt = await tx.prepare(
      `SELECT MAX(hlc) as max_hlc FROM "${this.changesTable}" WHERE table_name = ? AND row_id = ? AND hlc != ''`,
    )
    const logRow = (await logStmt.get(table, rowId)) as { max_hlc: string | null } | undefined
    return logRow?.max_hlc ?? null
  }

  async insertRow(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    if (!change.newData) return

    const columns = Object.keys(change.newData).filter(validateIdentifier)
    if (columns.length === 0) return

    const placeholders = columns.map(() => '?').join(', ')
    const colNames = columns.map(c => `"${c}"`).join(', ')
    const values = columns.map(c => change.newData?.[c])

    const stmt = await tx.prepare(`INSERT INTO "${change.table}" (${colNames}) VALUES (${placeholders})`)
    await stmt.run(...values)
  }

  async applyRemoteChange(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    if (change.operation === 'delete') {
      await this.deleteRow(tx, change)
      return
    }

    if (!change.newData) return

    const pkColumns = await this.pkResolver.forTable(change.table)
    const sourceData = change.newData
    const wherePkSource = change.oldData ?? sourceData

    const setClauses: string[] = []
    const setValues: unknown[] = []
    const whereConditions: string[] = []
    const whereValues: unknown[] = []

    const pkSet = new Set(pkColumns)

    for (const [col, val] of Object.entries(sourceData)) {
      if (!validateIdentifier(col)) continue
      if (pkSet.has(col)) continue
      setClauses.push(`"${col}" = ?`)
      setValues.push(val)
    }

    for (const col of pkColumns) {
      if (!validateIdentifier(col)) continue
      whereConditions.push(`"${col}" = ?`)
      whereValues.push(wherePkSource[col])
    }

    if (setClauses.length === 0 || whereConditions.length === 0) return

    const stmt = await tx.prepare(
      `UPDATE "${change.table}" SET ${setClauses.join(', ')} WHERE ${whereConditions.join(' AND ')}`,
    )
    await stmt.run(...setValues, ...whereValues)
  }

  async applyMergedData(
    tx: SQLiteConnection,
    change: ReplicationChange,
    mergedData: Record<string, unknown>,
  ): Promise<void> {
    const pkColumns = await this.pkResolver.forTable(change.table)
    const sourceData = change.newData ?? change.oldData ?? {}

    const setClauses: string[] = []
    const setValues: unknown[] = []
    const whereConditions: string[] = []
    const whereValues: unknown[] = []

    for (const [col, val] of Object.entries(mergedData)) {
      if (!validateIdentifier(col)) continue
      if (!pkColumns.includes(col)) {
        setClauses.push(`"${col}" = ?`)
        setValues.push(val)
      }
    }

    for (const col of pkColumns) {
      if (!validateIdentifier(col)) continue
      whereConditions.push(`"${col}" = ?`)
      whereValues.push(sourceData[col])
    }

    if (setClauses.length === 0 || whereConditions.length === 0) return

    const stmt = await tx.prepare(
      `UPDATE "${change.table}" SET ${setClauses.join(', ')} WHERE ${whereConditions.join(' AND ')}`,
    )
    await stmt.run(...setValues, ...whereValues)
  }

  async deleteRow(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    const pkColumns = await this.pkResolver.forTable(change.table)
    const sourceData = change.oldData ?? change.newData ?? {}

    const conditions: string[] = []
    const values: unknown[] = []

    for (const col of pkColumns) {
      if (!validateIdentifier(col)) continue
      conditions.push(`"${col}" = ?`)
      values.push(sourceData[col])
    }

    if (conditions.length === 0) return

    const stmt = await tx.prepare(`DELETE FROM "${change.table}" WHERE ${conditions.join(' AND ')}`)
    await stmt.run(...values)
  }

  async recordColumnVersions(
    tx: SQLiteConnection,
    change: ReplicationChange,
    data: Record<string, unknown> | null,
  ): Promise<void> {
    if (change.operation === 'delete') {
      const delStmt = await tx.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`)
      await delStmt.run(change.table, change.rowId)
      return
    }

    if (!data) return

    const upsertStmt = await tx.prepare(
      `INSERT INTO ${COLUMN_VERSIONS_TABLE} (table_name, row_id, column_name, hlc, node_id)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(table_name, row_id, column_name)
       DO UPDATE SET hlc = excluded.hlc, node_id = excluded.node_id`,
    )

    for (const col of Object.keys(data)) {
      if (!validateIdentifier(col)) continue
      await upsertStmt.run(change.table, change.rowId, col, change.hlc, change.nodeId)
    }
  }
}
