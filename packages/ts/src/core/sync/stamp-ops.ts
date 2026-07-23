import { decodeTaggedValues } from '../cdc/encoding.js'
import type { SQLiteConnection } from '../driver/types.js'
import { COLUMN_VERSIONS_TABLE } from '../internal-tables.js'
import { setMetaValue } from '../system-catalog/index.js'
import { canonicaliseForChecksum } from './canonicalise.js'
import type { HLC } from './hlc.js'
import { HLC_CLOCK_META_KEY } from './hlc-store.js'
import type { ChangeRow } from './internal-types.js'

export class StampOps {
  constructor(
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    private readonly changesTable: string,
  ) {}

  async stampChanges(tx: SQLiteConnection, afterSeq: bigint, txId: string): Promise<void> {
    const hlcValue = this.hlc.now()
    const stmt = await tx.prepare(
      `UPDATE "${this.changesTable}" SET node_id = ?, tx_id = ?, hlc = ? WHERE seq > ? AND node_id = ''`,
    )
    await stmt.run(this.localNodeId, txId, hlcValue, afterSeq.toString())
    await setMetaValue(tx, HLC_CLOCK_META_KEY, hlcValue)
  }

  async updateColumnVersions(tx: SQLiteConnection, afterSeq: bigint): Promise<void> {
    const selectStmt = await tx.prepare(
      `SELECT seq, table_name, operation, row_id, old_data, new_data, hlc, node_id
       FROM "${this.changesTable}"
       WHERE seq > ? AND node_id = ?
       ORDER BY seq ASC`,
    )
    const rows = (await selectStmt.all(afterSeq.toString(), this.localNodeId)) as ChangeRow[]

    for (const row of rows) {
      if (row.operation === 'DELETE') {
        const delStmt = await tx.prepare(`DELETE FROM ${COLUMN_VERSIONS_TABLE} WHERE table_name = ? AND row_id = ?`)
        await delStmt.run(row.table_name, String(row.row_id))
        continue
      }

      const oldData = row.old_data ? (decodeTaggedValues(JSON.parse(row.old_data)) as Record<string, unknown>) : {}
      const newData = row.new_data ? (decodeTaggedValues(JSON.parse(row.new_data)) as Record<string, unknown>) : {}
      const changedCols: string[] = []

      if (row.operation === 'INSERT') {
        for (const key of Object.keys(newData)) {
          changedCols.push(key)
        }
      } else {
        for (const key of Object.keys(newData)) {
          if (canonicaliseForChecksum(newData[key]) !== canonicaliseForChecksum(oldData[key])) {
            changedCols.push(key)
          }
        }
      }

      const upsertStmt = await tx.prepare(
        `INSERT INTO ${COLUMN_VERSIONS_TABLE} (table_name, row_id, column_name, hlc, node_id)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(table_name, row_id, column_name)
         DO UPDATE SET hlc = excluded.hlc, node_id = excluded.node_id`,
      )

      for (const col of changedCols) {
        await upsertStmt.run(row.table_name, String(row.row_id), col, row.hlc, row.node_id)
      }
    }
  }
}
