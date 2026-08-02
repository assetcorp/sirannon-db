import { decodeTaggedValues } from '../cdc/encoding.js'
import type { SQLiteConnection } from '../driver/types.js'
import {
  prepareDeleteRowColumnVersionsUpToHlc,
  prepareUpsertNewerColumnVersion,
  selectNodeChangesAfterSeqSql,
  updateChangeStampsAfterSeqSql,
  upsertMetaValue,
} from '../system-catalog/index.js'
import { canonicaliseForChecksum } from './canonicalise.js'
import type { HLC } from './hlc.js'
import { HLC_CLOCK_META_KEY } from './hlc-store.js'
import type { ChangeRow } from './internal-types.js'

export async function recordLocalColumnVersions(
  tx: SQLiteConnection,
  changesTable: string,
  localNodeId: string,
  afterSeq: bigint,
): Promise<void> {
  const selectStmt = await tx.prepare(selectNodeChangesAfterSeqSql(changesTable))
  const rows = (await selectStmt.all(afterSeq.toString(), localNodeId)) as ChangeRow[]

  for (const row of rows) {
    if (row.operation === 'DELETE') {
      const delStmt = await prepareDeleteRowColumnVersionsUpToHlc(tx)
      await delStmt.run(row.table_name, String(row.row_id), row.hlc)
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

    const upsertStmt = await prepareUpsertNewerColumnVersion(tx)

    for (const col of changedCols) {
      await upsertStmt.run(row.table_name, String(row.row_id), col, row.hlc, row.node_id)
    }
  }
}

export class StampOps {
  constructor(
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    private readonly changesTable: string,
  ) {}

  async stampChanges(tx: SQLiteConnection, afterSeq: bigint, txId: string): Promise<void> {
    const hlcValue = this.hlc.now()
    const stmt = await tx.prepare(updateChangeStampsAfterSeqSql(this.changesTable))
    await stmt.run(this.localNodeId, txId, hlcValue, afterSeq.toString())
    await upsertMetaValue(tx, HLC_CLOCK_META_KEY, hlcValue)
  }

  updateColumnVersions(tx: SQLiteConnection, afterSeq: bigint): Promise<void> {
    return recordLocalColumnVersions(tx, this.changesTable, this.localNodeId, afterSeq)
  }
}
