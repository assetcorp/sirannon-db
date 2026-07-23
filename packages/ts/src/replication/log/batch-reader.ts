import { decodeTaggedValues } from '../../core/cdc/encoding.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { computeChecksum } from '../../core/sync/checksum.js'
import { HLC } from '../../core/sync/hlc.js'
import type { ChangeRow } from '../../core/sync/internal-types.js'
import type { PkResolver } from '../../core/sync/pk.js'
import type { ReplicationBatch, ReplicationChange } from '../types.js'

export class BatchReader {
  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly changesTable: string,
    private readonly pkResolver: PkResolver,
  ) {}

  async readBatch(afterSeq: bigint, batchSize: number): Promise<ReplicationBatch | null> {
    const stmt = await this.conn.prepare(
      `SELECT seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, tx_id, hlc
       FROM "${this.changesTable}"
       WHERE seq > ? AND node_id = ?
       ORDER BY seq ASC
       LIMIT ?`,
    )
    const rows = (await stmt.all(afterSeq.toString(), this.localNodeId, batchSize)) as ChangeRow[]

    if (rows.length === 0) {
      return null
    }

    const changes: ReplicationChange[] = []
    let minHlc = rows[0].hlc
    let maxHlc = rows[0].hlc

    for (const row of rows) {
      const operation = row.operation.toLowerCase() as ReplicationChange['operation']
      const isDdl = operation === 'ddl'
      const rawNewData = row.new_data ? (decodeTaggedValues(JSON.parse(row.new_data)) as Record<string, unknown>) : null
      const rawOldData = row.old_data ? (decodeTaggedValues(JSON.parse(row.old_data)) as Record<string, unknown>) : null

      let ddlStatement: string | undefined
      let newData: Record<string, unknown> | null = rawNewData
      let oldData: Record<string, unknown> | null = rawOldData
      if (isDdl) {
        const candidate = rawNewData?.ddlStatement
        if (typeof candidate === 'string') {
          ddlStatement = candidate
        }
        newData = null
        oldData = null
      }

      const pkColumns = isDdl ? [] : await this.pkResolver.forTable(row.table_name)

      const primaryKey: Record<string, unknown> = {}
      if (!isDdl) {
        const sourceData = rawNewData ?? rawOldData ?? {}
        for (const col of pkColumns) {
          if (col in sourceData) {
            primaryKey[col] = sourceData[col]
          }
        }
      }

      const change: ReplicationChange = {
        table: row.table_name,
        operation,
        rowId: String(row.row_id),
        primaryKey,
        hlc: row.hlc,
        txId: row.tx_id,
        nodeId: row.node_id,
        newData,
        oldData,
      }
      if (ddlStatement !== undefined) {
        change.ddlStatement = ddlStatement
      }
      changes.push(change)

      if (HLC.compare(row.hlc, minHlc) < 0) {
        minHlc = row.hlc
      }
      if (HLC.compare(row.hlc, maxHlc) > 0) {
        maxHlc = row.hlc
      }
    }

    const fromSeq = BigInt(rows[0].seq)
    const toSeq = BigInt(rows[rows.length - 1].seq)
    const checksum = computeChecksum(changes)

    return {
      sourceNodeId: this.localNodeId,
      batchId: `${this.localNodeId}-${fromSeq}-${toSeq}`,
      fromSeq,
      toSeq,
      hlcRange: { min: minHlc, max: maxHlc },
      changes,
      checksum,
    }
  }
}
