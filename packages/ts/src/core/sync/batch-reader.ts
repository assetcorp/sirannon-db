import { decodeTaggedValues } from '../cdc/encoding.js'
import type { SQLiteConnection } from '../driver/types.js'
import { selectOutboundChangesSql, selectOutboundHistoryChangesSql } from '../system-catalog/index.js'
import { computeChecksum } from './checksum.js'
import { HLC } from './hlc.js'
import type { ChangeRow } from './internal-types.js'
import type { PkResolver } from './pk.js'
import type { ReplicationBatch, ReplicationChange } from './types.js'

export class BatchReader {
  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly changesTable: string,
    private readonly pkResolver: PkResolver,
    private readonly excludeDdl = false,
    private readonly describeTransactions = false,
  ) {}

  async readBatch(afterSeq: bigint, batchSize: number, origins?: readonly string[]): Promise<ReplicationBatch | null> {
    const limit = this.describeTransactions ? batchSize + 1 : batchSize
    const rows = (
      origins
        ? await (await this.conn.prepare(selectOutboundHistoryChangesSql(this.changesTable, origins.length))).all(
            afterSeq.toString(),
            ...origins,
            limit,
          )
        : await (await this.conn.prepare(selectOutboundChangesSql(this.changesTable, this.excludeDdl))).all(
            afterSeq.toString(),
            this.localNodeId,
            limit,
          )
    ) as ChangeRow[]
    const next = rows.length > batchSize ? rows.pop() : undefined

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

    const batch: ReplicationBatch = {
      sourceNodeId: this.localNodeId,
      batchId: `${this.localNodeId}-${fromSeq}-${toSeq}`,
      fromSeq,
      toSeq,
      hlcRange: { min: minHlc, max: maxHlc },
      changes,
      checksum,
    }
    if (this.describeTransactions) {
      batch.changeSeqs = rows.map(row => BigInt(row.seq))
      if (next !== undefined && next.tx_id !== '' && next.tx_id === rows[rows.length - 1].tx_id) {
        batch.txContinues = true
      }
    }
    return batch
  }
}
