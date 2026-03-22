import { createHash } from 'node:crypto'
import type { SQLiteConnection } from '../core/driver/types.js'
import { BatchValidationError, ReplicationError } from './errors.js'
import { HLC } from './hlc.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, ReplicationChange } from './types.js'

const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

const SAFE_DDL_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

function validateIdentifier(name: string): boolean {
  return IDENTIFIER_RE.test(name)
}

function validateDdlSafety(sql: string): boolean {
  if (!SAFE_DDL_RE.test(sql)) return false
  if (sql.includes(';')) return false
  return true
}

interface ChangeRow {
  seq: number
  table_name: string
  operation: string
  row_id: string
  changed_at: number
  old_data: string | null
  new_data: string | null
  node_id: string
  tx_id: string
  hlc: string
}

interface ColumnInfoRow {
  name: string
  pk: number
}

/**
 * Persistent change log that bridges CDC events and the replication protocol.
 *
 * On the outbound side, `readBatch` reads a window of local changes from the
 * `_sirannon_changes` table (populated by the CDC triggers) and packages them
 * into a signed ReplicationBatch ready for transport.
 *
 * On the inbound side, `applyBatch` validates an incoming batch (checksum,
 * table names, deduplication via `_sirannon_applied_changes`), groups changes
 * by transaction ID, and applies each group inside an atomic SQLite
 * transaction. Row-level conflicts are delegated to the caller-supplied
 * ConflictResolver (or resolver-lookup function for per-table policies).
 * After each applied change, the per-column version table
 * `_sirannon_column_versions` is updated so that future conflict comparisons
 * use fresh HLC metadata.
 *
 * Helper methods `stampChanges` and `updateColumnVersions` are called by the
 * ReplicationEngine after local writes to attach HLC timestamps and record
 * per-column version info before the changes are shipped to peers.
 */
export class ReplicationLog {
  private readonly pkCache = new Map<string, string[]>()

  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    private readonly changesTable: string = '_sirannon_changes',
  ) {}

  async ensureReplicationTables(): Promise<void> {
    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_peer_state (
	peer_node_id TEXT PRIMARY KEY,
	last_acked_seq INTEGER NOT NULL DEFAULT 0,
	last_received_hlc TEXT NOT NULL DEFAULT '',
	updated_at REAL NOT NULL
)`)

    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_applied_changes (
	source_node_id TEXT NOT NULL,
	source_seq INTEGER NOT NULL,
	applied_at REAL NOT NULL,
	PRIMARY KEY (source_node_id, source_seq)
)`)

    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_column_versions (
	table_name TEXT NOT NULL,
	row_id TEXT NOT NULL,
	column_name TEXT NOT NULL,
	hlc TEXT NOT NULL,
	node_id TEXT NOT NULL,
	PRIMARY KEY (table_name, row_id, column_name)
)`)
  }

  async readBatch(afterSeq: bigint, batchSize: number): Promise<ReplicationBatch | null> {
    const stmt = await this.conn.prepare(
      `SELECT seq, table_name, operation, row_id, changed_at, old_data, new_data, node_id, tx_id, hlc
			 FROM "${this.changesTable}"
			 WHERE seq > ? AND node_id = ?
			 ORDER BY seq ASC
			 LIMIT ?`,
    )
    const rows = (await stmt.all(Number(afterSeq), this.localNodeId, batchSize)) as ChangeRow[]

    if (rows.length === 0) {
      return null
    }

    const changes: ReplicationChange[] = []
    let minHlc = rows[0].hlc
    let maxHlc = rows[0].hlc

    for (const row of rows) {
      const pkColumns = await this.getPkColumns(row.table_name)
      const newData = row.new_data ? (JSON.parse(row.new_data) as Record<string, unknown>) : null
      const oldData = row.old_data ? (JSON.parse(row.old_data) as Record<string, unknown>) : null

      const primaryKey: Record<string, unknown> = {}
      const sourceData = newData ?? oldData ?? {}
      for (const col of pkColumns) {
        if (col in sourceData) {
          primaryKey[col] = sourceData[col]
        }
      }

      changes.push({
        table: row.table_name,
        operation: row.operation.toLowerCase() as ReplicationChange['operation'],
        rowId: String(row.row_id),
        primaryKey,
        hlc: row.hlc,
        txId: row.tx_id,
        nodeId: row.node_id,
        newData,
        oldData,
      })

      if (HLC.compare(row.hlc, minHlc) < 0) {
        minHlc = row.hlc
      }
      if (HLC.compare(row.hlc, maxHlc) > 0) {
        maxHlc = row.hlc
      }
    }

    const fromSeq = BigInt(rows[0].seq)
    const toSeq = BigInt(rows[rows.length - 1].seq)
    const checksum = this.computeChecksum(changes)

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

  async stampChanges(tx: SQLiteConnection, afterSeq: number, txId: string): Promise<void> {
    const hlcValue = this.hlc.now()
    const stmt = await tx.prepare(
      `UPDATE "${this.changesTable}" SET node_id = ?, tx_id = ?, hlc = ? WHERE seq > ? AND node_id = ''`,
    )
    await stmt.run(this.localNodeId, txId, hlcValue, afterSeq)
  }

  async updateColumnVersions(tx: SQLiteConnection, afterSeq: number): Promise<void> {
    const selectStmt = await tx.prepare(
      `SELECT seq, table_name, operation, row_id, old_data, new_data, hlc, node_id
			 FROM "${this.changesTable}"
			 WHERE seq > ? AND node_id = ?
			 ORDER BY seq ASC`,
    )
    const rows = (await selectStmt.all(afterSeq, this.localNodeId)) as ChangeRow[]

    for (const row of rows) {
      if (row.operation === 'DELETE') {
        const delStmt = await tx.prepare('DELETE FROM _sirannon_column_versions WHERE table_name = ? AND row_id = ?')
        await delStmt.run(row.table_name, String(row.row_id))
        continue
      }

      const oldData = row.old_data ? (JSON.parse(row.old_data) as Record<string, unknown>) : {}
      const newData = row.new_data ? (JSON.parse(row.new_data) as Record<string, unknown>) : {}
      const changedCols: string[] = []

      if (row.operation === 'INSERT') {
        for (const key of Object.keys(newData)) {
          changedCols.push(key)
        }
      } else {
        for (const key of Object.keys(newData)) {
          if (JSON.stringify(newData[key]) !== JSON.stringify(oldData[key])) {
            changedCols.push(key)
          }
        }
      }

      const upsertStmt = await tx.prepare(
        `INSERT INTO _sirannon_column_versions (table_name, row_id, column_name, hlc, node_id)
				 VALUES (?, ?, ?, ?, ?)
				 ON CONFLICT(table_name, row_id, column_name)
				 DO UPDATE SET hlc = excluded.hlc, node_id = excluded.node_id`,
      )

      for (const col of changedCols) {
        await upsertStmt.run(row.table_name, String(row.row_id), col, row.hlc, row.node_id)
      }
    }
  }

  async applyBatch(
    batch: ReplicationBatch,
    resolver: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    const expectedChecksum = this.computeChecksum(batch.changes)
    if (batch.checksum !== expectedChecksum) {
      throw new BatchValidationError(`Checksum mismatch: expected ${expectedChecksum}, got ${batch.checksum}`)
    }

    for (const change of batch.changes) {
      if (change.operation !== 'ddl') {
        if (!IDENTIFIER_RE.test(change.table)) {
          throw new BatchValidationError(`Invalid table name: ${change.table}`)
        }
      }
    }

    const lastApplied = await this.getLastAppliedSeq(batch.sourceNodeId)
    if (batch.toSeq <= lastApplied) {
      return { applied: 0, skipped: batch.changes.length, conflicts: 0 }
    }

    let applied = 0
    let skipped = 0
    let conflicts = 0

    const changesByTx = new Map<string, ReplicationChange[]>()
    for (const change of batch.changes) {
      const txGroup = changesByTx.get(change.txId)
      if (txGroup) {
        txGroup.push(change)
      } else {
        changesByTx.set(change.txId, [change])
      }
    }

    for (const [_txId, txChanges] of changesByTx) {
      const result = await this.conn.transaction(async tx => {
        let txApplied = 0
        let txSkipped = 0
        let txConflicts = 0

        const ddlChanges = txChanges.filter(c => c.operation === 'ddl')
        const dataChanges = txChanges.filter(c => c.operation !== 'ddl')

        for (const ddl of ddlChanges) {
          const ddlSql = ddl.ddlStatement
          if (!ddlSql || !validateDdlSafety(ddlSql)) {
            throw new BatchValidationError(`Unsafe or missing DDL statement: ${ddlSql ?? 'none'}`)
          }
          await tx.exec(ddlSql)
          txApplied += 1
        }

        for (const change of dataChanges) {
          const existingRow = await this.findExistingRow(tx, change)

          if (existingRow === undefined) {
            if (change.operation === 'insert' && change.newData) {
              await this.insertRow(tx, change)
              await this.recordColumnVersions(tx, change, change.newData)
              txApplied += 1
            } else if (change.operation === 'delete') {
              txApplied += 1
            } else {
              txSkipped += 1
            }
          } else {
            txConflicts += 1
            const localHlc = await this.getLocalHlcForRow(tx, change.table, change.rowId)

            const localChange: ReplicationChange = {
              table: change.table,
              operation: 'update',
              rowId: change.rowId,
              primaryKey: change.primaryKey,
              hlc: localHlc ?? '',
              txId: '',
              nodeId: this.localNodeId,
              newData: existingRow,
              oldData: null,
            }

            const changeResolver = typeof resolver === 'function' ? resolver(change.table) : resolver
            const resolution = await changeResolver.resolve({
              table: change.table,
              rowId: change.rowId,
              localChange,
              remoteChange: change,
              localHlc,
              remoteHlc: change.hlc,
            })

            if (resolution.action === 'accept_remote') {
              await this.applyRemoteChange(tx, change)
              await this.recordColumnVersions(tx, change, change.newData)
              txApplied += 1
            } else if (resolution.action === 'merge' && resolution.mergedData) {
              await this.applyMergedData(tx, change, resolution.mergedData)
              await this.recordColumnVersions(tx, change, resolution.mergedData)
              txApplied += 1
            } else {
              txSkipped += 1
            }
          }
        }

        return { txApplied, txSkipped, txConflicts }
      })

      applied += result.txApplied
      skipped += result.txSkipped
      conflicts += result.txConflicts
    }

    const recordStmt = await this.conn.prepare(
      'INSERT OR IGNORE INTO _sirannon_applied_changes (source_node_id, source_seq, applied_at) VALUES (?, ?, ?)',
    )
    const nowSec = Date.now() / 1000
    for (let seq = batch.fromSeq; seq <= batch.toSeq; seq += 1n) {
      await recordStmt.run(batch.sourceNodeId, Number(seq), nowSec)
    }

    try {
      this.hlc.receive(batch.hlcRange.max)
    } catch {
      /* batch is already durable; HLC merge failure should not surface as a batch error */
    }

    return { applied, skipped, conflicts }
  }

  async getLastAppliedSeq(fromNodeId: string): Promise<bigint> {
    const stmt = await this.conn.prepare(
      'SELECT MAX(source_seq) as max_seq FROM _sirannon_applied_changes WHERE source_node_id = ?',
    )
    const row = (await stmt.get(fromNodeId)) as { max_seq: number | null } | undefined
    if (!row || row.max_seq === null) {
      return 0n
    }
    return BigInt(row.max_seq)
  }

  async setLastAppliedSeq(fromNodeId: string, seq: bigint): Promise<void> {
    const stmt = await this.conn.prepare(
      `INSERT INTO _sirannon_peer_state (peer_node_id, last_acked_seq, updated_at)
			 VALUES (?, ?, ?)
			 ON CONFLICT(peer_node_id)
			 DO UPDATE SET last_acked_seq = excluded.last_acked_seq, updated_at = excluded.updated_at`,
    )
    await stmt.run(fromNodeId, Number(seq), Date.now() / 1000)
  }

  async getLocalSeq(): Promise<bigint> {
    const stmt = await this.conn.prepare(`SELECT MAX(seq) as max_seq FROM "${this.changesTable}"`)
    const row = (await stmt.get()) as { max_seq: number | null } | undefined
    if (!row || row.max_seq === null) {
      return 0n
    }
    return BigInt(row.max_seq)
  }

  async getMinAckedSeq(): Promise<bigint> {
    const stmt = await this.conn.prepare('SELECT MIN(last_acked_seq) as min_seq FROM _sirannon_peer_state')
    const row = (await stmt.get()) as { min_seq: number | null } | undefined
    if (!row || row.min_seq === null) {
      return 0n
    }
    return BigInt(row.min_seq)
  }

  async *dumpTable(table: string, batchSize: number): AsyncGenerator<ReplicationBatch> {
    if (!IDENTIFIER_RE.test(table)) {
      throw new ReplicationError(`Invalid table name: ${table}`)
    }

    const countStmt = await this.conn.prepare(`SELECT COUNT(*) as cnt FROM "${table}"`)
    const countRow = (await countStmt.get()) as { cnt: number } | undefined
    const total = countRow?.cnt ?? 0

    if (total === 0) return

    const pkColumns = await this.getPkColumns(table)

    let offset = 0
    let batchNum = 0

    const needsRowid = pkColumns.length === 1 && pkColumns[0] === 'rowid'

    while (offset < total) {
      const selectSql = needsRowid
        ? `SELECT rowid, * FROM "${table}" LIMIT ? OFFSET ?`
        : `SELECT * FROM "${table}" LIMIT ? OFFSET ?`
      const selectStmt = await this.conn.prepare(selectSql)
      const rows = (await selectStmt.all(batchSize, offset)) as Record<string, unknown>[]

      if (rows.length === 0) break

      const changes: ReplicationChange[] = []
      const hlcValue = this.hlc.now()

      for (const row of rows) {
        const primaryKey: Record<string, unknown> = {}
        for (const col of pkColumns) {
          if (col in row) {
            primaryKey[col] = row[col]
          }
        }

        const rowId = pkColumns.map(col => String(row[col] ?? '')).join('-')

        changes.push({
          table,
          operation: 'insert',
          rowId,
          primaryKey,
          hlc: hlcValue,
          txId: `dump-${table}-${batchNum}`,
          nodeId: this.localNodeId,
          newData: row,
          oldData: null,
        })
      }

      const checksum = this.computeChecksum(changes)
      const fromSeq = BigInt(offset)
      const toSeq = BigInt(offset + rows.length - 1)

      const dumpNodeId = `dump-${this.localNodeId}`

      yield {
        sourceNodeId: dumpNodeId,
        batchId: `dump-${table}-${batchNum}`,
        fromSeq,
        toSeq,
        hlcRange: { min: hlcValue, max: hlcValue },
        changes,
        checksum,
      }

      offset += rows.length
      batchNum += 1
    }
  }

  private async getPkColumns(table: string): Promise<string[]> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const stmt = await this.conn.prepare(`PRAGMA table_info("${table}")`)
    const info = (await stmt.all()) as ColumnInfoRow[]
    const pkCols = info
      .filter(col => col.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map(col => col.name)

    if (pkCols.length === 0) {
      pkCols.push('rowid')
    }

    this.pkCache.set(table, pkCols)
    return pkCols
  }

  private computeChecksum(changes: ReplicationChange[]): string {
    const hash = createHash('sha256')
    hash.update(JSON.stringify(changes))
    return hash.digest('hex')
  }

  private async findExistingRow(
    tx: SQLiteConnection,
    change: ReplicationChange,
  ): Promise<Record<string, unknown> | undefined> {
    if (!IDENTIFIER_RE.test(change.table)) return undefined

    const pkColumns = await this.getPkColumns(change.table)

    const result = await this.findRowByPk(tx, change.table, pkColumns, change.newData ?? change.oldData ?? {})
    if (result) return result

    if (change.operation === 'update' && change.oldData) {
      return this.findRowByPk(tx, change.table, pkColumns, change.oldData)
    }

    return undefined
  }

  private async findRowByPk(
    tx: SQLiteConnection,
    table: string,
    pkColumns: string[],
    sourceData: Record<string, unknown>,
  ): Promise<Record<string, unknown> | undefined> {
    const conditions: string[] = []
    const values: unknown[] = []

    for (const col of pkColumns) {
      if (!validateIdentifier(col)) return undefined
      if (!(col in sourceData)) return undefined
      conditions.push(`"${col}" = ?`)
      values.push(sourceData[col])
    }

    if (conditions.length === 0) return undefined

    const stmt = await tx.prepare(`SELECT * FROM "${table}" WHERE ${conditions.join(' AND ')} LIMIT 1`)
    return stmt.get<Record<string, unknown>>(...values)
  }

  private async getLocalHlcForRow(tx: SQLiteConnection, table: string, rowId: string): Promise<string | null> {
    const stmt = await tx.prepare(
      'SELECT MAX(hlc) as max_hlc FROM _sirannon_column_versions WHERE table_name = ? AND row_id = ?',
    )
    const row = (await stmt.get(table, rowId)) as { max_hlc: string | null } | undefined
    return row?.max_hlc ?? null
  }

  private async insertRow(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    if (!change.newData) return

    const columns = Object.keys(change.newData).filter(validateIdentifier)
    if (columns.length === 0) return

    const placeholders = columns.map(() => '?').join(', ')
    const colNames = columns.map(c => `"${c}"`).join(', ')
    const values = columns.map(c => change.newData?.[c])

    const stmt = await tx.prepare(`INSERT INTO "${change.table}" (${colNames}) VALUES (${placeholders})`)
    await stmt.run(...values)
  }

  private async applyRemoteChange(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    if (change.operation === 'delete') {
      await this.deleteRow(tx, change)
      return
    }

    if (!change.newData) return

    const pkColumns = await this.getPkColumns(change.table)
    const sourceData = change.newData
    const wherePkSource = change.oldData ?? sourceData

    const setClauses: string[] = []
    const setValues: unknown[] = []
    const whereConditions: string[] = []
    const whereValues: unknown[] = []

    for (const [col, val] of Object.entries(sourceData)) {
      if (!validateIdentifier(col)) continue
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

  private async applyMergedData(
    tx: SQLiteConnection,
    change: ReplicationChange,
    mergedData: Record<string, unknown>,
  ): Promise<void> {
    const pkColumns = await this.getPkColumns(change.table)
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

  private async deleteRow(tx: SQLiteConnection, change: ReplicationChange): Promise<void> {
    const pkColumns = await this.getPkColumns(change.table)
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

  private async recordColumnVersions(
    tx: SQLiteConnection,
    change: ReplicationChange,
    data: Record<string, unknown> | null,
  ): Promise<void> {
    if (change.operation === 'delete') {
      const delStmt = await tx.prepare('DELETE FROM _sirannon_column_versions WHERE table_name = ? AND row_id = ?')
      await delStmt.run(change.table, change.rowId)
      return
    }

    if (!data) return

    const upsertStmt = await tx.prepare(
      `INSERT INTO _sirannon_column_versions (table_name, row_id, column_name, hlc, node_id)
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
