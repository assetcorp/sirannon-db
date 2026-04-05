import { createHash } from 'node:crypto'
import type { ChangeTracker } from '../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import { BatchValidationError, ReplicationError } from './errors.js'
import { HLC } from './hlc.js'
import type {
  ApplyResult,
  ConflictResolver,
  ReplicationBatch,
  ReplicationChange,
  SyncPhase,
  SyncTableManifest,
} from './types.js'

const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

const SAFE_DDL_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

function validateIdentifier(name: string): boolean {
  return IDENTIFIER_RE.test(name)
}

const DDL_DENY_RE = /\b(load_extension|ATTACH|randomblob|zeroblob|writefile|readfile|fts3_tokenizer)\b/i

function validateDdlSafety(sql: string): boolean {
  if (!SAFE_DDL_RE.test(sql)) return false
  if (sql.includes(';')) return false
  if (/\bAS\s+SELECT\b/i.test(sql)) return false
  if (DDL_DENY_RE.test(sql)) return false
  const body = sql.replace(SAFE_DDL_RE, '')
  if (/\bSELECT\b/i.test(body)) return false
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
  private readonly activeSyncSnapshotSeqs = new Set<bigint>()

  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    private readonly changesTable: string = '_sirannon_changes',
  ) {}

  async ensureReplicationTables(): Promise<void> {
    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS "${this.changesTable}" (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	table_name TEXT NOT NULL,
	operation TEXT NOT NULL,
	row_id TEXT NOT NULL,
	changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
	old_data TEXT,
	new_data TEXT,
	node_id TEXT NOT NULL DEFAULT '',
	tx_id TEXT NOT NULL DEFAULT '',
	hlc TEXT NOT NULL DEFAULT ''
)`)

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

    await this.conn.exec(`
CREATE TABLE IF NOT EXISTS _sirannon_sync_state (
	table_name TEXT PRIMARY KEY,
	status TEXT NOT NULL DEFAULT 'pending',
	row_count INTEGER NOT NULL DEFAULT 0,
	pk_hash TEXT NOT NULL DEFAULT '',
	snapshot_seq INTEGER,
	source_peer_id TEXT,
	started_at REAL,
	completed_at REAL,
	request_id TEXT
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
    const rows = (await stmt.all(afterSeq.toString(), this.localNodeId, batchSize)) as ChangeRow[]

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

  async stampChanges(tx: SQLiteConnection, afterSeq: bigint, txId: string): Promise<void> {
    const hlcValue = this.hlc.now()
    const stmt = await tx.prepare(
      `UPDATE "${this.changesTable}" SET node_id = ?, tx_id = ?, hlc = ? WHERE seq > ? AND node_id = ''`,
    )
    await stmt.run(this.localNodeId, txId, hlcValue, afterSeq.toString())
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

    const needsPartialDedup = batch.fromSeq <= lastApplied
    let appliedSeqSet: Set<string> | null = null
    if (needsPartialDedup) {
      appliedSeqSet = new Set<string>()
      const checkStmt = await this.conn.prepare(
        'SELECT source_seq FROM _sirannon_applied_changes WHERE source_node_id = ? AND source_seq >= ? AND source_seq <= ?',
      )
      const applied = (await checkStmt.all(
        batch.sourceNodeId,
        batch.fromSeq.toString(),
        batch.toSeq.toString(),
      )) as Array<{ source_seq: number }>
      for (const row of applied) {
        appliedSeqSet.add(String(row.source_seq))
      }
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
      if (appliedSeqSet?.has(seq.toString())) continue
      await recordStmt.run(batch.sourceNodeId, seq.toString(), nowSec)
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
    await stmt.run(fromNodeId, seq.toString(), Date.now() / 1000)
  }

  async getLocalSeq(): Promise<bigint> {
    const stmt = await this.conn.prepare(`SELECT MAX(seq) as max_seq FROM "${this.changesTable}"`)
    const row = (await stmt.get()) as { max_seq: number | null } | undefined
    if (!row || row.max_seq === null) {
      return 0n
    }
    return BigInt(row.max_seq)
  }

  async getMinAckedSeq(): Promise<bigint | null> {
    const stmt = await this.conn.prepare(
      'SELECT MIN(last_acked_seq) as min_seq, COUNT(*) as cnt FROM _sirannon_peer_state',
    )
    const row = (await stmt.get()) as { min_seq: number | null; cnt: number } | undefined

    const hasPeers = row !== undefined && row.cnt > 0
    let result: bigint | null = null

    if (hasPeers && row.min_seq !== null) {
      result = BigInt(row.min_seq)
    } else if (hasPeers) {
      result = 0n
    }

    for (const syncSeq of this.activeSyncSnapshotSeqs) {
      if (result === null || syncSeq < result) {
        result = syncSeq
      }
    }

    return result
  }

  registerActiveSyncSeq(seq: bigint): void {
    this.activeSyncSnapshotSeqs.add(seq)
  }

  unregisterActiveSyncSeq(seq: bigint): void {
    this.activeSyncSnapshotSeqs.delete(seq)
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
      const dumpEpoch = BigInt(Date.now()) * 1_000_000n
      const fromSeq = dumpEpoch + BigInt(offset)
      const toSeq = dumpEpoch + BigInt(offset + rows.length - 1)

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

  async dumpSchema(conn: SQLiteConnection, excludePrefix: string = '_sirannon_'): Promise<string[]> {
    const stmt = await conn.prepare(
      `SELECT type, name, sql FROM sqlite_master
       WHERE type IN ('table', 'index') AND name NOT LIKE ? AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL`,
    )
    const rows = (await stmt.all(`${excludePrefix}%`)) as Array<{ type: string; name: string; sql: string }>

    const filtered = rows.filter(row => {
      if (row.name.startsWith(excludePrefix)) return false
      if (row.name.startsWith('sqlite_')) return false
      return validateDdlSafety(row.sql)
    })

    const tables: Array<{ name: string; sql: string }> = []
    const indexes: Array<{ sql: string }> = []

    for (const row of filtered) {
      if (row.type === 'table') {
        tables.push({ name: row.name, sql: row.sql })
      } else {
        indexes.push({ sql: row.sql })
      }
    }

    const tableOrder = await this.getTablesInFkOrder(conn)
    const orderMap = new Map<string, number>()
    for (let i = 0; i < tableOrder.length; i++) {
      orderMap.set(tableOrder[i], i)
    }

    tables.sort((a, b) => (orderMap.get(a.name) ?? 999) - (orderMap.get(b.name) ?? 999))

    const result: string[] = []
    for (const t of tables) {
      result.push(t.sql)
    }
    for (const idx of indexes) {
      result.push(idx.sql)
    }
    return result
  }

  async getTablesInFkOrder(conn: SQLiteConnection): Promise<string[]> {
    const stmt = await conn.prepare(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_sirannon_%' AND name NOT LIKE 'sqlite_%'`,
    )
    const tableRows = (await stmt.all()) as Array<{ name: string }>
    const tableNames = tableRows.map(r => r.name)

    const adjacency = new Map<string, Set<string>>()
    const inDegree = new Map<string, number>()

    for (const name of tableNames) {
      adjacency.set(name, new Set())
      inDegree.set(name, 0)
    }

    for (const name of tableNames) {
      const fkStmt = await conn.prepare(`PRAGMA foreign_key_list("${name}")`)
      const fks = (await fkStmt.all()) as Array<{ table: string }>
      for (const fk of fks) {
        if (tableNames.includes(fk.table) && fk.table !== name) {
          const deps = adjacency.get(fk.table)
          if (deps && !deps.has(name)) {
            deps.add(name)
            inDegree.set(name, (inDegree.get(name) ?? 0) + 1)
          }
        }
      }
    }

    const queue: string[] = []
    for (const name of tableNames) {
      if ((inDegree.get(name) ?? 0) === 0) {
        queue.push(name)
      }
    }

    const sorted: string[] = []
    while (queue.length > 0) {
      const current = queue.shift()
      if (current === undefined) break
      sorted.push(current)
      const deps = adjacency.get(current)
      if (deps) {
        for (const dep of deps) {
          const newDeg = (inDegree.get(dep) ?? 1) - 1
          inDegree.set(dep, newDeg)
          if (newDeg === 0) {
            queue.push(dep)
          }
        }
      }
    }

    if (sorted.length < tableNames.length) {
      const remaining = tableNames.filter(n => !sorted.includes(n)).sort()
      sorted.push(...remaining)
    }

    return sorted
  }

  async *dumpTableOnConnection(
    conn: SQLiteConnection,
    table: string,
    batchSize: number,
  ): AsyncGenerator<{ rows: Record<string, unknown>[]; checksum: string; isLast: boolean }> {
    if (!IDENTIFIER_RE.test(table)) {
      throw new ReplicationError(`Invalid table name: ${table}`)
    }

    const pkColumns = await this.getPkColumnsOnConnection(conn, table)
    const needsRowid = pkColumns.length === 1 && pkColumns[0] === 'rowid'

    let lastPkValues: unknown[] | null = null
    let done = false

    while (!done) {
      let selectSql: string
      const params: unknown[] = []

      if (lastPkValues === null) {
        selectSql = needsRowid
          ? `SELECT rowid, * FROM "${table}" ORDER BY rowid LIMIT ?`
          : `SELECT * FROM "${table}" ORDER BY ${pkColumns.map(c => `"${c}"`).join(', ')} LIMIT ?`
        params.push(batchSize)
      } else if (needsRowid) {
        selectSql = `SELECT rowid, * FROM "${table}" WHERE rowid > ? ORDER BY rowid LIMIT ?`
        params.push(lastPkValues[0], batchSize)
      } else if (pkColumns.length === 1) {
        selectSql = `SELECT * FROM "${table}" WHERE "${pkColumns[0]}" > ? ORDER BY "${pkColumns[0]}" LIMIT ?`
        params.push(lastPkValues[0], batchSize)
      } else {
        const colList = pkColumns.map(c => `"${c}"`).join(', ')
        const placeholders = pkColumns.map(() => '?').join(', ')
        selectSql = `SELECT * FROM "${table}" WHERE (${colList}) > (${placeholders}) ORDER BY ${colList} LIMIT ?`
        params.push(...lastPkValues, batchSize)
      }

      const stmt = await conn.prepare(selectSql)
      const rows = (await stmt.all(...params)) as Record<string, unknown>[]

      if (rows.length === 0) {
        break
      }

      const lastRow = rows[rows.length - 1]
      lastPkValues = pkColumns.map(col => lastRow[col])

      let isLast = rows.length < batchSize
      if (!isLast) {
        let peekSql: string
        const peekParams: unknown[] = []
        if (needsRowid) {
          peekSql = `SELECT 1 FROM "${table}" WHERE rowid > ? LIMIT 1`
          peekParams.push(lastPkValues[0])
        } else if (pkColumns.length === 1) {
          peekSql = `SELECT 1 FROM "${table}" WHERE "${pkColumns[0]}" > ? LIMIT 1`
          peekParams.push(lastPkValues[0])
        } else {
          const colList = pkColumns.map(c => `"${c}"`).join(', ')
          const placeholders = pkColumns.map(() => '?').join(', ')
          peekSql = `SELECT 1 FROM "${table}" WHERE (${colList}) > (${placeholders}) LIMIT 1`
          peekParams.push(...lastPkValues)
        }
        const peekStmt = await conn.prepare(peekSql)
        const peekRow = await peekStmt.get(...peekParams)
        if (!peekRow) {
          isLast = true
        }
      }

      done = isLast
      const checksum = createHash('sha256').update(JSON.stringify(rows)).digest('hex')

      yield { rows, checksum, isLast }
    }
  }

  async wipeTables(conn: SQLiteConnection, tables: string[], tracker: ChangeTracker): Promise<void> {
    for (const table of tables) {
      if (!IDENTIFIER_RE.test(table)) {
        throw new ReplicationError(`Invalid table name: ${table}`)
      }
    }

    await conn.exec('PRAGMA foreign_keys = OFF')
    try {
      await conn.transaction(async tx => {
        for (const table of tables) {
          await tracker.unwatch(tx, table)
        }
        const reversed = [...tables].reverse()
        for (const table of reversed) {
          await tx.exec(`DELETE FROM "${table}"`)
        }
        await tx.exec(`DELETE FROM _sirannon_sync_state WHERE table_name != '__sync_meta__'`)
      })
    } finally {
      await conn.exec('PRAGMA foreign_keys = ON')
    }
  }

  async setSyncTableStatus(table: string, status: string, rowCount?: number, pkHash?: string): Promise<void> {
    const stmt = await this.conn.prepare(
      `INSERT INTO _sirannon_sync_state (table_name, status, row_count, pk_hash, completed_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(table_name) DO UPDATE SET
         status = excluded.status,
         row_count = COALESCE(excluded.row_count, row_count),
         pk_hash = COALESCE(excluded.pk_hash, pk_hash),
         completed_at = excluded.completed_at`,
    )
    await stmt.run(table, status, rowCount ?? 0, pkHash ?? '', status === 'completed' ? Date.now() / 1000 : null)
  }

  async setSyncMeta(phase: SyncPhase, snapshotSeq?: bigint, sourcePeerId?: string, requestId?: string): Promise<void> {
    const stmt = await this.conn.prepare(
      `INSERT INTO _sirannon_sync_state (table_name, status, snapshot_seq, source_peer_id, started_at, request_id)
       VALUES ('__sync_meta__', ?, ?, ?, ?, ?)
       ON CONFLICT(table_name) DO UPDATE SET
         status = excluded.status,
         snapshot_seq = COALESCE(excluded.snapshot_seq, snapshot_seq),
         source_peer_id = COALESCE(excluded.source_peer_id, source_peer_id),
         started_at = COALESCE(excluded.started_at, started_at),
         request_id = COALESCE(excluded.request_id, request_id)`,
    )
    await stmt.run(
      phase,
      snapshotSeq !== undefined ? snapshotSeq.toString() : null,
      sourcePeerId ?? null,
      phase === 'syncing' ? Date.now() / 1000 : null,
      requestId ?? null,
    )
  }

  async getSyncState(): Promise<{
    phase: SyncPhase
    completedTables: string[]
    snapshotSeq: bigint | null
    sourcePeerId: string | null
  }> {
    const metaStmt = await this.conn.prepare(
      `SELECT status, snapshot_seq, source_peer_id FROM _sirannon_sync_state WHERE table_name = '__sync_meta__'`,
    )
    const meta = (await metaStmt.get()) as
      | { status: string; snapshot_seq: number | null; source_peer_id: string | null }
      | undefined

    if (!meta) {
      return { phase: 'ready', completedTables: [], snapshotSeq: null, sourcePeerId: null }
    }

    const completedStmt = await this.conn.prepare(
      `SELECT table_name FROM _sirannon_sync_state WHERE table_name != '__sync_meta__' AND status = 'completed'`,
    )
    const completedRows = (await completedStmt.all()) as Array<{ table_name: string }>

    return {
      phase: meta.status as SyncPhase,
      completedTables: completedRows.map(r => r.table_name),
      snapshotSeq: meta.snapshot_seq !== null ? BigInt(meta.snapshot_seq) : null,
      sourcePeerId: meta.source_peer_id,
    }
  }

  async isSyncCompleted(): Promise<boolean> {
    const stmt = await this.conn.prepare(`SELECT status FROM _sirannon_sync_state WHERE table_name = '__sync_meta__'`)
    const row = (await stmt.get()) as { status: string } | undefined
    return row?.status === 'ready'
  }

  async generateManifest(conn: SQLiteConnection, table: string): Promise<SyncTableManifest> {
    if (!IDENTIFIER_RE.test(table)) {
      throw new ReplicationError(`Invalid table name: ${table}`)
    }

    const countStmt = await conn.prepare(`SELECT COUNT(*) as cnt FROM "${table}"`)
    const countRow = (await countStmt.get()) as { cnt: number } | undefined
    const rowCount = countRow?.cnt ?? 0

    const hash = createHash('sha256')
    const pkColumns = await this.getPkColumnsOnConnection(conn, table)
    const needsRowid = pkColumns.length === 1 && pkColumns[0] === 'rowid'
    let lastPkValues: unknown[] | null = null
    const PK_BATCH_SIZE = 10_000

    let hasRows = true
    while (hasRows) {
      let selectSql: string
      const params: unknown[] = []

      if (needsRowid) {
        if (lastPkValues === null) {
          selectSql = `SELECT rowid FROM "${table}" ORDER BY rowid LIMIT ?`
        } else {
          selectSql = `SELECT rowid FROM "${table}" WHERE rowid > ? ORDER BY rowid LIMIT ?`
          params.push(lastPkValues[0])
        }
      } else if (pkColumns.length === 1) {
        const col = pkColumns[0]
        if (lastPkValues === null) {
          selectSql = `SELECT "${col}" FROM "${table}" ORDER BY "${col}" LIMIT ?`
        } else {
          selectSql = `SELECT "${col}" FROM "${table}" WHERE "${col}" > ? ORDER BY "${col}" LIMIT ?`
          params.push(lastPkValues[0])
        }
      } else {
        const colList = pkColumns.map(c => `"${c}"`).join(', ')
        if (lastPkValues === null) {
          selectSql = `SELECT ${colList} FROM "${table}" ORDER BY ${colList} LIMIT ?`
        } else {
          const placeholders = pkColumns.map(() => '?').join(', ')
          selectSql = `SELECT ${colList} FROM "${table}" WHERE (${colList}) > (${placeholders}) ORDER BY ${colList} LIMIT ?`
          params.push(...lastPkValues)
        }
      }
      params.push(PK_BATCH_SIZE)

      const stmt = await conn.prepare(selectSql)
      const rows = (await stmt.all(...params)) as Record<string, unknown>[]

      if (rows.length === 0) {
        hasRows = false
        break
      }

      for (const row of rows) {
        const pkVal = pkColumns.map(col => String(row[col] ?? '')).join('-')
        hash.update(pkVal)
      }

      if (rows.length < PK_BATCH_SIZE) {
        hasRows = false
      } else {
        const lastRow = rows[rows.length - 1]
        lastPkValues = pkColumns.map(col => lastRow[col])
      }
    }

    return { table, rowCount, pkHash: hash.digest('hex') }
  }

  async verifyManifest(manifest: SyncTableManifest): Promise<boolean> {
    const local = await this.generateManifest(this.conn, manifest.table)
    return local.rowCount === manifest.rowCount && local.pkHash === manifest.pkHash
  }

  private async getPkColumnsOnConnection(conn: SQLiteConnection, table: string): Promise<string[]> {
    const stmt = await conn.prepare(`PRAGMA table_info("${table}")`)
    const info = (await stmt.all()) as ColumnInfoRow[]
    const pkCols = info
      .filter(col => col.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map(col => col.name)
    if (pkCols.length === 0) {
      pkCols.push('rowid')
    }
    return pkCols
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
