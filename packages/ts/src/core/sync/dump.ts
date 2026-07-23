import { createHash } from 'node:crypto'
import type { SQLiteConnection } from '../driver/types.js'
import { canonicaliseForChecksum } from './canonicalise.js'
import { computeChecksum } from './checksum.js'
import { ReplicationError } from './errors.js'
import type { HLC } from './hlc.js'
import type { PkResolver } from './pk.js'
import type { ReplicationBatch, ReplicationChange, SyncTableManifest } from './types.js'
import { IDENTIFIER_RE } from './validators.js'

export class DumpOps {
  constructor(
    private readonly conn: SQLiteConnection,
    private readonly localNodeId: string,
    private readonly hlc: HLC,
    private readonly pkResolver: PkResolver,
  ) {}

  async *dumpTable(table: string, batchSize: number): AsyncGenerator<ReplicationBatch> {
    if (!IDENTIFIER_RE.test(table)) {
      throw new ReplicationError(`Invalid table name: ${table}`)
    }

    const countStmt = await this.conn.prepare(`SELECT COUNT(*) as cnt FROM "${table}"`)
    const countRow = (await countStmt.get()) as { cnt: number } | undefined
    const total = countRow?.cnt ?? 0

    if (total === 0) return

    const pkColumns = await this.pkResolver.forTable(table)

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

      const checksum = computeChecksum(changes)
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

  dumpTableOnConnection(
    conn: SQLiteConnection,
    table: string,
    batchSize: number,
    startAfter?: unknown[],
  ): AsyncGenerator<{ rows: Record<string, unknown>[]; checksum: string; isLast: boolean }> {
    return dumpTablePages(conn, this.pkResolver, table, batchSize, startAfter)
  }

  async generateManifest(conn: SQLiteConnection, table: string): Promise<SyncTableManifest> {
    if (!IDENTIFIER_RE.test(table)) {
      throw new ReplicationError(`Invalid table name: ${table}`)
    }

    const countStmt = await conn.prepare(`SELECT COUNT(*) as cnt FROM "${table}"`)
    const countRow = (await countStmt.get()) as { cnt: number } | undefined
    const rowCount = countRow?.cnt ?? 0

    const hash = createHash('sha256')
    const pkColumns = await this.pkResolver.forTableOnConnection(conn, table)
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
}

export async function* dumpTablePages(
  conn: SQLiteConnection,
  pkResolver: PkResolver,
  table: string,
  batchSize: number,
  startAfter?: unknown[],
): AsyncGenerator<{ rows: Record<string, unknown>[]; checksum: string; isLast: boolean }> {
  if (!IDENTIFIER_RE.test(table)) {
    throw new ReplicationError(`Invalid table name: ${table}`)
  }

  const pkColumns = await pkResolver.forTableOnConnection(conn, table)
  const needsRowid = pkColumns.length === 1 && pkColumns[0] === 'rowid'

  if (startAfter !== undefined && startAfter.length !== pkColumns.length) {
    throw new ReplicationError(
      `Resume key for table '${table}' must carry ${pkColumns.length} value(s), received ${startAfter.length}`,
    )
  }

  let lastPkValues: unknown[] | null = startAfter ?? null
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
    const checksum = createHash('sha256').update(canonicaliseForChecksum(rows)).digest('hex')

    yield { rows, checksum, isLast }
  }
}
