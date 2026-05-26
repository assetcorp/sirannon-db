import type { SQLiteConnection } from '../../../core/driver/types.js'

const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

interface SqliteTableRow {
  name: string
}

export interface CompareDatabasesOptions {
  excludeTables?: readonly string[]
}

export async function compareDatabases(
  primaryConn: SQLiteConnection,
  replicaConn: SQLiteConnection,
  options: CompareDatabasesOptions = {},
): Promise<void> {
  const exclude = new Set(options.excludeTables ?? [])

  const primaryTables = await listUserTables(primaryConn, exclude)
  const replicaTables = await listUserTables(replicaConn, exclude)

  if (primaryTables.length !== replicaTables.length) {
    throw new Error(
      `Table set mismatch: primary has [${primaryTables.join(', ')}], replica has [${replicaTables.join(', ')}]`,
    )
  }

  for (let i = 0; i < primaryTables.length; i++) {
    if (primaryTables[i] !== replicaTables[i]) {
      throw new Error(
        `Table set mismatch: primary[${i}]=${primaryTables[i]} vs replica[${i}]=${replicaTables[i]} ` +
          `(full lists: primary=[${primaryTables.join(', ')}], replica=[${replicaTables.join(', ')}])`,
      )
    }
  }

  for (const table of primaryTables) {
    await compareTable(primaryConn, replicaConn, table)
  }
}

async function listUserTables(conn: SQLiteConnection, exclude: Set<string>): Promise<string[]> {
  const stmt = await conn.prepare(
    `SELECT name FROM sqlite_master
     WHERE type = 'table' AND name NOT LIKE '_sirannon_%' AND name NOT LIKE 'sqlite_%'
     ORDER BY name`,
  )
  const rows = (await stmt.all()) as SqliteTableRow[]
  return rows.map(r => r.name).filter(name => !exclude.has(name) && IDENTIFIER_RE.test(name))
}

async function compareTable(primary: SQLiteConnection, replica: SQLiteConnection, table: string): Promise<void> {
  const orderingClause = await buildOrderingClause(primary, table)
  const sql = `SELECT * FROM "${table}"${orderingClause}`

  const primaryStmt = await primary.prepare(sql)
  const replicaStmt = await replica.prepare(sql)
  const primaryRows = (await primaryStmt.all()) as Record<string, unknown>[]
  const replicaRows = (await replicaStmt.all()) as Record<string, unknown>[]

  if (primaryRows.length !== replicaRows.length) {
    throw new Error(
      `Row count mismatch in '${table}': primary has ${primaryRows.length}, replica has ${replicaRows.length}`,
    )
  }

  for (let i = 0; i < primaryRows.length; i++) {
    const primaryRow = primaryRows[i]
    const replicaRow = replicaRows[i]
    const diff = diffRow(primaryRow, replicaRow)
    if (diff) {
      throw new Error(`Row ${i} of '${table}' differs: ${diff}`)
    }
  }
}

async function buildOrderingClause(conn: SQLiteConnection, table: string): Promise<string> {
  const pragmaStmt = await conn.prepare(`PRAGMA table_info("${table}")`)
  const pragma = (await pragmaStmt.all()) as Array<{ name: string; pk: number }>
  const pkColumns = pragma
    .filter(col => col.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map(col => col.name)
    .filter(name => IDENTIFIER_RE.test(name))

  if (pkColumns.length === 0) {
    return ' ORDER BY rowid'
  }
  return ` ORDER BY ${pkColumns.map(c => `"${c}"`).join(', ')}`
}

function diffRow(a: Record<string, unknown>, b: Record<string, unknown>): string | null {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)])
  const differences: string[] = []

  for (const key of keys) {
    if (!normalisedEqual(a[key], b[key])) {
      differences.push(`${key}: primary=${formatValue(a[key])} replica=${formatValue(b[key])}`)
    }
  }

  return differences.length === 0 ? null : differences.join('; ')
}

function normalisedEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (a === null || a === undefined) return b === null || b === undefined
  if (b === null || b === undefined) return false

  if (typeof a === 'bigint' && typeof b === 'number') {
    return Number(a) === b
  }
  if (typeof b === 'bigint' && typeof a === 'number') {
    return Number(b) === a
  }
  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a === b
  }

  if (isBinary(a) && isBinary(b)) {
    return binaryEqual(a, b)
  }

  return false
}

function isBinary(value: unknown): value is Uint8Array | Buffer {
  return value instanceof Uint8Array || (typeof Buffer !== 'undefined' && Buffer.isBuffer(value))
}

function binaryEqual(a: Uint8Array | Buffer, b: Uint8Array | Buffer): boolean {
  const ab = Buffer.isBuffer(a) ? a : Buffer.from(a)
  const bb = Buffer.isBuffer(b) ? b : Buffer.from(b)
  return Buffer.compare(ab, bb) === 0
}

function formatValue(v: unknown): string {
  if (v === null) return 'null'
  if (v === undefined) return 'undefined'
  if (typeof v === 'bigint') return `${v}n`
  if (typeof v === 'string') return JSON.stringify(v)
  if (isBinary(v)) {
    const b = Buffer.isBuffer(v) ? v : Buffer.from(v)
    return `<${b.length} bytes>`
  }
  return String(v)
}
