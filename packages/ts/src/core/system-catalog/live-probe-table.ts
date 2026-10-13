import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { assertSafeIdentifier } from './columns.js'

const SAFE_DECLARED_TYPE = /^[A-Za-z0-9_ ()',.+-]*$/

export interface LiveProbeColumn {
  name: string
  type: string
  collation: string | null
  generatedAs: string | null
}

export function storedLiveProbeColumns(columns: readonly LiveProbeColumn[]): LiveProbeColumn[] {
  return columns.filter(column => column.generatedAs === null)
}

export async function ensureLiveProbeTable(
  conn: SQLiteConnection,
  probeTable: string,
  columns: readonly LiveProbeColumn[],
): Promise<void> {
  assertSafeIdentifier(probeTable)
  if (columns.length === 0) {
    throw new SirannonError(`Cannot stage rows for '${probeTable}': the table declares no columns`, 'CDC_ERROR')
  }

  const definitions = columns.map(column => {
    assertSafeIdentifier(column.name)
    if (!SAFE_DECLARED_TYPE.test(column.type)) {
      throw new SirannonError(`Unsafe column type for internal schema: ${column.type}`, 'INTERNAL_SCHEMA_ERROR')
    }
    if (column.collation !== null) assertSafeIdentifier(column.collation)

    const type = column.type.trim().length === 0 ? '' : ` ${column.type.trim()}`
    const collate = column.collation === null ? '' : ` COLLATE "${column.collation}"`
    const generated = column.generatedAs === null ? '' : ` GENERATED ALWAYS AS (${column.generatedAs}) VIRTUAL`
    return `"${column.name}"${type}${collate}${generated}`
  })

  await conn.exec(`CREATE TEMP TABLE IF NOT EXISTS "${probeTable}" (${definitions.join(', ')})`)
}

export async function dropLiveProbeTable(conn: SQLiteConnection, probeTable: string): Promise<void> {
  assertSafeIdentifier(probeTable)
  await conn.exec(`DROP TABLE IF EXISTS temp."${probeTable}"`)
}

export async function prepareInsertLiveProbeRow(
  conn: SQLiteConnection,
  probeTable: string,
  columns: readonly LiveProbeColumn[],
): Promise<SQLiteStatement> {
  assertSafeIdentifier(probeTable)
  const stored = storedLiveProbeColumns(columns)
  const names = stored.map(column => {
    assertSafeIdentifier(column.name)
    return `"${column.name}"`
  })
  const placeholders = new Array(stored.length + 1).fill('?').join(', ')
  return conn.prepare(`INSERT INTO temp."${probeTable}" (rowid, ${names.join(', ')}) VALUES (${placeholders})`)
}

export async function prepareDeleteLiveProbeRows(conn: SQLiteConnection, probeTable: string): Promise<SQLiteStatement> {
  assertSafeIdentifier(probeTable)
  return conn.prepare(`DELETE FROM temp."${probeTable}"`)
}

export function selectLiveProbeMatchesSql(
  probeTable: string,
  keyColumn: string,
  projection: string,
  where: string | null,
  sourceName: string,
): string {
  assertSafeIdentifier(probeTable)
  assertSafeIdentifier(keyColumn)
  const filter = where === null ? '' : `\nWHERE (${where}\n)`
  return `SELECT rowid AS "${keyColumn}"\n, ${projection}\nFROM temp."${probeTable}" AS "${sourceName}"${filter}`
}
