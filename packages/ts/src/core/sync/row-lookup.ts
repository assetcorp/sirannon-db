import type { SQLiteConnection } from '../driver/types.js'
import { validateIdentifier } from './validators.js'

export async function findRowByPk(
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
