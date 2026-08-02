import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import { ensureMetaTable, insertMetaValueIfAbsent, selectMetaValue } from '../system-catalog/index.js'

const EPOCH_KEY = 'cdc_epoch'

export async function ensureCdcEpoch(conn: SQLiteConnection): Promise<string> {
  await ensureMetaTable(conn)
  await insertMetaValueIfAbsent(conn, EPOCH_KEY, randomHex(16))

  const value = await selectMetaValue(conn, EPOCH_KEY)
  if (value === null || value.length === 0) {
    throw new CDCError('Failed to read the CDC epoch identifier')
  }
  return value
}
