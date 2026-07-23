import type { SQLiteConnection } from '../driver/types.js'
import { ensureMetaTable, getMetaValue, setMetaValue } from '../system-catalog/index.js'
import { HLC } from './hlc.js'

export const HLC_CLOCK_META_KEY = 'hlc_clock'

export function isWellFormedHlc(candidate: string): boolean {
  if (candidate.length === 0) return false
  let decoded: ReturnType<typeof HLC.decode>
  try {
    decoded = HLC.decode(candidate)
  } catch {
    return false
  }
  if (!Number.isInteger(decoded.wallMs) || decoded.wallMs < 0) return false
  if (!Number.isInteger(decoded.logical) || decoded.logical < 0) return false
  if (decoded.nodeId.length === 0) return false
  return true
}

export async function persistHlcClock(conn: SQLiteConnection, encoded: string): Promise<void> {
  await ensureMetaTable(conn)
  await setMetaValue(conn, HLC_CLOCK_META_KEY, encoded)
}

export async function loadPersistedHlc(conn: SQLiteConnection): Promise<string | null> {
  await ensureMetaTable(conn)
  const value = await getMetaValue(conn, HLC_CLOCK_META_KEY)
  if (value === null || !isWellFormedHlc(value)) return null
  return value
}
