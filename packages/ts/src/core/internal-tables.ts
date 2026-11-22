export const INTERNAL_TABLE_PREFIX = '_sirannon_'

export const CHANGES_TABLE = '_sirannon_changes'
export const META_TABLE = '_sirannon_meta'
export const MIGRATIONS_TABLE = '_sirannon_migrations'
export const PEER_STATE_TABLE = '_sirannon_peer_state'
export const APPLIED_CHANGES_TABLE = '_sirannon_applied_changes'
export const COLUMN_VERSIONS_TABLE = '_sirannon_column_versions'
export const SYNC_STATE_TABLE = '_sirannon_sync_state'
export const DEVICE_CURSORS_TABLE = '_sirannon_device_cursors'
export const STAGED_CHANGES_TABLE = '_sirannon_staged_changes'
export const STAGED_PUSH_CHANGES_TABLE = '_sirannon_staged_push_changes'
export const REPLACED_ROWS_TABLE = '_sirannon_replaced_rows'
export const ROLLED_BACK_CHANGES_TABLE = '_sirannon_rolled_back_changes'

export const CDC_TRIGGER_PREFIX = '_sirannon_trg_'
export const LIVE_PROBE_TABLE_PREFIX = '_sirannon_live_probe_'
export const KEY_CHANGE_CAPTURE_PREFIX = '_sirannon_key_change_'

const SIRANNON_PREFIX = '_sirannon'
const SQLITE_PREFIX = 'sqlite_'

/**
 * Which reserved family a name belongs to: `sirannon` for Sirannon's tables, `sqlite` for the SQLite catalogue, and
 * `null` for every other name.
 *
 * @internal
 */
export type ReservedIdentifierKind = 'sirannon' | 'sqlite' | null

/**
 * Returns the reserved family of a name, ignoring letter case.
 *
 * @param name - The identifier, without quotes.
 * @returns `sirannon` for a name beginning with `_sirannon`, `sqlite` for one beginning with `sqlite_`, and `null`
 * otherwise.
 * @internal
 */
export function reservedIdentifierKind(name: string): ReservedIdentifierKind {
  const lower = name.toLowerCase()
  if (lower.startsWith(SIRANNON_PREFIX)) return 'sirannon'
  if (lower.startsWith(SQLITE_PREFIX)) return 'sqlite'
  return null
}

export function isReservedIdentifier(name: string): boolean {
  return reservedIdentifierKind(name) !== null
}
