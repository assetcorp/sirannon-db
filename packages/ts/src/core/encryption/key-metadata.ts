import type { SQLiteConnection } from '../driver/types.js'
import { META_TABLE } from '../internal-tables.js'
import { deleteMetaValue, ensureMetaTable, selectMetaValue, upsertMetaValue } from '../system-catalog/meta-table.js'
import { selectTableExists } from '../system-catalog/sqlite-catalog.js'

/**
 * The meta key under which a database stores the identifier that came with its current key.
 *
 * @internal
 */
export const ENCRYPTION_KEY_ID_META_KEY = 'encryption_key_id'

/**
 * The meta key under which a database stores the time at which its last key change finished, in milliseconds since
 * the Unix epoch.
 *
 * @internal
 */
export const ENCRYPTION_CHANGED_AT_META_KEY = 'encryption_changed_at'

/**
 * What a database stores about its key.
 *
 * @internal
 */
export interface StoredKeyMetadata {
  keyId?: string
  changedAt?: number
}

/**
 * Returns the key identifier and the time of the last key change that a database stores.
 *
 * @param conn - A connection to the database.
 * @returns The stored values, with each one absent where the database stores none.
 * @internal
 */
export async function readKeyMetadata(conn: SQLiteConnection): Promise<StoredKeyMetadata> {
  if (!(await selectTableExists(conn, META_TABLE))) return {}
  const keyId = await selectMetaValue(conn, ENCRYPTION_KEY_ID_META_KEY)
  const changedAt = Number(await selectMetaValue(conn, ENCRYPTION_CHANGED_AT_META_KEY))
  return {
    ...(keyId === null ? {} : { keyId }),
    ...(Number.isSafeInteger(changedAt) && changedAt > 0 ? { changedAt } : {}),
  }
}

/**
 * Stores the identifier that came with the database's current key, and deletes a stored identifier when the key came
 * with none.
 *
 * @param conn - The writer connection.
 * @param keyId - The identifier, or `undefined` when the key came with none.
 * @internal
 */
export async function writeKeyId(conn: SQLiteConnection, keyId: string | undefined): Promise<void> {
  const stored = (await selectTableExists(conn, META_TABLE))
    ? await selectMetaValue(conn, ENCRYPTION_KEY_ID_META_KEY)
    : null
  if ((stored ?? undefined) === keyId) return
  if (keyId === undefined) {
    await deleteMetaValue(conn, ENCRYPTION_KEY_ID_META_KEY)
    return
  }
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, ENCRYPTION_KEY_ID_META_KEY, keyId)
}

/**
 * Stores the key identifier and the time at which a key change finished, in the file that the change writes.
 *
 * @param conn - A connection to the new file.
 * @param keyId - The identifier that came with the new key, or `undefined` when it came with none.
 * @param changedAt - The time at which the change finished, in milliseconds since the Unix epoch.
 * @internal
 */
export async function writeKeyChange(
  conn: SQLiteConnection,
  keyId: string | undefined,
  changedAt: number,
): Promise<void> {
  await ensureMetaTable(conn)
  await upsertMetaValue(conn, ENCRYPTION_CHANGED_AT_META_KEY, String(changedAt))
  if (keyId === undefined) await deleteMetaValue(conn, ENCRYPTION_KEY_ID_META_KEY)
  else await upsertMetaValue(conn, ENCRYPTION_KEY_ID_META_KEY, keyId)
}
