import type { SQLiteConnection, SQLiteDriver } from '../driver/types.js'
import { EncryptionError } from '../encryption/errors.js'
import { EncryptionKeyring } from '../encryption/keyring.js'
import type { EncryptionOptions } from '../encryption/types.js'
import type { BackupChainBase } from './chain.js'
import type { RebuiltDatabaseOpener } from './restore-apply.js'
import { type DatabaseHeader, readDatabaseHeader } from './restore-log.js'

const KEY_FAILURE_CODES = new Set(['ENCRYPTION_KEY_REJECTED', 'ENCRYPTION_KEY_MISSING'])

/**
 * Returns the function that opens a database that a restore rebuilds, under the key of the database that the restore
 * rebuilds.
 *
 * @param driver - The driver to open the database through.
 * @param encryption - The key of the database, or `undefined` for a plaintext one.
 * @returns The opener.
 * @internal
 */
export function rebuiltDatabaseOpener(driver: SQLiteDriver, encryption?: EncryptionOptions): RebuiltDatabaseOpener {
  const keyring = new EncryptionKeyring(encryption?.key ?? null)
  return path => keyring.openWith(keyed => driver.open(path, { walMode: false, walAutoCheckpoint: 0, ...keyed }))
}

function explainKeyFailure(err: unknown, base: BackupChainBase): unknown {
  if (!(err instanceof EncryptionError) || !KEY_FAILURE_CODES.has(err.code)) return err
  const heldUnder = base.keyId === undefined ? 'a key that came with no identifier' : `the key '${base.keyId}'`
  return new EncryptionError(
    `The key that the restore opened with failed on the full copy '${base.name}', which holds ciphertext under ${heldUnder}`,
    err.code === 'ENCRYPTION_KEY_MISSING' ? 'ENCRYPTION_KEY_MISSING' : 'ENCRYPTION_KEY_REJECTED',
    err,
  )
}

async function readText(conn: SQLiteConnection, sql: string): Promise<unknown> {
  const row = (await (await conn.prepare(sql)).get()) as Record<string, unknown> | undefined
  return row === undefined ? undefined : Object.values(row)[0]
}

/**
 * Reads the page size and the journal format of the full copy that a restore assembled, through a connection under
 * the key where the copy holds ciphertext.
 *
 * @param path - The assembled full copy.
 * @param openRebuilt - Opens the copy under the key of the database.
 * @param base - The record of the full copy, whose key identifier a key failure states.
 * @returns The page size and the journal format.
 * @throws An `EncryptionError` with `ENCRYPTION_KEY_REJECTED` that states the copy's key identifier when the key fails.
 * @internal
 */
export async function readRebuiltHeader(
  path: string,
  openRebuilt: RebuiltDatabaseOpener,
  base: BackupChainBase,
): Promise<DatabaseHeader> {
  const plaintext = await readDatabaseHeader(path)
  if (plaintext !== null) return plaintext
  let conn: SQLiteConnection
  try {
    conn = await openRebuilt(path)
  } catch (err) {
    throw explainKeyFailure(err, base)
  }
  try {
    const pageSize = Number(await readText(conn, 'PRAGMA page_size'))
    const journalMode = await readText(conn, 'PRAGMA journal_mode')
    return { pageSize, walMode: typeof journalMode === 'string' && journalMode.toLowerCase() === 'wal' }
  } finally {
    await conn.close()
  }
}
