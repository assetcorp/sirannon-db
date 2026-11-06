import { EncryptionError, keyRejected } from '../../core/encryption/errors.js'

const NOT_A_DATABASE = /file is not a database/i
const HEX_DIGITS = '0123456789abcdef'
const EXPO_SQLITE_ENCRYPTION_FLOOR = '15.0.0'
const SQLCIPHER_FLOOR = [4, 6, 0] as const

/**
 * The calls that the Expo driver makes on an `expo-sqlite` database while it keys a connection.
 *
 * @internal
 */
export interface ExpoKeyedHandle {
  execAsync(source: string): Promise<void>
  getFirstAsync<T>(source: string): Promise<T | null>
  closeAsync(): Promise<void>
}

function meetsSqlcipherFloor(cipherVersion: string): boolean {
  const parts = /^(\d+)\.(\d+)\.(\d+)/.exec(cipherVersion)
  if (parts === null) return false
  const found = [Number(parts[1]), Number(parts[2]), Number(parts[3])]
  for (let at = 0; at < SQLCIPHER_FLOOR.length; at++) {
    const floor = SQLCIPHER_FLOOR[at] ?? 0
    const value = found[at] ?? 0
    if (value !== floor) return value > floor
  }
  return true
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function rawKeyLiteral(rawKey: Uint8Array): string {
  let hex = ''
  for (const byte of rawKey) hex += HEX_DIGITS[byte >> 4] + HEX_DIGITS[byte & 0x0f]
  return `"x'${hex}'"`
}

/**
 * Reads the schema on a connection that opened without a key, before any other statement, so that an encrypted file
 * fails at the open whatever options follow.
 *
 * @param handle - A connection without a key on which no statement has run.
 * @param path - The database's name, which the error carries.
 * @throws An `EncryptionError` with `ENCRYPTION_KEY_MISSING` when SQLite reads the file as not a database.
 * @internal
 */
export async function readUnkeyedExpoSchema(handle: ExpoKeyedHandle, path: string): Promise<void> {
  try {
    await handle.getFirstAsync('SELECT count(*) FROM sqlite_schema')
  } catch (err) {
    if (!NOT_A_DATABASE.test(errorMessage(err))) throw err
    throw new EncryptionError(
      `The database ${path} is encrypted, so it opens only with the encryption option and its key`,
      'ENCRYPTION_KEY_MISSING',
      err,
    )
  }
}

/**
 * Opens a separate connection without a key, reads the schema, and closes it, to tell a new file, a plaintext
 * database, and an encrypted one apart, since the Expo driver cannot read a file's bytes.
 *
 * @param open - Opens a connection to the database without a key.
 * @returns The kind of file that the connection found.
 * @internal
 */
export async function probeExpoDatabase(
  open: () => Promise<ExpoKeyedHandle>,
): Promise<'new' | 'plaintext' | 'encrypted'> {
  const probe = await open()
  try {
    await probe.getFirstAsync('SELECT count(*) FROM sqlite_schema')
    const pages = await probe.getFirstAsync<{ page_count: number }>('PRAGMA page_count')
    return (pages?.page_count ?? 0) > 0 ? 'plaintext' : 'new'
  } catch (err) {
    if (NOT_A_DATABASE.test(errorMessage(err))) return 'encrypted'
    throw err
  } finally {
    await probe.closeAsync()
  }
}

/**
 * Keys an `expo-sqlite` connection before any other statement, keeps its temporary tables in memory, confirms that the
 * app carries SQLCipher, and reads the schema so that a wrong key fails at the open.
 *
 * @param handle - A connection on which no statement has run.
 * @param path - The database's name, which the errors carry.
 * @param rawKey - The 32-byte key.
 * @throws An `EncryptionError` with `ENCRYPTION_UNSUPPORTED` when the app carries no SQLCipher, or one older than the
 * SQLCipher 4.6.0 that expo-sqlite 15.0.0 vendors, and `ENCRYPTION_KEY_REJECTED` when the key fails the first page's
 * authentication check.
 * @internal
 */
export async function keyExpoConnection(handle: ExpoKeyedHandle, path: string, rawKey: Uint8Array): Promise<void> {
  await handle.execAsync(`PRAGMA key = ${rawKeyLiteral(rawKey)}`)
  await handle.execAsync('PRAGMA temp_store = MEMORY')
  const version = await handle.getFirstAsync<{ cipher_version?: unknown }>('PRAGMA cipher_version')
  const cipherVersion = typeof version?.cipher_version === 'string' ? version.cipher_version : ''
  if (!meetsSqlcipherFloor(cipherVersion)) {
    throw new EncryptionError(
      `Encryption on the Expo driver needs expo-sqlite ${EXPO_SQLITE_ENCRYPTION_FLOOR} or later, built with the useSQLCipher option of its config plugin, which carries SQLCipher ${SQLCIPHER_FLOOR.join('.')} or later. This app reports ${cipherVersion === '' ? 'no SQLCipher' : `SQLCipher ${cipherVersion}`}`,
      'ENCRYPTION_UNSUPPORTED',
    )
  }
  try {
    await handle.getFirstAsync('SELECT count(*) FROM sqlite_schema')
  } catch (err) {
    if (NOT_A_DATABASE.test(errorMessage(err))) throw keyRejected(path, err)
    throw err
  }
}
