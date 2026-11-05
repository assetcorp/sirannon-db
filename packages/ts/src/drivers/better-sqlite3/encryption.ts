import { EncryptionError, keyRejected } from '../../core/encryption/errors.js'
import { nodeDatabaseFiles } from '../node-files.js'
import type { SirannonSqliteFeature } from './sirannon-sqlite.js'

const AEGIS_CIPHER_NAME = 'aegis'
const AEGIS_256_ALGORITHM_NAME = 'aegis-256'
const SQLITE_HEADER = 'SQLite format 3\0'
const SQLITE_HEADER_BYTES = SQLITE_HEADER.length

/**
 * The encryption engine that the `encryption` option needs from Sirannon's SQLite build.
 *
 * @internal
 */
export const ENCRYPTION_FEATURE: SirannonSqliteFeature = {
  option: 'encryption',
  code: 'ENCRYPTION_UNSUPPORTED',
  description: 'encryption engine',
  presentIn: addon => typeof addon.Database?.prototype?.key === 'function',
}

/**
 * What a database path holds before a connection opens it: nothing yet, a plaintext SQLite database, ciphertext, or a
 * database that SQLite keeps in memory. `unknown` covers a path that the driver cannot read as a plain file, such as a
 * URI, whose contents {@link probeDatabaseFile} identifies by opening the path through SQLite.
 *
 * @internal
 */
export type DatabaseFileKind = 'new' | 'plaintext' | 'encrypted' | 'memory' | 'unknown'

/**
 * A better-sqlite3 database from Sirannon's SQLite3 Multiple Ciphers build, with the calls that key it and capture
 * its changes.
 *
 * @internal
 */
export interface EncryptingDatabase {
  key(rawKey: Uint8Array): unknown
  startChangeCapture(): unknown
  takeCapturedChanges(): Uint8Array
  stopChangeCapture(): unknown
  applyChanges(changes: Uint8Array): unknown
  pragma(source: string, options?: { simple?: boolean }): unknown
  prepare(source: string): { get(): unknown }
  close(): unknown
}

/**
 * Reads the start of a database file to tell a new file, a plaintext database, and an encrypted one apart.
 *
 * @param path - The path that the driver opens.
 * @returns The kind of file at the path.
 * @internal
 */
export async function classifyDatabaseFile(path: string): Promise<DatabaseFileKind> {
  if (path === '' || path === ':memory:') return 'memory'
  if (path.startsWith('file:')) return 'unknown'
  const start = await nodeDatabaseFiles.readStart(path, SQLITE_HEADER_BYTES)
  if (start === null || start.length === 0) return 'new'
  if (start.length === SQLITE_HEADER_BYTES && String.fromCharCode(...start) === SQLITE_HEADER) return 'plaintext'
  return 'encrypted'
}

/**
 * Checks a keyed open against the file that it opens, before the driver creates a connection.
 *
 * @param path - The path that the driver opens.
 * @param kind - What {@link classifyDatabaseFile} found at the path.
 * @param keyed - `true` when the open carries a key.
 * @throws An `EncryptionError` with `ENCRYPTION_UNSUPPORTED` for an in-memory database with a key,
 * `DATABASE_NOT_ENCRYPTED` for a key on a plaintext file, and `ENCRYPTION_KEY_MISSING` for an encrypted file without
 * a key.
 * @internal
 */
export function assertKeyFitsFile(path: string, kind: DatabaseFileKind, keyed: boolean): void {
  if (keyed && kind === 'memory') {
    throw new EncryptionError('An in-memory database cannot be encrypted', 'ENCRYPTION_UNSUPPORTED')
  }
  if (keyed && kind === 'plaintext') {
    throw new EncryptionError(
      `The database at ${path} is plaintext, so it opens without a key. Encrypt it through changeEncryption`,
      'DATABASE_NOT_ENCRYPTED',
    )
  }
  if (!keyed && kind === 'encrypted') {
    throw new EncryptionError(
      `The database at ${path} is encrypted, so it opens only with the encryption option and its key`,
      'ENCRYPTION_KEY_MISSING',
    )
  }
}

function sqliteCode(err: unknown): string | undefined {
  if (typeof err !== 'object' || err === null) return undefined
  const { code } = err as { code?: unknown }
  return typeof code === 'string' ? code : undefined
}

/**
 * Opens a separate connection without a key, reads the schema, and closes it, to tell a new file, a plaintext
 * database, an encrypted one, and an in-memory one apart at a path that the driver cannot read as a plain file.
 *
 * @param open - Opens a connection to the path without a key, with the flags of the open that follows.
 * @returns The kind of database that the connection found.
 * @internal
 */
export function probeDatabaseFile(open: () => EncryptingDatabase): DatabaseFileKind {
  let probe: EncryptingDatabase
  try {
    probe = open()
  } catch (err) {
    if (sqliteCode(err) === 'SQLITE_CANTOPEN') return 'new'
    throw err
  }
  try {
    const main = probe.prepare("SELECT file FROM pragma_database_list WHERE name = 'main'").get() as
      | { file?: unknown }
      | undefined
    if (main?.file === '') return 'memory'
    probe.prepare('SELECT count(*) FROM sqlite_schema').get()
    return Number(probe.pragma('page_count', { simple: true })) === 0 ? 'new' : 'plaintext'
  } catch (err) {
    if (sqliteCode(err) === 'SQLITE_NOTADB') return 'encrypted'
    throw err
  } finally {
    probe.close()
  }
}

function readSchema(db: EncryptingDatabase, path: string): void {
  try {
    db.prepare('SELECT count(*) FROM sqlite_schema').get()
  } catch (err) {
    const code = sqliteCode(err)
    if (code === 'SQLITE_NOTADB') throw keyRejected(path, err)
    if (code?.startsWith('SQLITE_CORRUPT')) {
      throw new EncryptionError(
        `A schema page of the encrypted database at ${path} failed its authentication check`,
        'ENCRYPTED_PAGE_CORRUPT',
        err,
      )
    }
    throw err
  }
}

function engineVersion(db: EncryptingDatabase): unknown {
  try {
    const row = db.prepare('SELECT sqlite3mc_version() AS version').get() as { version?: unknown } | undefined
    return row?.version
  } catch {
    return undefined
  }
}

/**
 * Applies a key to a connection before any other statement, keeps its temporary tables in memory, reads the schema so
 * that a wrong key fails at the open, and confirms that the engine encrypts with AEGIS-256.
 *
 * @param db - A connection from Sirannon's SQLite3 Multiple Ciphers build on which no statement has run.
 * @param path - The path of the database, which the error names.
 * @param rawKey - The 32-byte key.
 * @throws An `EncryptionError` with `ENCRYPTION_UNSUPPORTED` when the engine reports no version or another cipher,
 * `ENCRYPTION_KEY_REJECTED` when the key fails the first page's authentication check, and `ENCRYPTED_PAGE_CORRUPT`
 * when a later schema page fails it.
 * @internal
 */
export function keyConnection(db: EncryptingDatabase, path: string, rawKey: Uint8Array): void {
  db.key(rawKey)
  db.pragma('temp_store = MEMORY')
  readSchema(db, path)
  const version = engineVersion(db)
  if (typeof version !== 'string' || version.length === 0) {
    throw new EncryptionError(
      'The SQLite build reports no encryption engine version, so it cannot encrypt',
      'ENCRYPTION_UNSUPPORTED',
    )
  }
  if (
    db.pragma('cipher', { simple: true }) !== AEGIS_CIPHER_NAME ||
    db.pragma('algorithm', { simple: true }) !== AEGIS_256_ALGORITHM_NAME
  ) {
    throw new EncryptionError('The SQLite build encrypts with a cipher other than AEGIS-256', 'ENCRYPTION_UNSUPPORTED')
  }
}
