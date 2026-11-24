import { EncryptionError, keyRejected } from '../../core/encryption/errors.js'
import type { SirannonSqliteFeature } from './sirannon-sqlite.js'

const AEGIS_CIPHER_NAME = 'aegis'
const AEGIS_256_ALGORITHM_NAME = 'aegis-256'
const SQLITE_HEADER = 'SQLite format 3\0'

/**
 * The encryption engine that the `encryption` option needs from Sirannon's SQLite build.
 *
 * @internal
 */
export const ENCRYPTION_FEATURE: SirannonSqliteFeature = {
  option: 'encryption',
  code: 'ENCRYPTION_UNSUPPORTED',
  description: 'encryption engine with file-header reads',
  presentIn: addon =>
    typeof addon.Database?.prototype?.key === 'function' &&
    typeof addon.Database?.prototype?.readFileHeader === 'function',
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
 * What the driver can tell from a database path alone: an in-memory database, a URI, or a path to a file.
 *
 * @internal
 */
export type DatabasePathKind = 'memory' | 'unknown' | 'file'

/**
 * A better-sqlite3 database from Sirannon's SQLite3 Multiple Ciphers build, with the calls that key it and capture
 * its changes.
 *
 * @internal
 */
export interface EncryptingDatabase {
  readFileHeader(): Uint8Array | null
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
 * Tells an in-memory database, a URI, and a path to a file apart from the path alone, without opening the file. The
 * connection that opens a file identifies its contents, because a descriptor that the driver opened and closed outside
 * SQLite would release every lock that SQLite holds on that file in this process.
 *
 * @param path - The path that the driver opens.
 * @returns The kind of path.
 * @internal
 */
export function classifyDatabasePath(path: string): DatabasePathKind {
  if (path === '' || path === ':memory:') return 'memory'
  if (path.startsWith('file:')) return 'unknown'
  return 'file'
}

/**
 * Checks whether an open, with or without a key, fits the kind of database at its path.
 *
 * @param path - The path that the driver opens.
 * @param kind - What {@link classifyOpenDatabase} or {@link probeDatabaseFile} found at the path.
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
 * database, an encrypted one, and an in-memory one apart. SQLite defers closing the connection's descriptor while
 * another connection in this process holds a lock on the file, so the probe leaves those locks in place.
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

/**
 * Identifies a file from its header through SQLite's open handle, before a key or a schema read can start recovery.
 *
 * @param db - A connection without a key on which no statement has run.
 * @returns Whether the connection has no file, an empty file, a plaintext header, or an encrypted header.
 * @internal
 */
export function classifyOpenDatabase(db: EncryptingDatabase): DatabaseFileKind {
  const header = db.readFileHeader()
  if (header === null) return 'memory'
  if (header.length === 0) return 'new'
  return header.length === SQLITE_HEADER.length && String.fromCharCode(...header) === SQLITE_HEADER
    ? 'plaintext'
    : 'encrypted'
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
