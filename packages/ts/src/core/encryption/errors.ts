import { SirannonError } from '../errors.js'

/**
 * The codes of the errors that an encrypted database raises.
 *
 * @public
 */
export type EncryptionErrorCode =
  | 'ENCRYPTION_UNSUPPORTED'
  | 'INVALID_ENCRYPTION_KEY'
  | 'ENCRYPTION_KEY_UNAVAILABLE'
  | 'ENCRYPTION_KEY_MISSING'
  | 'ENCRYPTION_KEY_REJECTED'
  | 'DATABASE_NOT_ENCRYPTED'
  | 'ENCRYPTED_PAGE_CORRUPT'
  | 'ENCRYPTION_REQUIRED'
  | 'ENCRYPTION_CHANGE_IN_PROGRESS'
  | 'ENCRYPTION_CHANGE_ABORTED'

/**
 * Thrown when a database cannot be encrypted, opened under its key, or given a new key. The message never contains a
 * key.
 *
 * @public
 */
export class EncryptionError extends SirannonError {
  /**
   * Returns an error with an encryption code, and the failure that caused it when there is one.
   *
   * @param message - What failed, written without the key.
   * @param code - The machine-readable code.
   * @param cause - The failure that caused this error, such as the error that a key function threw.
   */
  constructor(message: string, code: EncryptionErrorCode, cause?: unknown) {
    super(message, code)
    this.name = 'EncryptionError'
    if (cause !== undefined) {
      Object.defineProperty(this, 'cause', { value: cause, enumerable: false, writable: true, configurable: true })
    }
  }
}

/**
 * Returns the error for a key that fails the authentication check of a database's first page, which names both
 * schemes, because a file that another driver encrypted fails the same check under the right key.
 *
 * @param path - The database, as the error names it.
 * @param cause - The engine's error.
 * @returns An `EncryptionError` with `ENCRYPTION_KEY_REJECTED`.
 * @internal
 */
export function keyRejected(path: string, cause: unknown): EncryptionError {
  return new EncryptionError(
    `The key failed the authentication check of the first page of the database ${path}. Either the key is wrong, or the file comes from another encryption scheme: the better-sqlite3 driver encrypts with AEGIS-256 and the Expo driver encrypts with SQLCipher 4, and neither opens the other's files`,
    'ENCRYPTION_KEY_REJECTED',
    cause,
  )
}

/**
 * Fails an open that carries a key on a driver whose SQLite cannot encrypt.
 *
 * @param driverName - The driver, as the error names it.
 * @param options - The open's settings.
 * @throws An `EncryptionError` with `ENCRYPTION_UNSUPPORTED` when the open carries a key.
 * @internal
 */
export function refuseEncryptedOpen(driverName: string, options: { encryption?: unknown } | undefined): void {
  if (options?.encryption === undefined) return
  throw new EncryptionError(
    `The ${driverName} driver opens SQLite through a library that Sirannon cannot replace with an encrypting build, so it cannot open an encrypted database`,
    'ENCRYPTION_UNSUPPORTED',
  )
}
