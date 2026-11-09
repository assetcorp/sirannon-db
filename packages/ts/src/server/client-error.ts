import type { SirannonError } from '../core/errors.js'

/**
 * The message that the server sends a client for an error that Sirannon did not raise, because such an error can name
 * files and other details of the server's machine.
 *
 * @internal
 */
export const UNEXPECTED_ERROR_MESSAGE = 'An unexpected error occurred'

const OPTIONAL_PACKAGE_MISSING = 'The server is missing an optional package that this request needs'

const SERVER_FAULT_MESSAGES: ReadonlyMap<string, string> = new Map([
  ['DATABASE_OPEN_FAILED', 'The server could not open this database'],
  ['EXTENSION_ERROR', 'The server could not load a SQLite extension that this database needs'],
  [
    'ROW_COUNTS_UNSUPPORTED',
    "The server's SQLite build cannot count rows, so it opens no database that needs row counts",
  ],
  ['SERVER_DEPENDENCY_MISSING', OPTIONAL_PACKAGE_MISSING],
  ['TRANSPORT_DEPENDENCY_MISSING', OPTIONAL_PACKAGE_MISSING],
  ['COORDINATOR_DEPENDENCY_MISSING', OPTIONAL_PACKAGE_MISSING],
  ['ENCRYPTION_UNSUPPORTED', "The server's SQLite driver cannot encrypt a database"],
  ['INVALID_ENCRYPTION_KEY', 'The encryption key for this database is not a 32-byte key'],
  ['ENCRYPTION_KEY_UNAVAILABLE', 'The server could not obtain the encryption key for this database'],
  ['ENCRYPTION_KEY_MISSING', 'This database is encrypted, and the server opened it without a key'],
  ['ENCRYPTION_KEY_REJECTED', 'The key that the server holds does not open this database'],
  ['DATABASE_NOT_ENCRYPTED', 'This database is not encrypted, and the server opened it with a key'],
  ['ENCRYPTED_PAGE_CORRUPT', 'A page of this encrypted database failed its integrity check'],
  ['ENCRYPTION_REQUIRED', 'An encrypted node sends no data to a node that holds a plaintext database'],
  ['ENCRYPTION_CHANGE_IN_PROGRESS', 'A key change is running on this database'],
  ['ENCRYPTION_CHANGE_ABORTED', 'The key change stopped and left the database under its current key'],
  ['BACKUP_ERROR', 'The backup failed on the server'],
  ['BACKUP_DESTINATION_ERROR', 'The backup destination failed the request'],
])

/**
 * Returns the message that the server sends a client for an error. An error that reports how the server itself is
 * set up, such as a database file that fails to open, a missing package, or a SQLite build that cannot count rows,
 * names server file paths in its own message, so the client receives a fixed message for its code, and the operator
 * keeps the full message on the error.
 *
 * @param err - The error that the request raised.
 * @returns The message to send.
 * @internal
 */
export function clientErrorMessage(err: SirannonError): string {
  return clientMessageFor(err)
}

/**
 * Returns the message that the server sends a client for an error that the server recorded as a code and a message,
 * which is the fixed message for its code where the recorded message can name server files.
 *
 * @param error - The recorded code and message.
 * @returns The message to send.
 * @internal
 */
export function clientMessageFor(error: { code: string; message: string }): string {
  return SERVER_FAULT_MESSAGES.get(error.code) ?? error.message
}

/**
 * Returns whether the server can send a client the details of an error, which holds for every error except one that
 * reports how the server itself is set up.
 *
 * @param err - The error that the request raised.
 * @returns `true` when the error's details can reach the client.
 * @internal
 */
export function sharesErrorDetails(err: SirannonError): boolean {
  return !SERVER_FAULT_MESSAGES.has(err.code)
}
