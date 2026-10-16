import type { SirannonError } from '../core/errors.js'

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
  return SERVER_FAULT_MESSAGES.get(err.code) ?? err.message
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
