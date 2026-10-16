import { DEFAULT_HTTP_REQUEST_TIMEOUT_MS } from '../client/http-json.js'
import { SirannonError } from '../core/errors.js'

const DEFAULT_MAX_BODY_BYTES = 1_048_576
const DEFAULT_WS_BACKPRESSURE_BYTES = 16 * 1_048_576
export const UWS_MAX_LIMIT_BYTES = 4_294_967_295
const DEFAULT_CLOSE_TIMEOUT_MS = DEFAULT_HTTP_REQUEST_TIMEOUT_MS
const MAX_TIMER_DELAY_MS = 2_147_483_647

/**
 * Returns how long, in milliseconds, closing the server waits for requests in progress before it ends every
 * connection.
 *
 * The default is 30_000, the client's default request timeout, because a client on default settings stops waiting for
 * a reply after that long.
 *
 * @param value - The `timeoutMs` that the caller passed to `close`, or undefined for the default.
 * @returns The wait in milliseconds.
 * @throws A `SirannonError` with code `INVALID_CLOSE_TIMEOUT` when the value is not a whole number from 0 to
 * 2_147_483_647, the longest delay that a JavaScript timer holds.
 * @internal
 */
export function resolveCloseTimeout(value: number | undefined): number {
  if (value === undefined) return DEFAULT_CLOSE_TIMEOUT_MS
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0 || value > MAX_TIMER_DELAY_MS) {
    throw new SirannonError(
      `close timeoutMs must be a whole number of milliseconds from 0 to ${MAX_TIMER_DELAY_MS}, the longest delay that a JavaScript timer holds`,
      'INVALID_CLOSE_TIMEOUT',
    )
  }
  return value
}

/**
 * Throws when the operator opens the restore route without an `authenticate` hook.
 *
 * The restore route replaces a database while the server is serving it, and the
 * `authenticate` hook is the only check that the server performs before that route.
 * Without the hook, the server accepts every restore request anonymously, so under
 * the default cross-origin rules a plain form post from any web page could start a
 * restore. The server therefore refuses to start with that configuration.
 *
 * @param acceptBackupRestore - Whether the operator opens the restore route.
 * @param hasAuthenticate - Whether the operator supplies an `authenticate` hook.
 *
 * @internal
 */
export function assertBackupRestoreAuthenticated(acceptBackupRestore: boolean, hasAuthenticate: boolean): void {
  if (acceptBackupRestore && !hasAuthenticate) {
    throw new SirannonError(
      'ServerOptions.acceptBackupRestore opens a route that replaces a running database, so it needs an authenticate hook to name the caller',
      'INVALID_BACKUP_RESTORE',
    )
  }
}

export function assertDeviceSyncAuthenticated(acceptDeviceSync: boolean, hasAuthenticate: boolean): void {
  if (acceptDeviceSync && !hasAuthenticate) {
    throw new SirannonError(
      'ServerOptions.acceptDeviceSync opens a route that writes rows into every table of a database, so it needs an authenticate hook to name the caller',
      'INVALID_DEVICE_SYNC',
    )
  }
}

export function resolveMaxBodyBytes(value: number | undefined): number {
  if (value === undefined) return DEFAULT_MAX_BODY_BYTES
  if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) {
    throw new SirannonError(
      'ServerOptions.maxBodyBytes must be a positive integer number of bytes',
      'INVALID_MAX_BODY_BYTES',
    )
  }
  if (value > UWS_MAX_LIMIT_BYTES) {
    throw new SirannonError(
      `ServerOptions.maxBodyBytes must be at most ${UWS_MAX_LIMIT_BYTES} bytes; uWebSockets.js stores the limit as an unsigned 32-bit integer and would silently wrap a larger value modulo 2^32`,
      'INVALID_MAX_BODY_BYTES',
    )
  }
  return value
}

export function resolveWsBackpressure(value: number | undefined, maxBodyBytes: number): number {
  const resolved = value ?? Math.max(DEFAULT_WS_BACKPRESSURE_BYTES, maxBodyBytes)
  if (typeof resolved !== 'number' || !Number.isInteger(resolved) || resolved <= 0) {
    throw new SirannonError(
      'ServerOptions.maxWebSocketBackpressureBytes must be a positive integer number of bytes',
      'INVALID_WS_BACKPRESSURE',
    )
  }
  if (resolved > UWS_MAX_LIMIT_BYTES) {
    throw new SirannonError(
      `ServerOptions.maxWebSocketBackpressureBytes must be at most ${UWS_MAX_LIMIT_BYTES} bytes; uWebSockets.js stores the limit as an unsigned 32-bit integer and would silently wrap a larger value modulo 2^32`,
      'INVALID_WS_BACKPRESSURE',
    )
  }
  if (resolved < maxBodyBytes) {
    throw new SirannonError(
      'ServerOptions.maxWebSocketBackpressureBytes must be at least maxBodyBytes so a single frame fits',
      'INVALID_WS_BACKPRESSURE',
    )
  }
  return resolved
}
