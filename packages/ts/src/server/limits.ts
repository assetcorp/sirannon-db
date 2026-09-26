import { SirannonError } from '../core/errors.js'

const DEFAULT_MAX_BODY_BYTES = 1_048_576
const DEFAULT_WS_BACKPRESSURE_BYTES = 16 * 1_048_576
const UWS_MAX_LIMIT_BYTES = 4_294_967_295

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
