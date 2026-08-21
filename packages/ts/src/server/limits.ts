import { SirannonError } from '../core/errors.js'

const DEFAULT_MAX_BODY_BYTES = 1_048_576
const DEFAULT_WS_BACKPRESSURE_BYTES = 16 * 1_048_576
const UWS_MAX_LIMIT_BYTES = 4_294_967_295

/**
 * Refuses a server that would rebuild a database for a caller it cannot name.
 *
 * The restore route replaces a database that is serving traffic, and the
 * `authenticate` hook is the only gate in front of it. Without that hook every
 * request reaches the route anonymously, and under the default cross-origin
 * rules a plain form post from any page would be enough to trigger one, so the
 * refusal comes at startup.
 *
 * @param acceptBackupRestore - Whether the operator opened the restore route.
 * @param hasAuthenticate - Whether the operator supplied an authenticate hook.
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
