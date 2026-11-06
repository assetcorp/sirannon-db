import type { SQLiteDriver } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { EncryptionError } from './errors.js'
import {
  FILE_IDENTITY_BYTES,
  keyChangeFilePath,
  keyChangeMarkerPath,
  removeKeyChangeFile,
  sameBytes,
} from './key-change-files.js'
import { flushDirectoryThroughMarker, readKeyChangeMarker, removeKeyChangeMarker } from './key-change-marker.js'

/**
 * Which key the database file needs after an open found the marker of a key change that stopped part way.
 *
 * @internal
 */
export type InterruptedKeyChange = 'key-before-change' | 'key-after-change'

function missingDatabaseFile(path: string): SirannonError {
  return new SirannonError(
    `The database file ${path} is missing, and the marker of an interrupted key change does not show that the new file can take its place, so Sirannon leaves every file as it is`,
    'DATABASE_OPEN_FAILED',
  )
}

/**
 * Finishes the cleanup of a key change that stopped part way, following what its marker records.
 *
 * When the database file starts with the bytes in the marker, the new file replaced the original, and the open
 * deletes any new file left behind. When the database file is missing, the marker records that the replacement
 * began, and the new file starts with the marker's bytes, the open moves the new file into place. Any other missing
 * database file fails the open and leaves every file as it is. In every other case the original stands, and the open
 * deletes the new file. The open deletes the marker after each of these steps except the failure. A read-only open
 * changes no file and only learns which key the file needs. When the driver's file operations report
 * `ENCRYPTION_UNSUPPORTED`, the driver can run no key change, so the open finds none to finish.
 *
 * @param driver - The driver, whose file operations and connections the recovery uses.
 * @param path - The database file.
 * @param readOnly - Whether the open is read-only.
 * @returns Which key the file needs, or `null` when no key change stopped part way.
 * @throws A `SirannonError` with `DATABASE_OPEN_FAILED` when the database file is missing and the new file cannot
 * take its place.
 * @internal
 */
export async function recoverInterruptedKeyChange(
  driver: SQLiteDriver,
  path: string,
  readOnly: boolean,
): Promise<InterruptedKeyChange | null> {
  const files = driver.files
  if (files === undefined || path === ':memory:' || path === '' || path.startsWith('file:')) return null
  let markerStart: Uint8Array | null
  try {
    markerStart = await files.readStart(keyChangeMarkerPath(path), FILE_IDENTITY_BYTES)
  } catch (err) {
    if (err instanceof EncryptionError && err.code === 'ENCRYPTION_UNSUPPORTED') return null
    throw err
  }
  if (markerStart === null) {
    if (!readOnly) await removeKeyChangeFile(files, path)
    return null
  }
  const marker = await readKeyChangeMarker(driver, path, readOnly)
  const current = await files.readStart(path, FILE_IDENTITY_BYTES)
  if (current === null) {
    const newFile = await files.readStart(keyChangeFilePath(path), FILE_IDENTITY_BYTES)
    const movable = marker?.replacementBegun === true && newFile !== null && sameBytes(marker.identity, newFile)
    if (!movable) throw missingDatabaseFile(path)
    if (readOnly) return 'key-after-change'
    await files.rename(keyChangeFilePath(path), path)
    await flushDirectoryThroughMarker(driver, path)
    await removeKeyChangeMarker(files, path)
    return 'key-after-change'
  }
  const replaced = marker !== null && current.length === FILE_IDENTITY_BYTES && sameBytes(marker.identity, current)
  if (readOnly) return replaced ? 'key-after-change' : 'key-before-change'
  await removeKeyChangeFile(files, path)
  await removeKeyChangeMarker(files, path)
  return replaced ? 'key-after-change' : 'key-before-change'
}

const KEY_ERROR_CODES = new Set(['ENCRYPTION_KEY_REJECTED', 'ENCRYPTION_KEY_MISSING', 'DATABASE_NOT_ENCRYPTED'])

/**
 * Turns a key failure on the first open after an interrupted key change into `ENCRYPTION_KEY_REJECTED`, stating which
 * key the file needs, and returns every other error unchanged.
 *
 * @param err - The error that the open raised.
 * @param interrupted - Which key the file needs.
 * @returns The error to raise.
 * @internal
 */
export function explainKeyAfterInterruptedChange(err: unknown, interrupted: InterruptedKeyChange): unknown {
  if (!(err instanceof EncryptionError) || !KEY_ERROR_CODES.has(err.code)) return err
  const needed =
    interrupted === 'key-after-change'
      ? 'the key from after the change, or no key where the change removed encryption, because the new file replaced the original'
      : 'the key from before the change, or no key where the file was plaintext, because the change stopped before the new file replaced the original'
  return new EncryptionError(
    `A key change on this database stopped part way, and the file needs ${needed}`,
    'ENCRYPTION_KEY_REJECTED',
    err,
  )
}
