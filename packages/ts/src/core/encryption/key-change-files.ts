import type { DatabaseFileSystem } from '../driver/file-system.js'

/**
 * The number of bytes at the start of a database file that tell one file from another: the salt of an encrypted file,
 * or the header string of a plaintext one.
 *
 * @internal
 */
export const FILE_IDENTITY_BYTES = 16

const NEW_FILE_SUFFIX = '.sirannon-key-change'
const MARKER_SUFFIX = '.sirannon-key-change-marker'
const SIDE_FILE_SUFFIXES = ['', '-journal', '-wal', '-shm'] as const

/**
 * Returns the path of the file that a key change writes the database to before it replaces the original.
 *
 * @param path - The database file.
 * @returns The path of the new file.
 * @internal
 */
export function keyChangeFilePath(path: string): string {
  return `${path}${NEW_FILE_SUFFIX}`
}

/**
 * Returns the path of the marker that records the identity of the new file while a key change runs.
 *
 * @param path - The database file.
 * @returns The path of the marker.
 * @internal
 */
export function keyChangeMarkerPath(path: string): string {
  return `${path}${MARKER_SUFFIX}`
}

/**
 * Deletes the new file of a key change and the journal files that SQLite kept beside it.
 *
 * @param files - The driver's file operations.
 * @param path - The database file.
 * @internal
 */
export async function removeKeyChangeFile(files: DatabaseFileSystem, path: string): Promise<void> {
  const newFile = keyChangeFilePath(path)
  for (const suffix of SIDE_FILE_SUFFIXES) await files.remove(`${newFile}${suffix}`)
}

/**
 * Returns whether two byte arrays hold the same bytes.
 *
 * @param a - The first array.
 * @param b - The second array.
 * @returns `true` when both have the same length and bytes.
 * @internal
 */
export function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false
  return true
}
