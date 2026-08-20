import type { FileHandle } from 'node:fs/promises'
import { SirannonError } from '../errors.js'

/**
 * Writes a run of bytes to a file and repeats the call until every one of them
 * is written. A single write can stop short once the disk has no room left, so
 * this reports an error there instead of returning with part of the run
 * unwritten.
 *
 * @param handle - The open file.
 * @param path - Path of that file, which the error names.
 * @param bytes - The bytes to write.
 * @param byteLength - How many of them to write, counted from the front.
 * @param offset - Where in the file they go.
 * @throws A `BACKUP_ERROR` where the file stops accepting bytes.
 *
 * @internal
 */
export async function writeFully(
  handle: FileHandle,
  path: string,
  bytes: Uint8Array,
  byteLength: number,
  offset: number,
): Promise<void> {
  let written = 0
  while (written < byteLength) {
    const result = await handle.write(bytes, written, byteLength - written, offset + written)
    if (result.bytesWritten === 0) {
      throw new SirannonError(
        `Writing to '${path}' stopped after ${written} of ${byteLength} bytes. Check the free space and the permissions on that directory.`,
        'BACKUP_ERROR',
      )
    }
    written += result.bytesWritten
  }
}
