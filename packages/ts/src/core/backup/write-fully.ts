import type { FileHandle } from 'node:fs/promises'
import { SirannonError } from '../errors.js'

/**
 * Writes a range of bytes to a file, and repeats the write call until the file
 * holds every byte, since a single call can write only part of the range. It
 * throws a `BACKUP_ERROR` when a call writes no bytes at all, so that the loop
 * ends once the writes stop making progress.
 *
 * @param handle - The open file.
 * @param path - The path of that file, which the error message quotes.
 * @param bytes - The bytes to write.
 * @param byteLength - The number of bytes to write, counted from the start of `bytes`.
 * @param offset - The position in the file to write them at.
 * @throws A `BACKUP_ERROR` where a write call writes no bytes.
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
