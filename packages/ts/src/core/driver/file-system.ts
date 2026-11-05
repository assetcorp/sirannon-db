/**
 * The file operations that a key change and its recovery perform on a database's files, which a driver for a runtime
 * with a file system supplies.
 *
 * When the runtime lacks a module that the operations need, each one can throw an `EncryptionError` with
 * `ENCRYPTION_UNSUPPORTED`. A key change then stops before it writes any file, and an open finds no key change to
 * finish.
 *
 * @public
 */
export interface DatabaseFileSystem {
  /**
   * Returns the first bytes of a file, fewer when the file is shorter.
   *
   * @param path - The file to read.
   * @param length - The most bytes to return.
   * @returns The bytes, or `null` when the file does not exist.
   */
  readStart(path: string, length: number): Promise<Uint8Array | null>
  /**
   * Deletes a file, and resolves without an error when the file does not exist.
   *
   * @param path - The file to delete.
   */
  remove(path: string): Promise<void>
  /**
   * Moves a file to a new path in the same directory, replacing any file at that path.
   *
   * Where the file system replaces a file in one rename, the move takes that one step. Where it cannot, the move
   * deletes the file at the new path and then renames the file, so a crash between the two steps can leave neither
   * file at the new path. Sirannon recovers from that state on the next open.
   *
   * @param from - The file to move.
   * @param to - The path that the file takes.
   */
  rename(from: string, to: string): Promise<void>
}
