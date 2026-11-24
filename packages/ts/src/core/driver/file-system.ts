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
   * Returns whether a file exists, without opening it.
   *
   * Sirannon checks a database file, its marker, and its new file through this call while SQLite can hold any of them
   * open in the same process. On a POSIX system, closing any descriptor on a file releases every lock that the process
   * holds on that file, so the check must answer from the file's metadata, as `stat` does, and never open and close a
   * descriptor.
   *
   * @param path - The file to look for.
   * @returns `true` when the file exists.
   */
  exists(path: string): Promise<boolean>
  /**
   * Returns the first bytes of a file, fewer when the file is shorter.
   *
   * Sirannon reads only a new file that a key change wrote, at a moment when no SQLite connection holds that file open.
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
