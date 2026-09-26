/** One piece of a backup file at a destination.
 * @public
 */
export interface BackupPiece {
  /** The position of this piece in the file, counted in whole pieces from zero. */
  index: number
  /** The size of this piece, in bytes. */
  byteLength: number
}

/**
 * The storage that Sirannon writes backup bytes to and reads them back from.
 * Sirannon includes no storage client, so you supply the three required
 * operations and connect object storage, another machine, or any other store
 * of bytes. Every piece except the last holds the same number of bytes, and
 * Sirannon can write the pieces in any order, because SQLite writes page one
 * last. A backup chain uses several
 * names, since Sirannon stores its full copy, each change piece, and the list
 * of its records under names of their own.
 *
 * @public
 */
export interface BackupDestination {
  /**
   * Stores one piece of a named file. A second write to the same name and index
   * must replace the piece already there, because when Sirannon stops between
   * storing a piece and recording it, it stores that piece again when it
   * resumes.
   */
  writePiece(name: string, index: number, bytes: Uint8Array): Promise<void>
  /**
   * Stores one piece only where no piece exists at that name and index, and
   * resolves to true when this call stored it. Sirannon claims each index in
   * its list of chains through this method, so that two nodes writing at the
   * same moment keep both of their chains. When you leave it out, Sirannon
   * writes the record and reads it back, which loses a chain when the other
   * node writes between those two calls.
   */
  writePieceIfAbsent?(name: string, index: number, bytes: Uint8Array): Promise<boolean>
  /** Returns one piece of a named file. */
  readPiece(name: string, index: number): Promise<Uint8Array>
  /** Returns every piece of a named file, in any order. */
  listPieces(name: string): Promise<BackupPiece[]>
}
