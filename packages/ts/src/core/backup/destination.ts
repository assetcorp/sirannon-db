/** One piece of a backup that a destination holds.
 * @public
 */
export interface BackupPiece {
  /** Position of this piece in the file, counted in whole pieces from zero. */
  index: number
  /** Bytes this piece holds. */
  byteLength: number
}

/**
 * Where Sirannon puts backup bytes and reads them back from. Sirannon carries
 * no storage client, so a caller supplies these three operations and connects
 * object storage, another machine, or anything else that moves bytes. Pieces
 * are fixed in size and arrive in any order, because SQLite writes page one
 * last. A backup chain uses more than one name, since its full copy, each
 * change piece, and its own log are each stored under a name of their own.
 *
 * @public
 */
export interface BackupDestination {
  /**
   * Stores one piece of a named file. A second write to the same name and index
   * must replace the piece already there, because a run that stops between
   * storing a piece and recording it stores that piece again when it resumes.
   */
  writePiece(name: string, index: number, bytes: Uint8Array): Promise<void>
  /** Returns one piece of a named file. */
  readPiece(name: string, index: number): Promise<Uint8Array>
  /** Returns every piece a named file has, in any order. */
  listPieces(name: string): Promise<BackupPiece[]>
}
