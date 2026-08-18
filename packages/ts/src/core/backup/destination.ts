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
 * last. A backup uses more than one name, since SQLite opens a journal file
 * beside the database file it writes.
 *
 * @public
 */
export interface BackupDestination {
  /** Stores one piece of a named file. */
  writePiece(name: string, index: number, bytes: Uint8Array): Promise<void>
  /** Returns one piece of a named file. */
  readPiece(name: string, index: number): Promise<Uint8Array>
  /** Returns every piece a named file has, in any order. */
  listPieces(name: string): Promise<BackupPiece[]>
}
