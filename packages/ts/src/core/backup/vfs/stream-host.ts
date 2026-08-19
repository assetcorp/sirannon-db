import type { SQLiteConnection } from '../../driver/types.js'
import { SirannonError } from '../../errors.js'
import {
  type BackupStreamStatements,
  prepareBackupStreamStatements,
} from '../../system-catalog/backup-stream-functions.js'

const PIECE_HEADER_BYTES = 8

/** One whole piece of a copy, taken from the extension on its way to the destination.
 * @internal
 */
export interface BackupStreamPiece {
  /** Position of this piece in the file, counted in whole pieces from zero. */
  index: number
  /** Bytes this piece holds. */
  bytes: Uint8Array
}

function decodePiece(framed: Uint8Array): BackupStreamPiece {
  const header = new DataView(framed.buffer, framed.byteOffset, PIECE_HEADER_BYTES)
  const index = header.getUint32(0, true)
  const length = header.getUint32(4, true)
  return { index, bytes: framed.subarray(PIECE_HEADER_BYTES, PIECE_HEADER_BYTES + length) }
}

/**
 * Runs the extension's statements on a connection of its own, so the copy and
 * the pieces it produces never share a statement with the database being
 * copied. The extension registers its virtual file system once per process and
 * keeps it after this connection closes, so a later run reaches the same one.
 *
 * @internal
 */
export class BackupStreamHost {
  private constructor(
    private readonly connection: SQLiteConnection,
    private readonly statements: BackupStreamStatements,
  ) {}

  /**
   * Opens a connection, loads the compiled extension into it, and compiles the
   * statements one run needs.
   *
   * @param openConnection - Opens the connection the statements run on.
   * @param extensionPath - Absolute path of the compiled extension.
   * @returns A host ready to open a stream.
   */
  static async start(
    openConnection: () => Promise<SQLiteConnection>,
    extensionPath: string,
  ): Promise<BackupStreamHost> {
    const connection = await openConnection()
    try {
      if (!connection.loadExtension) {
        throw new SirannonError(
          'This driver opens connections with no extension loading call, so it cannot stream a copy to a destination',
          'BACKUP_UNSUPPORTED',
        )
      }
      await connection.loadExtension(extensionPath)
      return new BackupStreamHost(connection, await prepareBackupStreamStatements(connection))
    } catch (err) {
      await connection.close().catch(() => undefined)
      throw err
    }
  }

  /**
   * Opens one stream and returns the identifier that names it in the
   * destination URI.
   *
   * @param pieceBytes - Bytes one whole piece holds.
   * @param maxQueuedPieces - Pieces the extension holds before it stops taking more.
   * @param waitWhenFull - Whether the copy waits for the destination to catch up rather than queueing further pieces.
   * @returns The identifier of the open stream.
   */
  async open(pieceBytes: number, maxQueuedPieces: number, waitWhenFull: boolean): Promise<number> {
    const streamId = await this.statements.selectNewStreamId(pieceBytes, maxQueuedPieces, waitWhenFull ? 1 : 0)
    if (streamId === 0) {
      throw new SirannonError('The streaming extension opened no stream for this run', 'BACKUP_ERROR')
    }
    return streamId
  }

  /**
   * Takes the next whole piece the copy has produced.
   *
   * @param streamId - Stream to take from.
   * @returns The piece, or null where the copy has produced none since the last call.
   */
  async take(streamId: number): Promise<BackupStreamPiece | null> {
    const framed = await this.statements.selectNextPiece(streamId)
    return framed ? decodePiece(framed) : null
  }

  /**
   * Reports how many bytes of the copy have reached the extension.
   *
   * @param streamId - Stream to ask about.
   * @returns Bytes SQLite has written to this stream.
   */
  written(streamId: number): Promise<number> {
    return this.statements.selectBytesWritten(streamId)
  }

  /**
   * Reports what stopped a stream, where anything did.
   *
   * @param streamId - Stream to ask about.
   * @returns The failure the extension recorded, or null where it recorded none.
   */
  failure(streamId: number): Promise<string | null> {
    return this.statements.selectFailure(streamId)
  }

  /**
   * Closes the file to further writes and queues the pieces the extension was
   * still holding, which are the first piece and the last one.
   *
   * @param streamId - Stream to finish.
   * @returns Bytes the finished file holds.
   */
  finish(streamId: number): Promise<number> {
    return this.statements.selectFinishedBytes(streamId)
  }

  /**
   * Releases a stream and everything it still held.
   *
   * @param streamId - Stream to release.
   */
  async close(streamId: number): Promise<void> {
    await this.statements.selectReleasedBytes(streamId)
  }

  /** Closes the connection the statements ran on. */
  async stop(): Promise<void> {
    await this.connection.close()
  }
}
