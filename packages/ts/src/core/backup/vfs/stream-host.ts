import type { SQLiteConnection } from '../../driver/types.js'
import { SirannonError } from '../../errors.js'
import {
  type BackupStreamStatements,
  prepareBackupStreamStatements,
} from '../../system-catalog/backup-stream-functions.js'

const PIECE_HEADER_BYTES = 8

/** One whole piece of a copy, which Sirannon takes from the extension and sends to the destination.
 * @internal
 */
export interface BackupStreamPiece {
  /** The position of this piece in the file, counted in whole pieces from zero. */
  index: number
  /** The bytes of this piece. */
  bytes: Uint8Array
}

function decodePiece(framed: Uint8Array): BackupStreamPiece {
  if (framed.byteLength < PIECE_HEADER_BYTES) {
    throw new SirannonError(
      `The streaming extension returned ${framed.byteLength} bytes for a piece, which is less than its ${PIECE_HEADER_BYTES}-byte header`,
      'BACKUP_ERROR',
    )
  }
  const header = new DataView(framed.buffer, framed.byteOffset, PIECE_HEADER_BYTES)
  const index = header.getUint32(0, true)
  const length = header.getUint32(4, true)
  const carried = framed.byteLength - PIECE_HEADER_BYTES
  if (carried < length) {
    throw new SirannonError(
      `The streaming extension declared ${length} bytes for piece ${index} and returned ${carried}`,
      'BACKUP_ERROR',
    )
  }
  return { index, bytes: framed.subarray(PIECE_HEADER_BYTES, PIECE_HEADER_BYTES + length) }
}

/**
 * Runs the statements of the extension on a separate connection, so that the
 * copy and its pieces share no statement with the database that SQLite copies.
 * The extension registers its virtual file system once per process and keeps
 * it after this connection closes, so a later backup uses the same virtual
 * file system.
 *
 * @internal
 */
export class BackupStreamHost {
  private constructor(
    private readonly connection: SQLiteConnection,
    private readonly statements: BackupStreamStatements,
  ) {}

  /**
   * Opens a connection, loads the compiled extension into it, and prepares the
   * statements that one backup needs.
   *
   * @param openConnection - Opens the connection that Sirannon runs the statements on.
   * @param extensionPath - The absolute path of the compiled extension.
   * @returns A host that is ready to open a stream.
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
   * Opens one stream and returns the identifier that appears in the
   * destination URI.
   *
   * @param pieceBytes - The size of one whole piece, in bytes.
   * @param maxQueuedPieces - The number of queued pieces at which the extension pauses the copy, when `waitWhenFull` is true.
   * @param waitWhenFull - Whether the extension pauses the copy while the queue is full. When false, the extension queues every piece without a limit.
   * @param stoppedTakerMicroseconds - The time in microseconds without a report from {@link BackupStreamHost.reportStillTaking} after which the extension lets one piece past the full queue.
   * @returns The identifier of the open stream.
   */
  async open(
    pieceBytes: number,
    maxQueuedPieces: number,
    waitWhenFull: boolean,
    stoppedTakerMicroseconds: number,
  ): Promise<number> {
    const streamId = await this.statements.selectNewStreamId(
      pieceBytes,
      maxQueuedPieces,
      waitWhenFull ? 1 : 0,
      stoppedTakerMicroseconds,
    )
    if (streamId === 0) {
      throw new SirannonError('The streaming extension opened no stream for this run', 'BACKUP_ERROR')
    }
    return streamId
  }

  /**
   * Takes the next whole piece of the copy from the queue.
   *
   * @param streamId - The stream to take from.
   * @returns The piece, or null where the queue is empty.
   */
  async take(streamId: number): Promise<BackupStreamPiece | null> {
    const framed = await this.statements.selectNextPiece(streamId)
    return framed ? decodePiece(framed) : null
  }

  /**
   * Tells the extension that Sirannon is still taking pieces. SQLite holds the
   * lock of the database for a whole copy step, so if Sirannon stopped taking
   * pieces while the copy waited on a full queue, every other statement on that
   * database would wait behind that step. The extension therefore lets one
   * piece past the full queue once these reports stop.
   *
   * @param streamId - The stream to report on.
   * @returns The number of pieces in the queue.
   */
  reportStillTaking(streamId: number): Promise<number> {
    return this.statements.selectQueuedPieces(streamId)
  }

  /**
   * Returns the number of bytes that SQLite has written to a stream.
   *
   * @param streamId - The stream to query.
   * @returns The number of bytes that SQLite has written to this stream.
   */
  written(streamId: number): Promise<number> {
    return this.statements.selectBytesWritten(streamId)
  }

  /**
   * Returns the failure that stopped a stream, if any.
   *
   * @param streamId - The stream to query.
   * @returns The failure that the extension recorded, or null where it recorded none.
   */
  failure(streamId: number): Promise<string | null> {
    return this.statements.selectFailure(streamId)
  }

  /**
   * Closes the file to further writes and queues the two pieces that the
   * extension still holds, which are the first piece and the most recent one.
   *
   * @param streamId - The stream to finish.
   * @returns The size of the finished file, in bytes.
   */
  finish(streamId: number): Promise<number> {
    return this.statements.selectFinishedBytes(streamId)
  }

  /**
   * Releases a stream and every piece that it still holds.
   *
   * @param streamId - The stream to release.
   */
  async close(streamId: number): Promise<void> {
    await this.statements.selectReleasedBytes(streamId)
  }

  /** Closes the connection that Sirannon runs the statements on. */
  async stop(): Promise<void> {
    await this.connection.close()
  }
}
