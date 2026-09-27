import type { SQLiteConnection } from '../driver/types.js'

const SELECT_NEW_STREAM_ID = 'SELECT sirannon_stream_open(?, ?, ?, ?) AS streamId'
const SELECT_NEXT_PIECE = 'SELECT sirannon_stream_take(?) AS piece'
const SELECT_QUEUED_PIECES = 'SELECT sirannon_stream_taker_seen(?) AS queued'
const SELECT_BYTES_WRITTEN = 'SELECT sirannon_stream_written(?) AS bytes'
const SELECT_FAILURE = 'SELECT sirannon_stream_error(?) AS failure'
const SELECT_FINISHED_BYTES = 'SELECT sirannon_stream_finish(?) AS bytes'
const SELECT_RELEASED_BYTES = 'SELECT sirannon_stream_close(?) AS bytes'

/**
 * Holds the prepared statements that a streamed backup calls on the streaming
 * extension. Sirannon calls some of them once per piece, so it prepares each
 * one before the backup starts.
 *
 * @internal
 */
export interface BackupStreamStatements {
  /** Opens a stream and returns its ID, which the destination URI includes. */
  selectNewStreamId(
    pieceBytes: number,
    maxQueuedPieces: number,
    waitWhenFull: number,
    stoppedTakerMicroseconds: number,
  ): Promise<number>
  /** Returns the next whole piece of the copy, or `null` while the queue is empty. */
  selectNextPiece(streamId: number): Promise<Uint8Array | null>
  /** Signals to the extension that the caller is still taking pieces, and returns the number of pieces in the queue. */
  selectQueuedPieces(streamId: number): Promise<number>
  /** Returns the current size in bytes of the file that SQLite writes through a stream. */
  selectBytesWritten(streamId: number): Promise<number>
  /** Returns the failure message for a stream, or `null` when the stream has none. */
  selectFailure(streamId: number): Promise<string | null>
  /** Closes a stream to further writes, queues the bytes that the stream still buffers, and returns the size of the file in bytes. */
  selectFinishedBytes(streamId: number): Promise<number>
  /** Releases a stream and returns the size of its file in bytes. */
  selectReleasedBytes(streamId: number): Promise<number>
}

/**
 * Prepares the statements that a streamed backup calls on the extension, and
 * keeps their SQL in this module.
 *
 * @param conn - The connection into which the caller loaded the extension.
 * @returns The prepared statements, each of which returns one value from the extension.
 */
export async function prepareBackupStreamStatements(conn: SQLiteConnection): Promise<BackupStreamStatements> {
  const [newStreamId, nextPiece, queuedPieces, bytesWritten, failure, finishedBytes, releasedBytes] = await Promise.all(
    [
      conn.prepare(SELECT_NEW_STREAM_ID),
      conn.prepare(SELECT_NEXT_PIECE),
      conn.prepare(SELECT_QUEUED_PIECES),
      conn.prepare(SELECT_BYTES_WRITTEN),
      conn.prepare(SELECT_FAILURE),
      conn.prepare(SELECT_FINISHED_BYTES),
      conn.prepare(SELECT_RELEASED_BYTES),
    ],
  )

  const readBytes = async (
    stmt: Awaited<ReturnType<SQLiteConnection['prepare']>>,
    streamId: number,
  ): Promise<number> => {
    const row = await stmt.get<{ bytes: number | bigint }>(streamId)
    return row ? Number(row.bytes) : 0
  }

  return {
    async selectNewStreamId(pieceBytes, maxQueuedPieces, waitWhenFull, stoppedTakerMicroseconds) {
      const row = await newStreamId.get<{ streamId: number | bigint }>(
        pieceBytes,
        maxQueuedPieces,
        waitWhenFull,
        stoppedTakerMicroseconds,
      )
      return row ? Number(row.streamId) : 0
    },
    async selectNextPiece(streamId) {
      const row = await nextPiece.get<{ piece: Uint8Array | null }>(streamId)
      return row?.piece ?? null
    },
    async selectQueuedPieces(streamId) {
      const row = await queuedPieces.get<{ queued: number | bigint }>(streamId)
      return row ? Number(row.queued) : 0
    },
    selectBytesWritten: streamId => readBytes(bytesWritten, streamId),
    async selectFailure(streamId) {
      const row = await failure.get<{ failure: string | null }>(streamId)
      return row?.failure ?? null
    },
    selectFinishedBytes: streamId => readBytes(finishedBytes, streamId),
    selectReleasedBytes: streamId => readBytes(releasedBytes, streamId),
  }
}
