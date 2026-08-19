import type { SQLiteConnection } from '../driver/types.js'

const SELECT_NEW_STREAM_ID = 'SELECT sirannon_stream_open(?, ?, ?) AS streamId'
const SELECT_NEXT_PIECE = 'SELECT sirannon_stream_take(?) AS piece'
const SELECT_BYTES_WRITTEN = 'SELECT sirannon_stream_written(?) AS bytes'
const SELECT_FAILURE = 'SELECT sirannon_stream_error(?) AS failure'
const SELECT_FINISHED_BYTES = 'SELECT sirannon_stream_finish(?) AS bytes'
const SELECT_RELEASED_BYTES = 'SELECT sirannon_stream_close(?) AS bytes'

/**
 * The statements one backup run needs from the streaming extension. Sirannon
 * runs some of them once per piece, so each one is compiled ahead of the run.
 *
 * @internal
 */
export interface BackupStreamStatements {
  /** Opens a stream and returns the identifier that names it in the destination URI. */
  selectNewStreamId(pieceBytes: number, maxQueuedPieces: number, waitWhenFull: number): Promise<number>
  /** Returns the next whole piece the copy has produced, or null where it has produced none. */
  selectNextPiece(streamId: number): Promise<Uint8Array | null>
  /** Returns the bytes SQLite has written to a stream. */
  selectBytesWritten(streamId: number): Promise<number>
  /** Returns what stopped a stream, or null where nothing did. */
  selectFailure(streamId: number): Promise<string | null>
  /** Closes a stream to further writes, queues what it still held, and returns the bytes the file holds. */
  selectFinishedBytes(streamId: number): Promise<number>
  /** Releases a stream and returns the bytes it carried. */
  selectReleasedBytes(streamId: number): Promise<number>
}

/**
 * Compiles the statements a streamed backup runs against the extension, so the
 * run itself carries no SQL.
 *
 * @param conn - Connection the extension is loaded into.
 * @returns The compiled statements, each returning the value its question asks for.
 */
export async function prepareBackupStreamStatements(conn: SQLiteConnection): Promise<BackupStreamStatements> {
  const [newStreamId, nextPiece, bytesWritten, failure, finishedBytes, releasedBytes] = await Promise.all([
    conn.prepare(SELECT_NEW_STREAM_ID),
    conn.prepare(SELECT_NEXT_PIECE),
    conn.prepare(SELECT_BYTES_WRITTEN),
    conn.prepare(SELECT_FAILURE),
    conn.prepare(SELECT_FINISHED_BYTES),
    conn.prepare(SELECT_RELEASED_BYTES),
  ])

  const readBytes = async (
    stmt: Awaited<ReturnType<SQLiteConnection['prepare']>>,
    streamId: number,
  ): Promise<number> => {
    const row = await stmt.get<{ bytes: number | bigint }>(streamId)
    return row ? Number(row.bytes) : 0
  }

  return {
    async selectNewStreamId(pieceBytes, maxQueuedPieces, waitWhenFull) {
      const row = await newStreamId.get<{ streamId: number | bigint }>(pieceBytes, maxQueuedPieces, waitWhenFull)
      return row ? Number(row.streamId) : 0
    },
    async selectNextPiece(streamId) {
      const row = await nextPiece.get<{ piece: Uint8Array | null }>(streamId)
      return row?.piece ?? null
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
