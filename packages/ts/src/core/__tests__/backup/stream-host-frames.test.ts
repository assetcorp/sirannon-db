import { describe, expect, it } from 'vitest'
import { BackupStreamHost } from '../../backup/vfs/stream-host.js'
import type { SQLiteConnection, SQLiteStatement } from '../../driver/types.js'

const PIECE_HEADER_BYTES = 8
const TAKE_SQL = 'SELECT sirannon_stream_take(?) AS piece'

function frame(index: number, declaredLength: number, carried: number): Uint8Array {
  const framed = new Uint8Array(PIECE_HEADER_BYTES + carried)
  const header = new DataView(framed.buffer)
  header.setUint32(0, index, true)
  header.setUint32(4, declaredLength, true)
  framed.fill(7, PIECE_HEADER_BYTES)
  return framed
}

function connectionReturning(framed: Uint8Array): SQLiteConnection {
  const statement = (sql: string): SQLiteStatement =>
    ({
      all: async () => [],
      get: async () => (sql === TAKE_SQL ? { piece: framed } : { streamId: 1n, bytes: 0n, failure: null }),
      run: async () => ({ changes: 0, lastInsertRowid: 0 }),
    }) as unknown as SQLiteStatement
  return {
    prepare: async (sql: string) => statement(sql),
    loadExtension: async () => undefined,
    close: async () => undefined,
  } as unknown as SQLiteConnection
}

async function takeOnePiece(framed: Uint8Array): Promise<{ index: number; bytes: Uint8Array } | null> {
  const host = await BackupStreamHost.start(async () => connectionReturning(framed), '/tmp/sirannonvfs.dylib')
  return host.take(1)
}

describe('BackupStreamHost.take', () => {
  it('returns the bytes a whole frame carries', async () => {
    await expect(takeOnePiece(frame(3, 16, 16))).resolves.toMatchObject({ index: 3 })
  })

  it('refuses a frame shorter than its header', async () => {
    await expect(takeOnePiece(new Uint8Array(4))).rejects.toMatchObject({
      code: 'BACKUP_ERROR',
      message: expect.stringContaining('less than its 8-byte header'),
    })
  })

  it('refuses a frame that declares more bytes than it carries', async () => {
    await expect(takeOnePiece(frame(2, 4096, 512))).rejects.toMatchObject({
      code: 'BACKUP_ERROR',
      message: 'The streaming extension declared 4096 bytes for piece 2 and returned 512',
    })
  })
})
