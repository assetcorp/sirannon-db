import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import type { BackupChainChange, BackupChainPosition } from '../../backup/chain.js'
import { assertChangePiecesRunOn } from '../../backup/restore-apply.js'
import { RestoreLogWriter, readDatabaseHeader } from '../../backup/restore-log.js'
import type { SirannonError } from '../../errors.js'
import { createTestDb, tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()

function pieceAt(sequence: number, position: Partial<BackupChainPosition>): BackupChainChange {
  return {
    kind: 'change',
    chainId: 'cccccccccccccccc',
    name: `piece-${sequence}.wal`,
    runId: 'rrrrrrrrrrrrrrrr',
    sequence,
    position: {
      logSequence: 1,
      salt1: 100,
      salt2: 200,
      firstFrame: 1,
      lastFrame: 1,
      ...position,
    },
    capturedAt: sequence,
    frameCount: 1,
    pieceCount: 1,
    pieceBytes: 16,
    bytesWritten: 16,
    checkpointed: false,
  }
}

function refusal(run: () => void): SirannonError {
  try {
    run()
  } catch (err) {
    return err as SirannonError
  }
  throw new Error('the chain was accepted')
}

describe('assertChangePiecesRunOn', () => {
  it('accepts pieces that meet frame by frame down one run of the log', () => {
    const pieces = [
      pieceAt(1, { firstFrame: 1, lastFrame: 4 }),
      pieceAt(2, { firstFrame: 5, lastFrame: 9 }),
      pieceAt(3, { firstFrame: 10, lastFrame: 12 }),
    ]

    expect(() => assertChangePiecesRunOn(pieces, 'cccccccccccccccc')).not.toThrow()
  })

  it('accepts a piece that opens a fresh run at frame one', () => {
    const pieces = [
      pieceAt(1, { firstFrame: 1, lastFrame: 4 }),
      pieceAt(2, { logSequence: 2, salt1: 101, salt2: 201, firstFrame: 1, lastFrame: 2 }),
    ]

    expect(() => assertChangePiecesRunOn(pieces, 'cccccccccccccccc')).not.toThrow()
  })

  it('names the piece the frames stop short of', () => {
    const pieces = [pieceAt(1, { firstFrame: 1, lastFrame: 4 }), pieceAt(2, { firstFrame: 7, lastFrame: 9 })]

    const error = refusal(() => assertChangePiecesRunOn(pieces, 'cccccccccccccccc'))

    expect(error.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(error.message).toContain('Change piece 2')
    expect(error.message).toContain('starts at frame 7')
    expect(error.message).toContain('frame 5')
  })

  it('refuses a first piece that starts past the head of the log', () => {
    const error = refusal(() => assertChangePiecesRunOn([pieceAt(1, { firstFrame: 3 })], 'cccccccccccccccc'))

    expect(error.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(error.message).toContain('the full copy underneath it')
  })

  it('refuses a fresh run that starts past frame one', () => {
    const pieces = [
      pieceAt(1, { firstFrame: 1, lastFrame: 4 }),
      pieceAt(2, { logSequence: 2, salt1: 101, salt2: 201, firstFrame: 6, lastFrame: 8 }),
    ]

    const error = refusal(() => assertChangePiecesRunOn(pieces, 'cccccccccccccccc'))

    expect(error.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(error.message).toContain('frame 1')
  })
})

describe('readDatabaseHeader', () => {
  it('reads the page size and the journalling format of a real database', async () => {
    const conn = await createTestDb(temp.path)
    await conn.close()

    const header = await readDatabaseHeader(join(temp.path, 'source.db'))

    expect(header.pageSize).toBeGreaterThanOrEqual(512)
    expect(header.walMode).toBe(true)
  })

  it('refuses a file too short to be a database', async () => {
    const path = join(temp.path, 'stub.db')
    await writeFile(path, new Uint8Array(10))

    const error = await readDatabaseHeader(path).catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
  })
})

describe('RestoreLogWriter', () => {
  it('refuses pieces that end part-way through a frame', async () => {
    const writer = await RestoreLogWriter.create(join(temp.path, 'partial.db-wal'), 512, 1)
    writer.beginPiece(0)
    await writer.add(new Uint8Array(writer.frameBytes + 8))

    const error = await writer.finish().catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect((error as SirannonError).message).toContain('8 bytes into a frame')
  })

  it('refuses a batch whose last frame commits nothing', async () => {
    const writer = await RestoreLogWriter.create(join(temp.path, 'uncommitted.db-wal'), 512, 1)
    writer.beginPiece(0)
    await writer.add(new Uint8Array(writer.frameBytes))

    const error = await writer.finish().catch((err: unknown) => err)

    expect((error as SirannonError).code).toBe('BACKUP_CHAIN_BROKEN')
    expect((error as SirannonError).message).toContain('commits no transaction')
  })
})
