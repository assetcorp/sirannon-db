import { open } from 'node:fs/promises'
import {
  LOG_HEADER_BYTES,
  type LogChecksum,
  type LogHeader,
  logFrameOffset,
  readLogHeader,
  readValidLogFrame,
} from './wal-format.js'

const READ_CHUNK_BYTES = 4 * 1024 * 1024

/** Where in the write-ahead log one capture stopped.
 * @internal
 */
export interface LogCursor {
  /** Checkpoint sequence of the log these frames belong to. */
  logSequence: number
  /** First salt of that same log. */
  salt1: number
  /** Second salt of it. */
  salt2: number
  /** The last frame taken, counted from one. */
  lastFrame: number
  /** First half of the running checksum at that frame, which the next capture starts from. */
  checksum1: number
  /** Second half of it. */
  checksum2: number
  /** Whether the checkpoint after this capture emptied the log. */
  checkpointed: boolean
}

/** How far down a log the live frames run.
 * @internal
 */
export interface LogScan {
  /** The last frame that commits a transaction. Where the walk found none, this stays where it began. */
  lastCommitFrame: number
  /** The byte just past that frame. */
  endOffset: number
  /** The running checksum at it. */
  checksum: LogChecksum
}

async function openForReading(path: string): Promise<Awaited<ReturnType<typeof open>> | undefined> {
  try {
    return await open(path, 'r')
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return undefined
    throw err
  }
}

/**
 * Reads the header of a database's write-ahead log. SQLite names that file
 * after the database file, and truncates it to nothing at a checkpoint, so an
 * absent or empty file is ordinary and comes back as undefined.
 *
 * @param logPath - Path of the log file.
 * @returns The header, or undefined where there is no readable log there.
 */
export async function readLogFileHeader(logPath: string): Promise<LogHeader | undefined> {
  const file = await openForReading(logPath)
  if (!file) return undefined
  try {
    const buffer = Buffer.allocUnsafe(LOG_HEADER_BYTES)
    const { bytesRead } = await file.read(buffer, 0, LOG_HEADER_BYTES, 0)
    if (bytesRead < LOG_HEADER_BYTES) return undefined
    return readLogHeader(buffer)
  } finally {
    await file.close()
  }
}

/**
 * Walks a log from a frame you name and finds the last frame after it that
 * commits a transaction. A capture stops there, so it never takes half of a
 * transaction. Frames past that point are either uncommitted or left over from
 * a rolled-back transaction, and the checksum chain is what tells them apart
 * from live ones.
 *
 * @param logPath - Path of the log file.
 * @param header - Header of that log.
 * @param from - The frame to walk on from, and the checksum it left.
 * @returns Where the live frames end, and the checksum there.
 */
export async function scanLogFrames(
  logPath: string,
  header: LogHeader,
  from: { frame: number; checksum: LogChecksum },
): Promise<LogScan> {
  const stopped: LogScan = {
    lastCommitFrame: from.frame,
    endOffset: logFrameOffset(from.frame + 1, header.frameBytes),
    checksum: from.checksum,
  }

  const file = await openForReading(logPath)
  if (!file) return stopped

  try {
    const { size } = await file.stat()
    const framesInFile = Math.max(0, Math.floor((size - LOG_HEADER_BYTES) / header.frameBytes))
    const framesPerChunk = Math.max(1, Math.floor(READ_CHUNK_BYTES / header.frameBytes))
    const buffer = Buffer.allocUnsafe(framesPerChunk * header.frameBytes)
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)

    let running = from.checksum
    let found = stopped
    let frame = from.frame + 1

    while (frame <= framesInFile) {
      const frames = Math.min(framesPerChunk, framesInFile - frame + 1)
      const wanted = frames * header.frameBytes
      let filled = 0
      while (filled < wanted) {
        const { bytesRead } = await file.read(
          buffer,
          filled,
          wanted - filled,
          logFrameOffset(frame, header.frameBytes) + filled,
        )
        if (bytesRead === 0) break
        filled += bytesRead
      }
      const readable = Math.floor(filled / header.frameBytes)
      if (readable === 0) break

      for (let inChunk = 0; inChunk < readable; inChunk++) {
        const read = readValidLogFrame(view, inChunk * header.frameBytes, header, running)
        if (!read) return found
        running = read.checksum
        if (read.frame.databasePages !== 0) {
          found = {
            lastCommitFrame: frame + inChunk,
            endOffset: logFrameOffset(frame + inChunk + 1, header.frameBytes),
            checksum: running,
          }
        }
      }
      frame += readable
    }
    return found
  } finally {
    await file.close()
  }
}

/**
 * Copies a run of bytes out of the log into a file of its own. The checkpoint
 * that follows a capture empties the log, so the frames have to be somewhere
 * else by then.
 *
 * @param logPath - Path of the log file.
 * @param startOffset - First byte to copy.
 * @param endOffset - The byte just past the last one to copy.
 * @param destPath - Where to write them.
 * @returns How many bytes it wrote.
 */
export async function copyLogRange(
  logPath: string,
  startOffset: number,
  endOffset: number,
  destPath: string,
): Promise<number> {
  const source = await open(logPath, 'r')
  try {
    const dest = await open(destPath, 'w')
    try {
      const buffer = Buffer.allocUnsafe(Math.min(READ_CHUNK_BYTES, Math.max(endOffset - startOffset, 1)))
      let at = startOffset
      let written = 0
      while (at < endOffset) {
        const wanted = Math.min(buffer.byteLength, endOffset - at)
        const { bytesRead } = await source.read(buffer, 0, wanted, at)
        if (bytesRead === 0) break
        await dest.write(buffer, 0, bytesRead)
        at += bytesRead
        written += bytesRead
      }
      return written
    } finally {
      await dest.close()
    }
  } finally {
    await source.close()
  }
}

/**
 * Puts a cursor's two checksum halves back into the pair the frame walk starts from.
 *
 * @param cursor - Where the previous capture stopped.
 * @returns Its checksum.
 */
export function cursorChecksum(cursor: LogCursor): LogChecksum {
  return { first: cursor.checksum1, second: cursor.checksum2 }
}

/**
 * Tells you whether the log on disk is still the one a cursor came from. SQLite
 * changes both salts every time it restarts a log, so matching salts mean the
 * frames on disk continue the ones already captured.
 *
 * @param header - Header of the log now on disk.
 * @param cursor - Where the previous capture stopped.
 * @returns Whether it is the same log.
 */
export function sameLog(header: LogHeader, cursor: LogCursor): boolean {
  return header.salt1 === cursor.salt1 && header.salt2 === cursor.salt2
}
