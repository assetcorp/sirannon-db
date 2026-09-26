import { open } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import type { BackupLogPosition } from './report.js'
import {
  LOG_HEADER_BYTES,
  type LogChecksum,
  type LogHeader,
  logFrameOffset,
  readLogHeader,
  readValidLogFrame,
} from './wal-format.js'

const READ_CHUNK_BYTES = 4 * 1024 * 1024

/** The point in the write-ahead log where one capture stopped.
 * @internal
 */
export interface LogCursor {
  /** The checkpoint sequence of the log that holds these frames. */
  logSequence: number
  /** The first salt of that log. */
  salt1: number
  /** The second salt of that log. */
  salt2: number
  /** The last frame that the capture copied, counted from one. */
  lastFrame: number
  /** The first half of the running checksum at that frame, which the next capture continues from. */
  checksum1: number
  /** The second half of that checksum. */
  checksum2: number
  /** Whether the checkpoint after this capture emptied the log. */
  checkpointed: boolean
}

/** The last committed frame in a log, and the offset and checksum after it.
 * @internal
 */
export interface LogScan {
  /** The last frame that commits a transaction, or the starting frame of the scan where the scan finds none. */
  lastCommitFrame: number
  /** The offset of the byte after that frame. */
  endOffset: number
  /** The running checksum at that frame. */
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
 * Reads the header of the write-ahead log of a database. SQLite names that file
 * after the database file and truncates it to zero bytes at a truncating
 * checkpoint, so a missing or empty file is normal, and this returns undefined
 * for it.
 *
 * @param logPath - The path of the log file.
 * @returns The header, or undefined where the path holds no readable log.
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
 * Scans a log from a given frame and returns the last later frame that commits
 * a transaction. A capture stops at that frame, so that it always copies whole
 * transactions. The frames after that point either hold an uncommitted
 * transaction or remain from a rolled-back one, and the checksum chain
 * separates them from valid frames.
 *
 * @param logPath - The path of the log file.
 * @param header - The header of that log.
 * @param from - The frame to scan on from, and the running checksum at that frame.
 * @returns The end of the committed frames, and the checksum there.
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
 * Copies a range of bytes from the log into a separate file. The checkpoint
 * after a capture empties the log, so Sirannon must copy the frames out before
 * that checkpoint.
 *
 * @param logPath - The path of the log file.
 * @param startOffset - The first byte to copy.
 * @param endOffset - The offset of the byte after the last byte to copy.
 * @param destPath - The path of the file to write.
 * @returns The number of bytes written, which always equals the length of the range.
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
        if (bytesRead === 0) {
          throw new SirannonError(
            `The write-ahead log '${logPath}' ends at byte ${at} while the frames captured from it run to byte ${endOffset}, so it lost frames while Sirannon was reading it. ` +
              'Another connection checkpointed the log. Route every write and every checkpoint through Sirannon, and take a fresh full copy so a new chain starts from a known state.',
            'BACKUP_LOG_REWOUND',
          )
        }
        let put = 0
        while (put < bytesRead) {
          const { bytesWritten } = await dest.write(buffer, put, bytesRead - put)
          if (bytesWritten === 0) {
            throw new SirannonError(
              `The capture of '${logPath}' could write no more than ${written + put} of its ${endOffset - startOffset} bytes into '${destPath}'. Check the free space and the permissions on that directory.`,
              'BACKUP_ERROR',
            )
          }
          put += bytesWritten
        }
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
 * Returns the path of the write-ahead log that SQLite keeps next to a database file.
 *
 * @param sourcePath - The path of the database file.
 * @returns The path of its log.
 *
 * @internal
 */
export function logPathFor(sourcePath: string): string {
  return `${sourcePath}-wal`
}

/**
 * Reads the position of the write-ahead log of a database at the moment of the
 * call. A full copy records this position, so that a reader can tell which
 * generation of the log the database was on when the copy finished.
 *
 * Sirannon reads the log after SQLite copies the last page. A writer can commit
 * between those two steps, in which case the frame named here is later than
 * the last frame in the copy, so treat this as the state of the log at one
 * moment.
 *
 * The scan stops at the last frame that commits a transaction, so a restore can
 * apply every frame up to the frame that the position names.
 *
 * A copy of every page is worth keeping without the position, so this returns
 * undefined for a log that Sirannon cannot read, and the backup still
 * succeeds. It also returns undefined for a database without a write-ahead log,
 * and for an empty log. The capture path reads the log through
 * {@link readLogFileHeader} and {@link scanLogFrames}, where a failure stops
 * the capture, because those frames are the backup.
 *
 * @param sourcePath - The path of the database file, next to which SQLite keeps the log.
 * @returns The position of the log, or undefined where Sirannon cannot read it.
 *
 * @internal
 */
export async function readLogPosition(sourcePath: string): Promise<BackupLogPosition | undefined> {
  const logPath = logPathFor(sourcePath)
  try {
    const header = await readLogFileHeader(logPath)
    if (!header) return undefined
    const scan = await scanLogFrames(logPath, header, { frame: 0, checksum: header.checksum })
    return {
      logSequence: header.logSequence,
      salt1: header.salt1,
      salt2: header.salt2,
      lastFrame: scan.lastCommitFrame,
    }
  } catch {
    return undefined
  }
}

/**
 * Returns the two checksum halves of a cursor as the pair that the frame scan starts from.
 *
 * @param cursor - The point where the previous capture stopped.
 * @returns The checksum of the cursor.
 */
export function cursorChecksum(cursor: LogCursor): LogChecksum {
  return { first: cursor.checksum1, second: cursor.checksum2 }
}

/**
 * Returns whether the log on disk is the same generation as the log of a
 * cursor. SQLite changes both salts each time it restarts a log, so matching
 * salts mean that the frames on disk continue the frames already captured.
 *
 * @param header - The header of the log on disk.
 * @param cursor - The point where the previous capture stopped.
 * @returns Whether the log on disk is the same generation as the log of the cursor.
 */
export function sameLog(header: LogHeader, cursor: LogCursor): boolean {
  return header.salt1 === cursor.salt1 && header.salt2 === cursor.salt2
}
