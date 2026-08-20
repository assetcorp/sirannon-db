import type { FileHandle } from 'node:fs/promises'
import { open } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import { randomHex } from '../random-hex.js'
import {
  foldLogChecksum,
  LOG_FORMAT_VERSION,
  LOG_FRAME_HEADER_BYTES,
  LOG_HEADER_BYTES,
  type LogChecksum,
  type LogHeader,
  MAGIC_WITH_LITTLE_ENDIAN_CONTENT,
  readLogHeader,
} from './wal-format.js'
import { writeFully } from './write-fully.js'

const DATABASE_HEADER_BYTES = 100
const PAGE_SIZE_OFFSET = 16
const PAGE_SIZE_FOR_65536 = 1
const SMALLEST_PAGE_BYTES = 512
const LARGEST_PAGE_BYTES = 65536
const WRITE_FORMAT_OFFSET = 18
const WRITE_AHEAD_LOGGING_FORMAT = 2
const CHECKSUM_SEED: LogChecksum = { first: 0, second: 0 }

/** The first hundred bytes of a SQLite database file, as far as a restore reads them.
 * @internal
 */
export interface DatabaseHeader {
  /** Size of one page, in bytes. */
  pageSize: number
  /** Whether the file records itself as a write-ahead logging database. */
  walMode: boolean
}

/**
 * What a change piece's chain record says the log it came from looked like. The
 * first piece of every run of the log begins with that log's own 32-byte
 * header, and comparing the two catches a record that describes a different
 * database.
 *
 * @internal
 */
export interface ExpectedLogHeader {
  /** Size of one page, in bytes. */
  pageSize: number
  /** Checkpoint sequence number of the log the frames came from. */
  logSequence: number
  /** First salt of that log. */
  salt1: number
  /** Second salt of it. */
  salt2: number
}

/** What one written log holds, and where it leaves the database.
 * @internal
 */
export interface WrittenLog {
  /** Frames the log holds. */
  frameCount: number
  /** Pages the database reaches once SQLite replays them. */
  databasePages: number
}

function restoreError(message: string): SirannonError {
  return new SirannonError(message, 'BACKUP_CHAIN_BROKEN')
}

/**
 * Reads the page size and the journalling format out of a database file. A
 * restore needs both. The frames it replays hold pages of one size only, and
 * SQLite reads a log beside a file that records write-ahead logging while
 * ignoring one beside a file that does not.
 *
 * @param path - Path of the database file.
 * @returns What its header states.
 *
 * @internal
 */
export async function readDatabaseHeader(path: string): Promise<DatabaseHeader> {
  const handle = await open(path, 'r')
  try {
    const buffer = Buffer.allocUnsafe(DATABASE_HEADER_BYTES)
    const { bytesRead } = await handle.read(buffer, 0, DATABASE_HEADER_BYTES, 0)
    if (bytesRead < DATABASE_HEADER_BYTES) {
      throw restoreError(`The full copy assembled into '${path}' is too short to be a SQLite database`)
    }
    const stated = buffer.readUInt16BE(PAGE_SIZE_OFFSET)
    const pageSize = stated === PAGE_SIZE_FOR_65536 ? LARGEST_PAGE_BYTES : stated
    if (pageSize < SMALLEST_PAGE_BYTES || pageSize > LARGEST_PAGE_BYTES || (pageSize & (pageSize - 1)) !== 0) {
      throw restoreError(
        `The full copy assembled into '${path}' states a page size of ${pageSize}, which no SQLite database has`,
      )
    }
    return {
      pageSize,
      walMode: buffer.readUInt8(WRITE_FORMAT_OFFSET) === WRITE_AHEAD_LOGGING_FORMAT,
    }
  } finally {
    await handle.close()
  }
}

function logHeaderBytes(pageSize: number, logSequence: number, salt1: number, salt2: number): Uint8Array {
  const header = new Uint8Array(LOG_HEADER_BYTES)
  const view = new DataView(header.buffer)
  view.setUint32(0, MAGIC_WITH_LITTLE_ENDIAN_CONTENT, false)
  view.setUint32(4, LOG_FORMAT_VERSION, false)
  view.setUint32(8, pageSize, false)
  view.setUint32(12, logSequence, false)
  view.setUint32(16, salt1, false)
  view.setUint32(20, salt2, false)
  const checksum = foldLogChecksum(view, 0, 24, false, CHECKSUM_SEED)
  view.setUint32(24, checksum.first, false)
  view.setUint32(28, checksum.second, false)
  return header
}

function describeHeaderMismatch(captured: LogHeader, expected: ExpectedLogHeader): string | undefined {
  if (captured.pageSize !== expected.pageSize) {
    return `of ${captured.pageSize}-byte pages where its record says ${expected.pageSize}`
  }
  if (captured.logSequence !== expected.logSequence) {
    return `at checkpoint sequence ${captured.logSequence} where its record says ${expected.logSequence}`
  }
  if (captured.salt1 !== expected.salt1 || captured.salt2 !== expected.salt2) {
    return `salted ${captured.salt1} and ${captured.salt2} where its record says ${expected.salt1} and ${expected.salt2}`
  }
  return undefined
}

function randomSalt(): number {
  return Number.parseInt(randomHex(4), 16)
}

/**
 * Writes the write-ahead log that SQLite replays one batch of change pieces
 * through.
 *
 * A destination stores each frame exactly as it was captured, still holding the
 * salts and the running checksum of the log it came from. This writer stamps
 * its own salts over them and folds the checksum again from its own header,
 * which is what lets a batch start part-way down a chain instead of at frame
 * one.
 *
 * @internal
 */
export class RestoreLogWriter {
  private carry = new Uint8Array(0)
  private checksum: LogChecksum
  private offset = LOG_HEADER_BYTES
  private framesWritten = 0
  private lastDatabasePages = 0
  private skipRemaining = 0
  private skipped = new Uint8Array(0)
  private expected: ExpectedLogHeader | undefined
  private closed = false

  private constructor(
    private readonly handle: FileHandle,
    private readonly path: string,
    private readonly pageSize: number,
    private readonly salt1: number,
    private readonly salt2: number,
    checksum: LogChecksum,
  ) {
    this.checksum = checksum
  }

  /**
   * Opens the log beside a restored database and writes its header.
   *
   * @param path - Path of the log file.
   * @param pageSize - Size of one page of the database being restored, in bytes.
   * @param logSequence - Checkpoint sequence number to record in the header.
   * @returns The writer, ready for the first piece.
   */
  static async create(path: string, pageSize: number, logSequence: number): Promise<RestoreLogWriter> {
    const salt1 = randomSalt()
    const salt2 = randomSalt()
    const header = logHeaderBytes(pageSize, logSequence, salt1, salt2)
    const view = new DataView(header.buffer)
    const checksum = { first: view.getUint32(24, false), second: view.getUint32(28, false) }
    const handle = await open(path, 'w')
    try {
      await writeFully(handle, path, header, header.byteLength, 0)
    } catch (err) {
      await handle.close()
      throw err
    }
    return new RestoreLogWriter(handle, path, pageSize, salt1, salt2, checksum)
  }

  /** Bytes one frame takes, being its header plus one page. */
  get frameBytes(): number {
    return LOG_FRAME_HEADER_BYTES + this.pageSize
  }

  /**
   * Starts a new change piece, naming how many bytes of log header stand in
   * front of its first frame.
   *
   * Where the piece begins with a header, this reads it and checks it against
   * the record that named the piece. A record claiming a page size the frames
   * were never written at would otherwise have SQLite read every frame at the
   * wrong length.
   *
   * @param headerBytes - Bytes of log header in front of the frames.
   * @param expected - What the piece's chain record says that header holds.
   */
  beginPiece(headerBytes: number, expected?: ExpectedLogHeader): void {
    if (this.carry.byteLength > 0) {
      throw restoreError(
        `The change piece before this one left ${this.carry.byteLength} bytes of a frame unwritten in '${this.path}', so the two would be joined into a frame neither of them holds`,
      )
    }
    this.skipRemaining = headerBytes
    this.skipped = new Uint8Array(0)
    this.expected = expected
  }

  /**
   * Stamps every whole frame in the bytes given and appends them to the log.
   * Where a frame is split across two pieces, this keeps the first part until
   * the second one is added.
   *
   * @param bytes - The next run of bytes of the change piece.
   */
  async add(bytes: Uint8Array): Promise<void> {
    let arriving = bytes
    if (this.skipRemaining > 0) {
      const dropped = Math.min(this.skipRemaining, arriving.byteLength)
      this.keepSkipped(arriving.subarray(0, dropped))
      this.skipRemaining -= dropped
      arriving = arriving.subarray(dropped)
      if (this.skipRemaining === 0) this.checkCapturedHeader()
    }
    if (arriving.byteLength === 0) return

    const working = new Uint8Array(this.carry.byteLength + arriving.byteLength)
    working.set(this.carry, 0)
    working.set(arriving, this.carry.byteLength)

    const wholeFrames = Math.floor(working.byteLength / this.frameBytes)
    if (wholeFrames === 0) {
      this.carry = working
      return
    }

    const stampedBytes = wholeFrames * this.frameBytes
    this.stamp(working, wholeFrames)
    await writeFully(this.handle, this.path, working, stampedBytes, this.offset)
    this.offset += stampedBytes
    this.carry = working.slice(stampedBytes)
  }

  private keepSkipped(bytes: Uint8Array): void {
    const kept = new Uint8Array(this.skipped.byteLength + bytes.byteLength)
    kept.set(this.skipped, 0)
    kept.set(bytes, this.skipped.byteLength)
    this.skipped = kept
  }

  private checkCapturedHeader(): void {
    const expected = this.expected
    if (!expected) return
    const captured = readLogHeader(this.skipped)
    if (!captured) {
      throw restoreError(
        'The 32 bytes in front of the frames of a change piece are no log header SQLite wrote, so that piece reached the destination damaged',
      )
    }
    const mismatch = describeHeaderMismatch(captured, expected)
    if (mismatch) {
      throw restoreError(
        `A change piece was captured from a log ${mismatch}, so its record describes a different database and SQLite would read its frames at the wrong length`,
      )
    }
  }

  private stamp(working: Uint8Array, wholeFrames: number): void {
    const view = new DataView(working.buffer, working.byteOffset, working.byteLength)
    for (let frame = 0; frame < wholeFrames; frame++) {
      const at = frame * this.frameBytes
      view.setUint32(at + 8, this.salt1, false)
      view.setUint32(at + 12, this.salt2, false)
      const overHeader = foldLogChecksum(view, at, 8, false, this.checksum)
      const checksum = foldLogChecksum(view, at + LOG_FRAME_HEADER_BYTES, this.pageSize, false, overHeader)
      view.setUint32(at + 16, checksum.first, false)
      view.setUint32(at + 20, checksum.second, false)
      this.checksum = checksum
      this.lastDatabasePages = view.getUint32(at + 4, false)
      this.framesWritten++
    }
  }

  /**
   * Closes the log and reports what it holds.
   *
   * @returns The frames the log holds, and the size the database reaches once SQLite replays them.
   * @throws A `BACKUP_CHAIN_BROKEN` where the pieces ended part-way through a frame, or where the last frame commits nothing.
   */
  async finish(): Promise<WrittenLog> {
    await this.close()
    if (this.carry.byteLength > 0) {
      throw restoreError(
        `The change pieces written into '${this.path}' end ${this.carry.byteLength} bytes into a frame of ${this.frameBytes}, so the chain is missing the rest of it`,
      )
    }
    if (this.lastDatabasePages === 0) {
      throw restoreError(
        `The last frame written into '${this.path}' commits no transaction, so SQLite would replay none of the batch`,
      )
    }
    return { frameCount: this.framesWritten, databasePages: this.lastDatabasePages }
  }

  /** Closes the log without checking it, which is what a failed batch calls before it removes the file. */
  async abandon(): Promise<void> {
    await this.close().catch(() => {})
  }

  private async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    await this.handle.close()
  }
}
