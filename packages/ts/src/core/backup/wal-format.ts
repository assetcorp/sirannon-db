/** The magic number at the start of a log whose checksums treat each word as little-endian.
 * @internal
 */
export const MAGIC_WITH_LITTLE_ENDIAN_CONTENT = 0x377f0682

const MAGIC_WITH_BIG_ENDIAN_CONTENT = 0x377f0683

/** The log format version that SQLite writes into the header, and the only version that SQLite accepts.
 * @internal
 */
export const LOG_FORMAT_VERSION = 3007000

const SMALLEST_PAGE_BYTES = 512
const LARGEST_PAGE_BYTES = 65536

/** The size in bytes of the header at the start of a write-ahead log.
 * @internal
 */
export const LOG_HEADER_BYTES = 32

/** The size in bytes of the header before every log frame.
 * @internal
 */
export const LOG_FRAME_HEADER_BYTES = 24

/** The two halves of the running checksum that SQLite computes along a write-ahead log.
 * @internal
 */
export interface LogChecksum {
  /** The half that SQLite stores as checksum-1. */
  first: number
  /** The half that SQLite stores as checksum-2. */
  second: number
}

/** The fields in the first 32 bytes of a write-ahead log.
 * @internal
 */
export interface LogHeader {
  /** The size of one database page, in bytes. */
  pageSize: number
  /** The size of one frame, which is its 24-byte header plus one page. */
  frameBytes: number
  /** The checkpoint sequence number. SQLite adds one to it each time it restarts the log. */
  logSequence: number
  /** The first salt, which SQLite changes each time it restarts the log. */
  salt1: number
  /** The second salt, which SQLite also changes at each restart. */
  salt2: number
  /** The checksum over the first 24 bytes, which the checksum of frame one continues from. */
  checksum: LogChecksum
  /** Whether SQLite computes the checksum over big-endian words. */
  bigEndianContent: boolean
}

/** The fields in the 24-byte header before one log frame.
 * @internal
 */
export interface LogFrameHeader {
  /** The number of the database page in this frame. */
  pageNumber: number
  /** The size of the database in pages after this frame commits, or zero for a frame that commits no transaction. */
  databasePages: number
  /** The first salt, copied from the log header. */
  salt1: number
  /** The second salt, copied from the log header. */
  salt2: number
  /** The running checksum up to and including this frame. */
  checksum: LogChecksum
}

/**
 * Continues the running checksum of a log over one more range of bytes. The
 * magic number at the start of the log sets the byte order of the 32-bit words
 * in that range.
 *
 * @param view - The bytes to add to the checksum.
 * @param offset - The start of the range.
 * @param byteLength - The length of the range, which must be a multiple of eight bytes.
 * @param bigEndianContent - Whether to read the words as big-endian.
 * @param seed - The checksum to continue from.
 * @returns The checksum after the range.
 */
export function foldLogChecksum(
  view: DataView,
  offset: number,
  byteLength: number,
  bigEndianContent: boolean,
  seed: LogChecksum,
): LogChecksum {
  const littleEndian = !bigEndianContent
  const end = offset + byteLength
  let first = seed.first
  let second = seed.second
  for (let at = offset; at < end; at += 8) {
    first = (first + view.getUint32(at, littleEndian) + second) >>> 0
    second = (second + view.getUint32(at + 4, littleEndian) + first) >>> 0
  }
  return { first, second }
}

/**
 * Reads the header at the start of a write-ahead log and checks it against the
 * checksum that it stores. This returns undefined for a torn header, or for a
 * file that is not a log.
 *
 * @param bytes - At least the first 32 bytes of the log file.
 * @returns The header, or undefined where those bytes hold no valid log header.
 */
export function readLogHeader(bytes: Uint8Array): LogHeader | undefined {
  if (bytes.byteLength < LOG_HEADER_BYTES) return undefined
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)

  const magic = view.getUint32(0, false)
  if (magic !== MAGIC_WITH_LITTLE_ENDIAN_CONTENT && magic !== MAGIC_WITH_BIG_ENDIAN_CONTENT) return undefined
  const bigEndianContent = magic === MAGIC_WITH_BIG_ENDIAN_CONTENT

  const pageSize = view.getUint32(8, false)
  if (pageSize < SMALLEST_PAGE_BYTES || pageSize > LARGEST_PAGE_BYTES || (pageSize & (pageSize - 1)) !== 0) {
    return undefined
  }

  const stored: LogChecksum = { first: view.getUint32(24, false), second: view.getUint32(28, false) }
  const computed = foldLogChecksum(view, 0, 24, bigEndianContent, { first: 0, second: 0 })
  if (computed.first !== stored.first || computed.second !== stored.second) return undefined

  return {
    pageSize,
    frameBytes: LOG_FRAME_HEADER_BYTES + pageSize,
    logSequence: view.getUint32(12, false),
    salt1: view.getUint32(16, false),
    salt2: view.getUint32(20, false),
    checksum: stored,
    bigEndianContent,
  }
}

/**
 * Reads the 24-byte header before one frame.
 *
 * @param view - The bytes that hold the frame.
 * @param offset - The start of the frame.
 * @returns The fields of that header.
 */
export function readLogFrameHeader(view: DataView, offset: number): LogFrameHeader {
  return {
    pageNumber: view.getUint32(offset, false),
    databasePages: view.getUint32(offset + 4, false),
    salt1: view.getUint32(offset + 8, false),
    salt2: view.getUint32(offset + 12, false),
    checksum: { first: view.getUint32(offset + 16, false), second: view.getUint32(offset + 20, false) },
  }
}

/**
 * Checks that one frame follows the frame before it. The salts of the frame
 * must match the log header, and its stored checksum must match the running
 * checksum over its first eight bytes and its page. A frame that a rolled-back
 * transaction leaves in the file fails the checksum test, which is how Sirannon
 * tells a valid frame from a stale one.
 *
 * @param view - The bytes that hold the frame.
 * @param offset - The start of the frame.
 * @param header - The header of the log that holds the frame.
 * @param seed - The checksum after the previous frame, or the checksum of the log header for frame one.
 * @returns The frame header and the running checksum after it, or undefined where the frame does not follow.
 */
export function readValidLogFrame(
  view: DataView,
  offset: number,
  header: LogHeader,
  seed: LogChecksum,
): { frame: LogFrameHeader; checksum: LogChecksum } | undefined {
  const frame = readLogFrameHeader(view, offset)
  if (frame.salt1 !== header.salt1 || frame.salt2 !== header.salt2) return undefined

  const overHeader = foldLogChecksum(view, offset, 8, header.bigEndianContent, seed)
  const checksum = foldLogChecksum(
    view,
    offset + LOG_FRAME_HEADER_BYTES,
    header.pageSize,
    header.bigEndianContent,
    overHeader,
  )
  if (checksum.first !== frame.checksum.first || checksum.second !== frame.checksum.second) return undefined
  return { frame, checksum }
}

/**
 * Returns the byte offset of one frame in the log file.
 *
 * @param frameNumber - The frame, counted from one.
 * @param frameBytes - The size of one frame, in bytes.
 * @returns The byte offset of the frame.
 */
export function logFrameOffset(frameNumber: number, frameBytes: number): number {
  return LOG_HEADER_BYTES + (frameNumber - 1) * frameBytes
}
