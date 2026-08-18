const MAGIC_WITH_LITTLE_ENDIAN_CONTENT = 0x377f0682
const MAGIC_WITH_BIG_ENDIAN_CONTENT = 0x377f0683
const SMALLEST_PAGE_BYTES = 512
const LARGEST_PAGE_BYTES = 65536

/** Bytes in the header at the front of a write-ahead log.
 * @internal
 */
export const LOG_HEADER_BYTES = 32

/** Bytes in the header in front of every log frame.
 * @internal
 */
export const LOG_FRAME_HEADER_BYTES = 24

/** The two halves of the running checksum SQLite keeps down a write-ahead log.
 * @internal
 */
export interface LogChecksum {
  /** The half SQLite stores as checksum-1. */
  first: number
  /** The half SQLite stores as checksum-2. */
  second: number
}

/** The first 32 bytes of a write-ahead log.
 * @internal
 */
export interface LogHeader {
  /** Size of one database page, in bytes. */
  pageSize: number
  /** Size of one frame, being its 24-byte header plus one page. */
  frameBytes: number
  /** Checkpoint sequence number. SQLite adds one to it every time it restarts the log. */
  logSequence: number
  /** First salt. A restart of the log changes it. */
  salt1: number
  /** Second salt. A restart changes this one too. */
  salt2: number
  /** Checksum over the first 24 bytes. The checksum of frame one starts from here. */
  checksum: LogChecksum
  /** Whether the checksum takes the byte stream as big-endian words. */
  bigEndianContent: boolean
}

/** The 24 bytes in front of one log frame.
 * @internal
 */
export interface LogFrameHeader {
  /** Number of the database page in this frame. */
  pageNumber: number
  /** Size of the database in pages once this frame commits. A frame that commits nothing holds zero here. */
  databasePages: number
  /** First salt, copied from the log header. */
  salt1: number
  /** Second salt, copied from the log header. */
  salt2: number
  /** Running checksum up to and including this frame. */
  checksum: LogChecksum
}

/**
 * Continues the running checksum SQLite writes down a log, over one more run of
 * bytes. The magic number at the front of the log sets the byte order of the
 * 32-bit words that run is taken as.
 *
 * @param view - Bytes to fold in.
 * @param offset - Where the run starts.
 * @param byteLength - How far the run goes, always a multiple of eight bytes.
 * @param bigEndianContent - Whether to take the words as big-endian.
 * @param seed - The checksum this run continues from.
 * @returns The checksum once the run is folded in.
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
 * Reads the header at the front of a write-ahead log, checked against the
 * checksum stored inside it. A torn header, or a file that is no log at all,
 * comes back as undefined.
 *
 * @param bytes - At least the first 32 bytes of the log file.
 * @returns The header, or undefined where those bytes are no log Sirannon can read.
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
 * Reads the 24 bytes in front of one frame.
 *
 * @param view - Bytes holding the frame.
 * @param offset - Where the frame starts.
 * @returns What that header states.
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
 * Checks that one frame follows the frame before it. Two things have to hold:
 * the frame's salts still match the log header, and its stored checksum matches
 * the running checksum over its own first eight bytes and its page. A frame
 * left behind by a rolled-back transaction fails that second test, which is how
 * Sirannon tells a live frame from a dead one.
 *
 * @param view - Bytes holding the frame.
 * @param offset - Where the frame starts.
 * @param header - Header of the log the frame belongs to.
 * @param seed - The checksum the previous frame left, or the header's own for frame one.
 * @returns The frame header and the checksum it leaves, or undefined where the frame does not follow.
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
 * Works out where one frame starts in the log file.
 *
 * @param frameNumber - The frame, counted from one.
 * @param frameBytes - Size of one frame in bytes.
 * @returns Its byte offset.
 */
export function logFrameOffset(frameNumber: number, frameBytes: number): number {
  return LOG_HEADER_BYTES + (frameNumber - 1) * frameBytes
}
