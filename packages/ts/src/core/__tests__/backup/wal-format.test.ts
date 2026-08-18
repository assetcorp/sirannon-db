import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import {
  foldLogChecksum,
  LOG_HEADER_BYTES,
  logFrameOffset,
  readLogFrameHeader,
  readLogHeader,
} from '../../backup/wal-format.js'
import { readLogFileHeader, scanLogFrames } from '../../backup/wal-log.js'
import { testDriver } from '../helpers/test-driver.js'
import { tempDirPerTest } from './shared.js'

const temp = tempDirPerTest()
const openConnections: { close(): Promise<void> }[] = []

afterEach(async () => {
  for (const conn of openConnections.splice(0)) {
    await conn.close()
  }
})

async function databaseWithLog(rows: number): Promise<{ dbPath: string; logPath: string }> {
  const dbPath = join(temp.path, 'source.db')
  const conn = await testDriver.open(dbPath, { walMode: true, walAutoCheckpoint: 0 })
  openConnections.push(conn)
  await conn.exec('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  for (let row = 0; row < rows; row++) {
    await conn.exec(`INSERT INTO notes (body) VALUES ('note ${row}')`)
  }
  return { dbPath, logPath: `${dbPath}-wal` }
}

describe('write-ahead log header', () => {
  it('reads the header SQLite wrote and checks it against its own checksum', async () => {
    const { logPath } = await databaseWithLog(4)
    const header = await readLogFileHeader(logPath)

    expect(header).toBeDefined()
    expect(header?.pageSize).toBeGreaterThanOrEqual(512)
    expect(header?.frameBytes).toBe((header?.pageSize ?? 0) + 24)
    expect(header?.salt1).not.toBe(header?.salt2)
  })

  it('refuses a header whose checksum does not cover its own bytes', async () => {
    const { logPath } = await databaseWithLog(2)
    const bytes = readFileSync(logPath).subarray(0, LOG_HEADER_BYTES)
    const damaged = Uint8Array.from(bytes)
    damaged[16] = (damaged[16] ?? 0) ^ 0xff

    expect(readLogHeader(bytes)).toBeDefined()
    expect(readLogHeader(damaged)).toBeUndefined()
  })

  it('refuses a file that is not a log at all', () => {
    expect(readLogHeader(new Uint8Array(LOG_HEADER_BYTES))).toBeUndefined()
    expect(readLogHeader(new Uint8Array(8))).toBeUndefined()
  })
})

describe('write-ahead log frames', () => {
  it('walks every frame SQLite wrote and stops at the last one that commits', async () => {
    const { logPath } = await databaseWithLog(6)
    const header = await readLogFileHeader(logPath)
    if (!header) throw new Error('the log carried no readable header')

    const bytes = readFileSync(logPath)
    const framesInFile = (bytes.byteLength - LOG_HEADER_BYTES) / header.frameBytes
    const scan = await scanLogFrames(logPath, header, { frame: 0, checksum: header.checksum })

    expect(scan.lastCommitFrame).toBe(framesInFile)
    expect(scan.endOffset).toBe(bytes.byteLength)

    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    const last = readLogFrameHeader(view, logFrameOffset(scan.lastCommitFrame, header.frameBytes))
    expect(last.databasePages).toBeGreaterThan(0)
    expect(last.salt1).toBe(header.salt1)
    expect(scan.checksum).toEqual(last.checksum)
  })

  it('stops at the frame before one whose checksum no longer follows', async () => {
    const { logPath } = await databaseWithLog(6)
    const header = await readLogFileHeader(logPath)
    if (!header) throw new Error('the log carried no readable header')

    const whole = await scanLogFrames(logPath, header, { frame: 0, checksum: header.checksum })
    const stale = { first: (header.checksum.first ^ 0xff) >>> 0, second: header.checksum.second }
    const fromStale = await scanLogFrames(logPath, header, { frame: 0, checksum: stale })

    expect(whole.lastCommitFrame).toBeGreaterThan(0)
    expect(fromStale.lastCommitFrame).toBe(0)
  })

  it('reads on from a frame in the middle without walking the ones before it', async () => {
    const { logPath } = await databaseWithLog(6)
    const header = await readLogFileHeader(logPath)
    if (!header) throw new Error('the log carried no readable header')

    const bytes = readFileSync(logPath)
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    const whole = await scanLogFrames(logPath, header, { frame: 0, checksum: header.checksum })
    const middle = Math.floor(whole.lastCommitFrame / 2)
    const seed = readLogFrameHeader(view, logFrameOffset(middle, header.frameBytes)).checksum

    const rest = await scanLogFrames(logPath, header, { frame: middle, checksum: seed })

    expect(rest.lastCommitFrame).toBe(whole.lastCommitFrame)
    expect(rest.checksum).toEqual(whole.checksum)
  })
})

describe('the checksum SQLite writes', () => {
  it('runs the pairs of words the log format sets out', () => {
    const bytes = new Uint8Array(16)
    const view = new DataView(bytes.buffer)
    view.setUint32(0, 1, false)
    view.setUint32(4, 2, false)
    view.setUint32(8, 3, false)
    view.setUint32(12, 4, false)

    expect(foldLogChecksum(view, 0, 16, true, { first: 0, second: 0 })).toEqual({ first: 7, second: 14 })
  })

  it('wraps at 32 bits rather than growing past them', () => {
    const bytes = new Uint8Array(8)
    const view = new DataView(bytes.buffer)
    view.setUint32(0, 0xffffffff, false)
    view.setUint32(4, 0xffffffff, false)

    expect(foldLogChecksum(view, 0, 8, true, { first: 1, second: 1 })).toEqual({ first: 1, second: 1 })
  })
})
