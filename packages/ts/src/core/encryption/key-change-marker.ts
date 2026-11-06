import type { DatabaseFileSystem } from '../driver/file-system.js'
import type { SQLiteConnection, SQLiteDriver } from '../driver/types.js'
import { keyChangeMarkerPath } from './key-change-files.js'

const MARKER_TABLE = 'key_change_marker'
const MARKER_JOURNAL_SUFFIX = '-journal'
const HEX_DIGITS = '0123456789abcdef'
const HEX_BYTE = /^[0-9a-f]{2}$/

/**
 * What the marker of a key change records: the first bytes of the new file, and whether the replacement of the
 * database file has begun.
 *
 * @internal
 */
export interface KeyChangeMarkerState {
  readonly identity: Uint8Array
  readonly replacementBegun: boolean
}

/**
 * The open marker of a running key change.
 *
 * @internal
 */
export interface KeyChangeMarker {
  recordReplacementBegun(): Promise<void>
  flushDirectory(): Promise<void>
  close(): Promise<void>
}

function hexOf(bytes: Uint8Array): string {
  let hex = ''
  for (const byte of bytes) hex += HEX_DIGITS[byte >> 4] + HEX_DIGITS[byte & 0x0f]
  return hex
}

function bytesOfHex(hex: string): Uint8Array | null {
  if (hex.length % 2 !== 0) return null
  const bytes = new Uint8Array(hex.length / 2)
  for (let i = 0; i < bytes.length; i++) {
    const pair = hex.slice(i * 2, i * 2 + 2)
    if (!HEX_BYTE.test(pair)) return null
    bytes[i] = Number.parseInt(pair, 16)
  }
  return bytes
}

function openMarker(driver: SQLiteDriver, path: string, readonly: boolean): Promise<SQLiteConnection> {
  return driver.open(keyChangeMarkerPath(path), { readonly, walMode: false, synchronous: 'full' })
}

async function bumpWrites(conn: SQLiteConnection, setBegun: boolean): Promise<void> {
  const begun = setBegun ? ', replacement_begun = 1' : ''
  await conn.exec(`UPDATE ${MARKER_TABLE} SET writes = writes + 1${begun}`)
}

/**
 * Deletes the marker of a key change and its rollback journal.
 *
 * @param files - The driver's file operations.
 * @param path - The database file.
 * @internal
 */
export async function removeKeyChangeMarker(files: DatabaseFileSystem, path: string): Promise<void> {
  const markerPath = keyChangeMarkerPath(path)
  await files.remove(markerPath)
  await files.remove(`${markerPath}${MARKER_JOURNAL_SUFFIX}`)
}

/**
 * Creates the marker of a key change beside the database: a plaintext SQLite database in rollback-journal mode with
 * `synchronous = FULL`, so that SQLite syncs the directory the first time it syncs each new rollback journal, and
 * every later write to the marker makes the renames and deletes made before it in that directory durable.
 *
 * @param driver - The driver that opens the marker.
 * @param path - The database file.
 * @param identity - The first bytes of the new file.
 * @returns The open marker.
 * @internal
 */
export async function createKeyChangeMarker(
  driver: SQLiteDriver,
  path: string,
  identity: Uint8Array,
): Promise<KeyChangeMarker> {
  const conn = await openMarker(driver, path, false)
  try {
    await conn.exec('PRAGMA journal_mode = DELETE')
    await conn.exec('PRAGMA synchronous = FULL')
    await conn.exec(
      `BEGIN IMMEDIATE; CREATE TABLE ${MARKER_TABLE} (identity TEXT NOT NULL, replacement_begun INTEGER NOT NULL, writes INTEGER NOT NULL); INSERT INTO ${MARKER_TABLE} VALUES ('${hexOf(identity)}', 0, 1); COMMIT`,
    )
  } catch (err) {
    await conn.close().catch(() => undefined)
    throw err
  }
  return {
    recordReplacementBegun: () => bumpWrites(conn, true),
    flushDirectory: () => bumpWrites(conn, false),
    close: () => conn.close(),
  }
}

/**
 * Reads the marker that a key change left beside the database, rolling back any write to it that a crash
 * interrupted, unless the open is read-only.
 *
 * @param driver - The driver that opens the marker.
 * @param path - The database file.
 * @param readonly - Whether the open of the database is read-only.
 * @returns What the marker records, or `null` when it holds no complete record, which happens when the process
 * stopped while it created the marker.
 * @internal
 */
export async function readKeyChangeMarker(
  driver: SQLiteDriver,
  path: string,
  readonly: boolean,
): Promise<KeyChangeMarkerState | null> {
  let conn: SQLiteConnection
  try {
    conn = await openMarker(driver, path, readonly)
  } catch {
    return null
  }
  try {
    const row = (await (await conn.prepare(`SELECT identity, replacement_begun FROM ${MARKER_TABLE} LIMIT 1`)).get()) as
      | { identity?: unknown; replacement_begun?: unknown }
      | undefined
    if (row === undefined || typeof row.identity !== 'string') return null
    const identity = bytesOfHex(row.identity)
    if (identity === null) return null
    return { identity, replacementBegun: Number(row.replacement_begun) === 1 }
  } catch {
    return null
  } finally {
    await conn.close().catch(() => undefined)
  }
}

/**
 * Writes to the marker of a key change once, so that SQLite syncs the directory that holds the database and every
 * rename and delete made in it before the write reaches the disk.
 *
 * @param driver - The driver that opens the marker.
 * @param path - The database file.
 * @internal
 */
export async function flushDirectoryThroughMarker(driver: SQLiteDriver, path: string): Promise<void> {
  const conn = await openMarker(driver, path, false)
  try {
    await bumpWrites(conn, false)
  } finally {
    await conn.close()
  }
}
