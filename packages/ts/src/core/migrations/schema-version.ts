import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { SQLITE_USER_VERSION_MAX } from './baseline.js'

interface UserVersionRow {
  user_version: number | bigint
}

export async function readSchemaVersion(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('PRAGMA user_version')
  const row = await stmt.get<UserVersionRow>()
  return row === undefined ? 0 : Number(row.user_version)
}

export async function writeSchemaVersion(conn: SQLiteConnection, version: number): Promise<void> {
  if (!Number.isSafeInteger(version) || version < 0 || version > SQLITE_USER_VERSION_MAX) {
    throw new SirannonError(
      `Schema version is outside the PRAGMA user_version range: ${version}`,
      'INTERNAL_SCHEMA_ERROR',
    )
  }
  await conn.exec(`PRAGMA user_version = ${version}`)
}

export async function mirrorSchemaVersion(conn: SQLiteConnection, version: number): Promise<void> {
  if (version > SQLITE_USER_VERSION_MAX) return
  await writeSchemaVersion(conn, version)
}

export async function syncSchemaVersion(conn: SQLiteConnection, expected: number): Promise<void> {
  if (expected > SQLITE_USER_VERSION_MAX) return
  const current = await readSchemaVersion(conn)
  if (current !== expected) {
    await writeSchemaVersion(conn, expected)
  }
}
