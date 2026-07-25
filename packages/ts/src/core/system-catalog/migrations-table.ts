import type { SQLiteConnection } from '../driver/types.js'
import { MIGRATIONS_TABLE } from '../internal-tables.js'
import { ensureColumn } from './columns.js'

const CREATE_MIGRATIONS_TABLE = `
  CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
    checksum TEXT
  )
`

type PreparedStatement = Awaited<ReturnType<SQLiteConnection['prepare']>>

export interface AppliedMigrationRow {
  version: number
  name: string
  checksum: string | null
}

export interface MigrationEntryRow {
  version: number
  name: string
}

export async function ensureMigrationsTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(CREATE_MIGRATIONS_TABLE)
  await ensureColumn(conn, MIGRATIONS_TABLE, 'checksum', 'TEXT')
}

export async function selectAppliedMigrations(conn: SQLiteConnection): Promise<AppliedMigrationRow[]> {
  const stmt = await conn.prepare(`SELECT version, name, checksum FROM ${MIGRATIONS_TABLE} ORDER BY version`)
  return (await stmt.all()) as AppliedMigrationRow[]
}

export async function replaceMigrationHistory(
  conn: SQLiteConnection,
  rows: readonly AppliedMigrationRow[],
): Promise<void> {
  await ensureMigrationsTable(conn)
  await conn.transaction(async tx => {
    await tx.exec(`DELETE FROM ${MIGRATIONS_TABLE}`)
    const insert = await tx.prepare(`INSERT INTO ${MIGRATIONS_TABLE} (version, name, checksum) VALUES (?, ?, ?)`)
    for (const row of rows) {
      await insert.run(row.version, row.name, row.checksum)
    }
  })
}

export async function selectAppliedMigrationsNewestFirst(conn: SQLiteConnection): Promise<MigrationEntryRow[]> {
  const stmt = await conn.prepare(`SELECT version, name FROM ${MIGRATIONS_TABLE} ORDER BY version DESC`)
  return (await stmt.all()) as MigrationEntryRow[]
}

export function prepareInsertMigration(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`INSERT INTO ${MIGRATIONS_TABLE} (version, name, checksum) VALUES (?, ?, ?)`)
}

export function prepareUpdateMigrationChecksum(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`UPDATE ${MIGRATIONS_TABLE} SET checksum = ? WHERE version = ?`)
}

export function prepareDeleteMigration(conn: SQLiteConnection): Promise<PreparedStatement> {
  return conn.prepare(`DELETE FROM ${MIGRATIONS_TABLE} WHERE version = ?`)
}

export function highestMigrationVersion(rows: readonly { version: number }[]): number {
  let highest = 0
  for (const row of rows) {
    if (row.version > highest) highest = row.version
  }
  return highest
}
