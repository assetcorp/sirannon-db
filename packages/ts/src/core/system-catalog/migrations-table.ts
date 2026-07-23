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

export async function ensureMigrationsTable(conn: SQLiteConnection): Promise<void> {
  await conn.exec(CREATE_MIGRATIONS_TABLE)
  await ensureColumn(conn, MIGRATIONS_TABLE, 'checksum', 'TEXT')
}
