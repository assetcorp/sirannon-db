import type { SQLiteConnection } from '../../driver/types.js'
import { testDriver } from '../helpers/test-driver.js'

export async function createTestDb(): Promise<SQLiteConnection> {
  const conn = await testDriver.open(':memory:')
  await conn.exec('PRAGMA journal_mode = WAL')
  await conn.exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT,
			age INTEGER
		)
	`)
  return conn
}

export async function insertUser(
  conn: SQLiteConnection,
  name: string,
  email: string | null = null,
  age: number | null = null,
): Promise<number> {
  const stmt = await conn.prepare('INSERT INTO users (name, email, age) VALUES (?, ?, ?)')
  const result = await stmt.run(name, email, age)
  return Number(result.lastInsertRowId)
}
