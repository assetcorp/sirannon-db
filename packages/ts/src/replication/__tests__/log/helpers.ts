import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'

export const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
export const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'

export async function createTestDb(): Promise<SQLiteConnection> {
  const conn = await testDriver.open(':memory:')
  await conn.exec('PRAGMA journal_mode = WAL')
  return conn
}

export async function setupTrackerAndTable(conn: SQLiteConnection): Promise<void> {
  const tracker = new ChangeTracker({ replication: true })
  await conn.exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		)
	`)
  await tracker.watch(conn, 'users')
}
