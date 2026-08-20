import type { Migration } from '@delali/sirannon-db'
import { Sirannon, SirannonError } from '@delali/sirannon-db'
import type { BackupDestination } from '@delali/sirannon-db/backup'
import { restoreBackup } from '@delali/sirannon-db/backup'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { bunSqlite } from '@delali/sirannon-db/driver/bun'
import { LWWResolver } from '@delali/sirannon-db/replication'
import { createServer } from '@delali/sirannon-db/server'

const migrations: Migration[] = [
  {
    version: 1,
    name: 'create_notes',
    up: 'CREATE TABLE notes (id TEXT PRIMARY KEY, author TEXT NOT NULL, body TEXT NOT NULL)',
    down: 'DROP TABLE notes',
  },
]

export const startServer = async (): Promise<void> => {
  const sirannon = new Sirannon({ driver: betterSqlite3() })
  const db = await sirannon.open('notes', './data/notes.db')
  await db.migrate(migrations)
  await db.watch('notes')
  const server = createServer(sirannon, { port: 9876 })
  try {
    await server.listen()
  } catch (error) {
    if (error instanceof SirannonError) {
      console.error(error.code, error.message)
    }
    throw error
  }
}

export const serverSideResolver = new LWWResolver()

export const bunRegistry = (): Sirannon => new Sirannon({ driver: bunSqlite({ busyTimeout: 10_000 }) })

export const recoverFromBackup = async (destination: BackupDestination): Promise<string> => {
  const report = await restoreBackup({
    destination,
    driver: betterSqlite3(),
    destPath: './data/recovered.db',
    moment: Date.now(),
  })
  return report.destPath
}
