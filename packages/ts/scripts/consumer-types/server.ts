import type { Migration } from '@delali/sirannon-db'
import { Sirannon, SirannonError } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
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
