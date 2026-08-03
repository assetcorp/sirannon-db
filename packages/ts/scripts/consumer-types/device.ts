import type { Database, Migration, OperationRef } from '@delali/sirannon-db'
import { Sirannon } from '@delali/sirannon-db'
import type { ConflictContext, ConflictResolution, ConflictResolver } from '@delali/sirannon-db/client'
import {
  FieldMergeResolver,
  LWWResolver,
  PrimaryWinsResolver,
  SirannonClient,
  SyncController,
} from '@delali/sirannon-db/client'
import { expoSqlite } from '@delali/sirannon-db/driver/expo'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'

const migrations: Migration[] = [
  {
    version: 1,
    name: 'create_notes',
    up: 'CREATE TABLE notes (id TEXT PRIMARY KEY, author TEXT NOT NULL, body TEXT NOT NULL)',
    down: 'DROP TABLE notes',
  },
]

interface Device {
  db: Database
  sync: SyncController
}

export const openDevice = async (fileName: string): Promise<Device> => {
  const sirannon = new Sirannon({ driver: waSqlite() })
  const db = await sirannon.open('notes', fileName)
  await db.migrate(migrations)
  await db.watch('notes')
  const sync = new SyncController(db, {
    url: 'http://localhost:9876',
    databaseId: 'notes',
    tables: ['notes'],
    onChange: event => {
      console.log('applied', event.type, event.row)
    },
    onResyncRequired: () => {
      console.log('the local copy is about to be replaced')
    },
    onSnapshotProgress: progress => {
      console.log(`snapshot ${progress.loadedRows}/${progress.totalRows} rows`)
    },
    onSnapshotComplete: outcome => {
      if (outcome.ok) {
        console.log('the local copy is ready')
        return
      }
      console.log(`the copy failed with ${outcome.error.code}, retrying: ${outcome.retrying}`)
    },
  })
  return { db, sync }
}

export const syncWithLastWriterWins = (db: Database): SyncController =>
  new SyncController(db, {
    url: 'http://localhost:9876',
    databaseId: 'notes',
    tables: ['notes'],
    resolver: new LWWResolver(),
  })

export const syncWithPrimaryWins = (db: Database): SyncController =>
  new SyncController(db, {
    url: 'http://localhost:9876',
    databaseId: 'notes',
    tables: ['notes'],
    resolver: new PrimaryWinsResolver('server-1'),
  })

export const syncWithFieldMerge = (db: Database): SyncController =>
  new SyncController(db, {
    url: 'http://localhost:9876',
    databaseId: 'notes',
    tables: ['notes'],
    resolver: new FieldMergeResolver(async () => new Map()),
  })

class KeepLocalResolver implements ConflictResolver {
  resolve(ctx: ConflictContext): ConflictResolution {
    return ctx.localChange === null ? { action: 'accept_remote' } : { action: 'keep_local' }
  }
}

export const syncWithCustomResolver = (db: Database): SyncController =>
  new SyncController(db, {
    url: 'http://localhost:9876',
    databaseId: 'notes',
    tables: ['notes'],
    resolver: new KeepLocalResolver(),
  })

const notes = { name: 'notes' } as OperationRef<Record<string, never>, { id: unknown; body: unknown }>
const addNote = { name: 'addNote' } as OperationRef<{ body: string }, never>

export const readNotesRemotely = async (): Promise<void> => {
  const client = new SirannonClient('http://localhost:9876')
  const db = client.database('notes')
  const rows = await db.query(notes, {})
  console.log(rows.map(row => row.id))

  const written = await db.execute(addNote, { body: 'a note' })
  console.log(written[0].changes)

  const live = await db.live(notes, {})
  const state = live.getState()
  console.log(state.status === 'ready' ? state.rows.length : state.status)
  await live.close()
  client.close()
}

export const expoRegistry = (): Sirannon => new Sirannon({ driver: expoSqlite() })
