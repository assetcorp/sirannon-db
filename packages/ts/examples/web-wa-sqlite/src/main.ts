import { Database } from '@delali/sirannon-db'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'

const output = document.getElementById('output') as HTMLDivElement
const status = document.getElementById('status') as HTMLDivElement
const btnInit = document.getElementById('btn-init') as HTMLButtonElement
const btnInsert = document.getElementById('btn-insert') as HTMLButtonElement
const btnQuery = document.getElementById('btn-query') as HTMLButtonElement
const btnTransaction = document.getElementById('btn-transaction') as HTMLButtonElement
const btnCdc = document.getElementById('btn-cdc') as HTMLButtonElement
const btnClear = document.getElementById('btn-clear') as HTMLButtonElement

let db: Database | null = null
let insertCounter = 0

function log(message: string) {
  output.textContent += `${message}\n`
  output.scrollTop = output.scrollHeight
}

function setStatus(text: string, state: 'ready' | 'loading' | 'error') {
  status.textContent = text
  status.className = `status ${state}`
}

function enableButtons(enabled: boolean) {
  btnInsert.disabled = !enabled
  btnQuery.disabled = !enabled
  btnTransaction.disabled = !enabled
  btnCdc.disabled = !enabled
}

btnClear.addEventListener('click', () => {
  output.textContent = ''
})

btnInit.addEventListener('click', async () => {
  btnInit.disabled = true
  setStatus('Initializing database...', 'loading')
  log('Creating wa-sqlite driver with IDBBatchAtomicVFS...')

  try {
    const driver = waSqlite({ vfs: 'IDBBatchAtomicVFS' })
    db = await Database.create('browser-demo', '/sirannon-example.db', driver, {
      readPoolSize: 1,
      walMode: false,
      cdcPollInterval: 50,
    })

    log('Database created. Running migrations...')

    await db.execute(`
      CREATE TABLE IF NOT EXISTS notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        body TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)
    await db.execute(`
      CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        note_id INTEGER NOT NULL REFERENCES notes(id),
        name TEXT NOT NULL
      )
    `)

    log('Schema created: notes, tags')
    setStatus('Database ready', 'ready')
    enableButtons(true)
  } catch (err) {
    log(`Error: ${err}`)
    setStatus('Initialization failed', 'error')
    btnInit.disabled = false
  }
})

btnInsert.addEventListener('click', async () => {
  if (!db) return
  btnInsert.disabled = true

  try {
    insertCounter++
    const result = await db.execute('INSERT INTO notes (title, body) VALUES (?, ?)', [
      `Note #${insertCounter}`,
      `Content for note ${insertCounter}, created at ${new Date().toISOString()}`,
    ])
    log(`Inserted note #${insertCounter} (lastInsertRowId: ${result.lastInsertRowId})`)
  } catch (err) {
    log(`Insert error: ${err}`)
  } finally {
    btnInsert.disabled = false
  }
})

btnQuery.addEventListener('click', async () => {
  if (!db) return
  btnQuery.disabled = true

  try {
    interface Note {
      id: number
      title: string
      body: string
      created_at: string
    }
    const notes = await db.query<Note>('SELECT * FROM notes ORDER BY id DESC LIMIT 10')
    log(`\n--- Query Results (${notes.length} rows) ---`)
    if (notes.length === 0) {
      log('  (no notes yet, click Insert Row first)')
    }
    for (const note of notes) {
      log(`  [${note.id}] ${note.title} - ${note.created_at}`)
    }
    log('---')
  } catch (err) {
    log(`Query error: ${err}`)
  } finally {
    btnQuery.disabled = false
  }
})

btnTransaction.addEventListener('click', async () => {
  if (!db) return
  btnTransaction.disabled = true

  try {
    log('\nRunning transaction: insert note + 2 tags...')
    await db.transaction(async tx => {
      const noteResult = await tx.execute('INSERT INTO notes (title, body) VALUES (?, ?)', [
        'Tagged Note',
        'This note has tags',
      ])
      const noteId = noteResult.lastInsertRowId
      await tx.execute('INSERT INTO tags (note_id, name) VALUES (?, ?)', [noteId, 'important'])
      await tx.execute('INSERT INTO tags (note_id, name) VALUES (?, ?)', [noteId, 'example'])

      const tags = await tx.query<{ name: string }>('SELECT name FROM tags WHERE note_id = ?', [noteId])
      log(`  Created note #${noteId} with tags: ${tags.map(t => t.name).join(', ')}`)
    })
    log('Transaction committed.')
  } catch (err) {
    log(`Transaction error: ${err}`)
  } finally {
    btnTransaction.disabled = false
  }
})

btnCdc.addEventListener('click', async () => {
  if (!db) return
  btnCdc.disabled = true

  try {
    log('\nSetting up CDC watch on "notes" table...')
    await db.watch('notes')

    let eventCount = 0
    const subscription = db.on('notes').subscribe(event => {
      eventCount++
      const row = event.row as Record<string, unknown>
      log(`  [CDC] ${event.type}: id=${row.id}, title=${row.title}`)
    })

    log('Inserting 3 rows to trigger CDC events...')
    for (let i = 1; i <= 3; i++) {
      await db.execute('INSERT INTO notes (title, body) VALUES (?, ?)', [`CDC Test ${i}`, `Triggering event ${i}`])
    }

    await new Promise(resolve => globalThis.setTimeout(resolve, 200))

    subscription.unsubscribe()
    log(`CDC watch complete. Received ${eventCount} event(s).`)
  } catch (err) {
    log(`CDC error: ${err}`)
  } finally {
    btnCdc.disabled = false
  }
})

setStatus('Click "Initialize DB" to start', 'loading')
btnInit.disabled = false
