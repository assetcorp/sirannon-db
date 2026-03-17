import { SirannonClient } from '@delali/sirannon-db/client'

const SERVER_URL = 'http://localhost:9876'

const output = document.getElementById('output') as HTMLDivElement
const status = document.getElementById('status') as HTMLDivElement
const btnHttp = document.getElementById('btn-http') as HTMLButtonElement
const btnWs = document.getElementById('btn-ws') as HTMLButtonElement
const btnQuery = document.getElementById('btn-query') as HTMLButtonElement
const btnInsert = document.getElementById('btn-insert') as HTMLButtonElement
const btnTransaction = document.getElementById('btn-transaction') as HTMLButtonElement
const btnSubscribe = document.getElementById('btn-subscribe') as HTMLButtonElement
const btnClear = document.getElementById('btn-clear') as HTMLButtonElement

let client: SirannonClient | null = null
let transport: 'http' | 'websocket' = 'http'

function log(message: string) {
  output.textContent += `${message}\n`
  output.scrollTop = output.scrollHeight
}

function setStatus(text: string, state: 'ready' | 'loading' | 'error') {
  status.textContent = text
  status.className = `status ${state}`
}

function enableOperations(enabled: boolean) {
  btnQuery.disabled = !enabled
  btnInsert.disabled = !enabled
  btnTransaction.disabled = !enabled
  btnSubscribe.disabled = !enabled
}

btnClear.addEventListener('click', () => {
  output.textContent = ''
})

btnHttp.addEventListener('click', () => {
  if (client) client.close()
  transport = 'http'
  client = new SirannonClient(SERVER_URL, { transport: 'http' })
  log(`Connected via HTTP to ${SERVER_URL}`)
  setStatus('Connected (HTTP)', 'ready')
  enableOperations(true)
})

btnWs.addEventListener('click', () => {
  if (client) client.close()
  transport = 'websocket'
  client = new SirannonClient(SERVER_URL, { transport: 'websocket' })
  log(`Connected via WebSocket to ${SERVER_URL}`)
  setStatus('Connected (WebSocket)', 'ready')
  enableOperations(true)
})

btnQuery.addEventListener('click', async () => {
  if (!client) return
  btnQuery.disabled = true

  try {
    const db = client.database('main')
    log(`\n--- Querying via ${transport} ---`)

    interface Message {
      id: number
      author: string
      content: string
      created_at: string
    }
    const messages = await db.query<Message>('SELECT * FROM messages ORDER BY id DESC LIMIT 10')
    log(`Found ${messages.length} message(s):`)
    for (const msg of messages) {
      log(`  [${msg.id}] ${msg.author}: ${msg.content} (${msg.created_at})`)
    }

    interface User {
      id: number
      name: string
      email: string
      status: string
    }
    const users = await db.query<User>('SELECT * FROM users ORDER BY id')
    log(`Found ${users.length} user(s):`)
    for (const user of users) {
      log(`  [${user.id}] ${user.name} <${user.email}> (${user.status})`)
    }
    log('---')
  } catch (err) {
    log(`Query error: ${err}`)
  } finally {
    btnQuery.disabled = false
  }
})

let messageCounter = 0

btnInsert.addEventListener('click', async () => {
  if (!client) return
  btnInsert.disabled = true

  try {
    messageCounter++
    const db = client.database('main')
    const result = await db.execute('INSERT INTO messages (author, content) VALUES (?, ?)', [
      'Browser User',
      `Message #${messageCounter} from the browser client`,
    ])
    log(`Inserted message (changes: ${result.changes})`)
  } catch (err) {
    log(`Insert error: ${err}`)
  } finally {
    btnInsert.disabled = false
  }
})

btnTransaction.addEventListener('click', async () => {
  if (!client) return
  btnTransaction.disabled = true

  try {
    const db = client.database('main')
    log('\nRunning transaction...')
    const results = await db.transaction([
      {
        sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
        params: [`User_${Date.now()}`, `user_${Date.now()}@test.com`],
      },
      {
        sql: 'INSERT INTO messages (author, content) VALUES (?, ?)',
        params: ['System', 'New user registered via transaction'],
      },
    ])
    log(`Transaction committed: ${results.length} statement(s) executed.`)
    for (let i = 0; i < results.length; i++) {
      log(`  Statement ${i + 1}: ${results[i].changes} change(s)`)
    }
  } catch (err) {
    log(`Transaction error: ${err}`)
  } finally {
    btnTransaction.disabled = false
  }
})

btnSubscribe.addEventListener('click', async () => {
  if (!client) return
  if (transport !== 'websocket') {
    log('CDC subscriptions require WebSocket transport. Click "Connect WS" first.')
    return
  }
  btnSubscribe.disabled = true

  try {
    const db = client.database('main')
    log('\nSubscribing to "messages" table changes...')

    const subscription = db.subscribe('messages', event => {
      const row = event.row as Record<string, unknown>
      log(`  [CDC] ${event.type}: id=${row.id}, author=${row.author}, content=${row.content}`)
    })

    log('Inserting 3 messages to trigger events...')
    for (let i = 1; i <= 3; i++) {
      await db.execute('INSERT INTO messages (author, content) VALUES (?, ?)', ['CDC Tester', `Subscription test ${i}`])
    }

    await new Promise(resolve => globalThis.setTimeout(resolve, 500))

    subscription.unsubscribe()
    log('Subscription closed.')
  } catch (err) {
    log(`Subscribe error: ${err}`)
  } finally {
    btnSubscribe.disabled = false
  }
})

setStatus('Choose a transport to connect', 'loading')
