import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { releaseAccessServers, serveTyped } from '../access-and-operations/served-access.js'
import { readSqlite, scratchDirectory, trackedRegistry, waitUntil } from '../lib/served-registry.js'

const INSERTS_SENT_ON_OPEN = 40
const CREATE_LEDGER = 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, memo TEXT NOT NULL)'
const READ_LEDGER = 'SELECT id, memo FROM ledger ORDER BY id'

interface Reply {
  id?: string
  type?: string
  data?: { rows?: Array<{ id: number; memo: string }> }
  error?: { code?: string }
}

afterEach(async () => {
  await releaseAccessServers()
})

function writesSentOnOpen(): Array<{ id: string; type: 'execute'; sql: string; params?: unknown[] }> {
  const writes: Array<{ id: string; type: 'execute'; sql: string; params?: unknown[] }> = [
    { id: 'create', type: 'execute', sql: CREATE_LEDGER },
  ]
  for (let index = 0; index < INSERTS_SENT_ON_OPEN; index++) {
    writes.push({
      id: `insert-${index}`,
      type: 'execute',
      sql: 'INSERT INTO ledger (id, memo) VALUES (?, ?)',
      params: [index, `entry ${index}`],
    })
  }
  return writes
}

describe('a WebSocket request sent before the server has opened its database', () => {
  it('is answered, in the order of its writes, once the database opens on first use', async () => {
    const tenants = scratchDirectory('opening-messages')
    const sirannon = trackedRegistry({
      lifecycle: { autoOpen: { resolver: id => ({ path: join(tenants, `${id}.db`) }) } },
    })
    const served = await serveTyped(sirannon, { acceptSql: true })
    const writes = writesSentOnOpen()
    const replies: Reply[] = []

    const ws = new WebSocket(`${served.wsBase}/db/ledger`)
    ws.addEventListener('open', () => {
      for (const write of writes) ws.send(JSON.stringify(write))
    })
    ws.addEventListener('message', event => {
      replies.push(JSON.parse(String(event.data)) as Reply)
    })
    await waitUntil(() => replies.length === writes.length, 'a reply to every write sent on open')
    ws.send(JSON.stringify({ id: 'read', type: 'query', sql: READ_LEDGER }))
    await waitUntil(() => replies.length === writes.length + 1, 'the reply to the read')
    ws.close()

    const stored = readSqlite<{ id: bigint; memo: string }>(join(tenants, 'ledger.db'), READ_LEDGER).map(row => ({
      id: Number(row.id),
      memo: row.memo,
    }))
    const expected = Array.from({ length: INSERTS_SENT_ON_OPEN }, (_, index) => ({ id: index, memo: `entry ${index}` }))

    expect(replies.filter(reply => reply.error !== undefined)).toEqual([])
    expect(replies.slice(0, writes.length).map(reply => reply.id)).toEqual(writes.map(write => write.id))
    expect(stored).toEqual(expected)
    expect(replies.at(-1)?.data?.rows).toEqual(expected)
  })
})
