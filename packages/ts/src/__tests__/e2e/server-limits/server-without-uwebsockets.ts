import { mkdtempSync, rmSync } from 'node:fs'
import { register } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const HIDE_UWEBSOCKETS = `
export async function resolve(specifier, context, nextResolve) {
  if (specifier === 'uWebSockets.js') {
    const error = new Error("Cannot find package 'uWebSockets.js' imported from " + context.parentURL)
    error.code = 'ERR_MODULE_NOT_FOUND'
    throw error
  }
  return nextResolve(specifier, context)
}
`

register(`data:text/javascript,${encodeURIComponent(HIDE_UWEBSOCKETS)}`)

interface ListenOutcome {
  code: string | null
  message: string
}

async function listenOutcome(listen: () => Promise<void>): Promise<ListenOutcome> {
  try {
    await listen()
    return { code: null, message: 'listened' }
  } catch (err) {
    return {
      code: (err as { code?: string }).code ?? null,
      message: err instanceof Error ? err.message : String(err),
    }
  }
}

const { Sirannon } = await import('../../../core/sirannon.js')
const { betterSqlite3 } = await import('../../../drivers/better-sqlite3/index.js')
const { createServer } = await import('../../../server/server.js')

const dir = mkdtempSync(join(tmpdir(), 'sirannon-without-uwebsockets-'))
const sirannon = new Sirannon({ driver: betterSqlite3() })
const db = await sirannon.open('orders', join(dir, 'orders.db'))
await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)')
const server = createServer(sirannon, { port: 0, acceptSql: true })

const first = await listenOutcome(() => server.listen())
const second = await listenOutcome(() => server.listen())
const portAfterFailure = server.listeningPort
let closed = false
await server.close().then(() => {
  closed = true
})
const rows = await db.query<{ n: number }>('SELECT count(*) AS n FROM orders')
await sirannon.shutdown()
rmSync(dir, { recursive: true, force: true })

process.stdout.write(`${JSON.stringify({ first, second, portAfterFailure, closed, rows })}\n`)
