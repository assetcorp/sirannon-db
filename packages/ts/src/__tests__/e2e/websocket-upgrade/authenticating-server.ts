import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { RequestDeniedError, SirannonError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer } from '../../../server/server.js'

const HOOK_DELAY_MS = 10
const ACCEPTED_TOKEN = 'Bearer inspector-token'

function pause(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, HOOK_DELAY_MS))
}

const sirannon = new Sirannon({
  driver: betterSqlite3(),
  hooks: {
    onBeforeSubscribe: async () => {
      await pause()
      throw new SirannonError('The inspector may not subscribe to minutes', 'HOOK_DENIED')
    },
  },
})
const db = await sirannon.open('council', join(mkdtempSync(join(tmpdir(), 'sirannon-upgrade-')), 'council.db'))
await db.execute('CREATE TABLE minutes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
await db.watch('minutes')

const server = createServer(sirannon, {
  port: 0,
  acceptSql: true,
  acceptDeviceSync: true,
  authenticate: async ctx => {
    await pause()
    if (ctx.headers.authorization !== ACCEPTED_TOKEN) {
      throw new RequestDeniedError(401, 'UNAUTHENTICATED', 'The inspector token was refused')
    }
    return { user: 'inspector' }
  },
})
await server.listen()
process.stdout.write(`listening ${server.listeningPort}\n`)

process.stdin.on('data', async () => {
  await server.close()
  await sirannon.shutdown()
  process.exit(0)
})
