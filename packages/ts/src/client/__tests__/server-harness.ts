import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import type { ServerOptions } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'

export interface ClientServerHarness {
  readonly baseUrl: string
  restart(options?: ServerOptions): Promise<string>
}

export interface ClientServerHarnessOptions {
  userName?: string
}

export function createClientServerHarness(options: ClientServerHarnessOptions = {}): ClientServerHarness {
  const driver = betterSqlite3()
  const userName = options.userName ?? 'Alice'
  let tempDir = ''
  let sirannon: Sirannon | null = null
  let server: SirannonServer | null = null
  let baseUrl = ''

  function requireSirannon(): Sirannon {
    if (!sirannon) {
      throw new Error('The client server harness has no open Sirannon instance')
    }
    return sirannon
  }

  async function listen(serverOptions?: ServerOptions): Promise<string> {
    server = createServer(requireSirannon(), { port: 0, acceptSql: true, ...serverOptions })
    await server.listen()
    baseUrl = `http://127.0.0.1:${server.listeningPort}`
    return baseUrl
  }

  beforeEach(async () => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-client-'))
    sirannon = new Sirannon({ driver })
    const db = await sirannon.open('testdb', join(tempDir, 'test.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute('INSERT INTO users (name) VALUES (?)', [userName])
    await listen()
  })

  afterEach(async () => {
    await server?.close()
    server = null
    await sirannon?.shutdown()
    sirannon = null
    rmSync(tempDir, { recursive: true, force: true })
  })

  return {
    get baseUrl() {
      return baseUrl
    },
    async restart(options?: ServerOptions) {
      await server?.close()
      return listen(options)
    },
  }
}
