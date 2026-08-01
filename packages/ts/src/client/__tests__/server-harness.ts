import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import type { ServerOptions } from '../../core/types.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { ServerProxy } from './server-proxy.js'

export interface ClientServerHarness {
  readonly baseUrl: string
  stop(): Promise<void>
  start(options?: ServerOptions): Promise<string>
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
  let proxy: ServerProxy | null = null
  let baseUrl = ''

  function requireSirannon(): Sirannon {
    if (!sirannon) {
      throw new Error('The client server harness has no open Sirannon instance')
    }
    return sirannon
  }

  async function listen(serverOptions?: ServerOptions): Promise<SirannonServer> {
    const started = createServer(requireSirannon(), { acceptSql: true, ...serverOptions, port: 0 })
    await started.listen()
    server = started
    return started
  }

  async function stopServer(): Promise<void> {
    proxy?.killAllConnections()
    await server?.close()
    server = null
  }

  async function startServer(serverOptions?: ServerOptions): Promise<string> {
    const started = await listen(serverOptions)
    proxy?.pointAt(started.listeningPort)
    return baseUrl
  }

  beforeEach(async () => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-client-'))
    sirannon = new Sirannon({ driver })
    const db = await sirannon.open('testdb', join(tempDir, 'test.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute('INSERT INTO users (name) VALUES (?)', [userName])
    const started = await listen()
    proxy = new ServerProxy(started.listeningPort)
    await proxy.listen()
    baseUrl = `http://127.0.0.1:${proxy.port}`
  })

  afterEach(async () => {
    await proxy?.close()
    proxy = null
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
    stop: stopServer,
    start: startServer,
    async restart(options?: ServerOptions) {
      await stopServer()
      return startServer(options)
    },
  }
}
