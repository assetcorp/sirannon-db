import { randomBytes } from 'node:crypto'
import { existsSync, statSync } from 'node:fs'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import type StockDatabase from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { Database } from '../../../core/database.js'
import type { SQLiteConnection, SQLiteDriver } from '../../../core/driver/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { ReplicationEngine } from '../../../replication/engine.js'
import { PrimaryReplicaTopology } from '../../../replication/topology/primary-replica.js'
import { InMemoryTransport, MemoryBus } from '../../../transport/memory/index.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'
import { gate } from '../../integration/backups/harness.js'
import { openBackedUp, restoreSettled } from '../backups-over-server/served-backups.js'
import {
  errorCodeOf,
  postJson,
  releaseServedRegistries,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
} from '../lib/served-registry.js'

const binding = builtSirannonSqliteBindingPath()
const require = createRequire(import.meta.url)
const OPERATOR = { authenticate: () => ({ operator: true }) }

let dir: string
const stops: (() => Promise<unknown>)[] = []

beforeEach(() => {
  dir = scratchDirectory('file-replacement')
})

afterEach(async () => {
  for (const stop of stops.splice(0).reverse()) await stop().catch(() => {})
  await releaseServedRegistries()
})

async function writerConnection(driver: SQLiteDriver, path: string, key?: Uint8Array): Promise<SQLiteConnection> {
  const conn = await driver.open(path, key === undefined ? {} : { encryption: { key: Uint8Array.from(key) } })
  stops.push(() => conn.close())
  await conn.exec('PRAGMA journal_mode = WAL')
  return conn
}

async function startReplication(db: Database, conn: SQLiteConnection, table: string): Promise<ReplicationEngine> {
  const tracker = new ChangeTracker()
  await tracker.watch(conn, table)
  const engine = new ReplicationEngine(db, conn, {
    nodeId: `${table}-primary`,
    topology: new PrimaryReplicaTopology('primary'),
    transport: new InMemoryTransport(new MemoryBus()),
    initialSync: false,
    changeTracker: tracker,
  })
  stops.unshift(() => engine.stop())
  await engine.start()
  return engine
}

async function codeOf(promise: Promise<unknown>): Promise<string | undefined> {
  try {
    await promise
    return undefined
  } catch (err) {
    return (err as { code?: string }).code
  }
}

function sidecarsOf(path: string): string[] {
  return [`${path}.sirannon-key-change`, `${path}.sirannon-key-change-marker`, `${path}.restoring`].filter(existsSync)
}

function labelsUnderKey(path: string, key: Uint8Array): string[] {
  const Sqlcipher = require('@delali/sirannon-sqlite') as typeof StockDatabase
  const reader = new Sqlcipher(path, { readonly: true, nativeBinding: binding ?? undefined })
  try {
    reader.pragma(`key = "x'${Buffer.from(key).toString('hex')}'"`)
    return (reader.prepare('SELECT label FROM orders ORDER BY id').all() as { label: string }[]).map(row => row.label)
  } finally {
    reader.close()
  }
}

describe.skipIf(binding === null)('a key change on a database that a replication engine writes to', () => {
  let sirannon: Sirannon
  let driver: SQLiteDriver

  beforeEach(() => {
    driver = betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined })
    sirannon = new Sirannon({ driver })
    stops.push(() => sirannon.shutdown())
  })

  it('refuses with REPLICATION_ACTIVE, leaves the file and the engine connection on the current key, and runs once the engine stops', async () => {
    const path = join(dir, 'orders.db')
    const firstKey = randomBytes(32)
    const secondKey = randomBytes(32)
    const db = await sirannon.open('orders', path, { encryption: { key: () => Uint8Array.from(firstKey) } })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT NOT NULL)')
    const conn = await writerConnection(driver, path, firstKey)
    const engine = await startReplication(db, conn, 'orders')
    await engine.execute("INSERT INTO orders (label) VALUES ('before-refusal')")
    const inode = statSync(path).ino

    const refused = await codeOf(db.changeEncryption({ key: () => Uint8Array.from(secondKey) }))
    await engine.execute("INSERT INTO orders (label) VALUES ('after-refusal')")

    expect(refused).toBe('REPLICATION_ACTIVE')
    expect(statSync(path).ino).toBe(inode)
    expect(sidecarsOf(path)).toEqual([])
    expect((await db.encryptionStatus()).change.state).toBe('idle')
    expect(await db.query('SELECT label FROM orders ORDER BY id')).toEqual([
      { label: 'before-refusal' },
      { label: 'after-refusal' },
    ])
    expect(labelsUnderKey(path, firstKey)).toEqual(['before-refusal', 'after-refusal'])
    expect(() => labelsUnderKey(path, secondKey)).toThrow()

    await engine.stop()
    await conn.close()
    const changed = await db.changeEncryption({ key: () => Uint8Array.from(secondKey) })
    const reopened = await writerConnection(driver, path, secondKey)
    const restarted = await startReplication(db, reopened, 'orders')
    await restarted.execute("INSERT INTO orders (label) VALUES ('after-change')")

    expect(changed.change.state).toBe('done')
    expect(await db.query('SELECT label FROM orders ORDER BY id')).toEqual([
      { label: 'before-refusal' },
      { label: 'after-refusal' },
      { label: 'after-change' },
    ])
  })

  it('refuses an engine that starts while a key change runs, and the change finishes', async () => {
    const path = join(dir, 'orders.db')
    const firstKey = randomBytes(32)
    const secondKey = randomBytes(32)
    const db = await sirannon.open('orders', path, { encryption: { key: () => Uint8Array.from(firstKey) } })
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT NOT NULL)')
    await db.execute("INSERT INTO orders (label) VALUES ('kept')")
    const conn = await writerConnection(driver, path, firstKey)
    const held = gate()
    const change = db.changeEncryption({
      key: async () => {
        await held.promise
        return Uint8Array.from(secondKey)
      },
    })

    const refused = await codeOf(startReplication(db, conn, 'orders'))
    held.open()
    const finished = await change

    expect(refused).toBe('ENCRYPTION_CHANGE_IN_PROGRESS')
    expect(finished.change.state).toBe('done')
    expect(labelsUnderKey(path, secondKey)).toEqual(['kept'])
  })
})

describe('a restore of a database that a replication engine writes to', () => {
  it('refuses with REPLICATION_ACTIVE from the registry and from the server, before either closes the database', async () => {
    const sirannon = trackedRegistry()
    const orders = await openBackedUp(sirannon, dir, 'orders')
    const served = await serveRegistry(sirannon, { ...OPERATOR, acceptSql: true, acceptBackupRestore: true })
    const conn = await writerConnection(sirannon.driver, orders.path)
    const engine = await startReplication(orders.db, conn, 'orders')
    await engine.execute("INSERT INTO orders (label, note) VALUES ('before-refusal', '')")
    const inode = statSync(orders.path).ino
    let actionRan = false

    const offline = await codeOf(
      sirannon.withDatabaseOffline('orders', async () => {
        actionRan = true
      }),
    )
    const answer = await postJson(served.base, '/db/orders/backup/restore', {})
    const status = await sendRaw(served.base, 'GET', '/db/orders/backup/restore')
    await engine.execute("INSERT INTO orders (label, note) VALUES ('after-refusal', '')")
    const rows = await postJson(served.base, '/db/orders/query', { sql: 'SELECT label FROM orders ORDER BY id' })

    expect(offline).toBe('REPLICATION_ACTIVE')
    expect(actionRan).toBe(false)
    expect([answer.status, errorCodeOf(answer.body)]).toEqual([409, 'REPLICATION_ACTIVE'])
    expect(status.body).toMatchObject({ state: 'idle' })
    expect(sirannon.get('orders')).toBe(orders.db)
    expect(statSync(orders.path).ino).toBe(inode)
    expect(sidecarsOf(orders.path)).toEqual([])
    expect(rows.body).toEqual({ rows: [{ label: 'before-refusal' }, { label: 'after-refusal' }] })

    await engine.stop()
    const accepted = await postJson(served.base, '/db/orders/backup/restore', {})
    expect(accepted.status).toBe(202)
    expect(await restoreSettled(served.base, 'orders')).toMatchObject({ state: 'done' })
  })
})
