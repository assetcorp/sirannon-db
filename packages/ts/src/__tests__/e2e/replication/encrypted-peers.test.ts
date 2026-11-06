import { randomBytes } from 'node:crypto'
import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { Client, credentials, type ServiceError, status } from '@grpc/grpc-js'
import type StockDatabase from 'better-sqlite3'
import { describe, expect, it } from 'vitest'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { ReplicationErrorEvent } from '../../../replication/index.js'
import { ReplicationMessage } from '../../../transport/grpc/generated/replication.js'
import { builtSirannonSqliteBindingPath, SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import type { ManagedNode } from '../lib/factory.js'
import { eventually, readRows, recordErrors } from '../lib/replication-scene.js'
import { INSERT_ORDER, ORDERS_SCHEMA, PRIMARY, READY_DEADLINE_MS, REPLICA, ready, sceneForEachTest } from './harness.js'

const binding = builtSirannonSqliteBindingPath()
const require = createRequire(import.meta.url)
const handle = sceneForEachTest()

const ORDER_COUNT = 40
const customerOf = (id: number) => `ledger-customer-${id.toString().padStart(4, '0')}-confidential`
const REPLICATE_PATH = '/sirannon.replication.v1.Replication/Replicate'

function encryptingDriver(): SQLiteDriver {
  return betterSqlite3({ encryption: true, sqliteBindingPath: binding ?? undefined })
}

async function writeOrders(primary: ManagedNode): Promise<void> {
  for (let id = 1; id <= ORDER_COUNT; id++) await primary.engine.execute(INSERT_ORDER, [id, customerOf(id), id * 100])
}

function plaintextIn(path: string): boolean {
  for (const file of [path, `${path}-wal`, `${path}-journal`]) {
    if (!existsSync(file)) continue
    const bytes = readFileSync(file)
    for (let id = 1; id <= ORDER_COUNT; id++) {
      if (bytes.includes(Buffer.from(customerOf(id), 'utf8'))) return true
    }
  }
  return false
}

function ordersUnderKey(path: string, key: Uint8Array): { id: number; customer: string }[] {
  const Sqlcipher = require('@delali/sirannon-sqlite') as typeof StockDatabase
  const reader = new Sqlcipher(path, { readonly: true, nativeBinding: binding ?? undefined })
  try {
    reader.pragma(`key = "x'${Buffer.from(key).toString('hex')}'"`)
    return reader.prepare('SELECT id, customer FROM orders ORDER BY id').all() as { id: number; customer: string }[]
  } finally {
    reader.close()
  }
}

function refusals(events: readonly ReplicationErrorEvent[]): ReplicationErrorEvent[] {
  return events.filter(event => (event.error as { code?: string }).code === 'ENCRYPTION_REQUIRED')
}

function lengthDelimited(field: number, payload: Buffer): Buffer {
  const length: number[] = []
  let remaining = payload.length
  while (remaining > 0x7f) {
    length.push((remaining & 0x7f) | 0x80)
    remaining >>>= 7
  }
  length.push(remaining)
  return Buffer.concat([Buffer.from([(field << 3) | 2, ...length]), payload])
}

function helloWithoutEncryptedField(nodeId: string): Buffer {
  const hello = Buffer.concat([
    lengthDelimited(1, Buffer.from(nodeId, 'utf8')),
    lengthDelimited(2, Buffer.from('replica', 'utf8')),
  ])
  return lengthDelimited(1, hello)
}

interface OlderPeerOutcome {
  serverHelloEncrypted: boolean | null
  batches: number
  code: number | null
  trailerCode: string | null
}

function dialAsOlderPeer(port: number, nodeId: string): Promise<OlderPeerOutcome> {
  const cert = handle.certs.certForNode(nodeId)
  const client = new Client(
    `localhost:${port}`,
    credentials.createSsl(
      readFileSync(handle.certs.caCertPath),
      readFileSync(cert.keyPath),
      readFileSync(cert.certPath),
    ),
  )
  const call = client.makeBidiStreamRequest(
    REPLICATE_PATH,
    (bytes: Buffer) => bytes,
    (bytes: Buffer) => ReplicationMessage.decode(bytes),
  )
  const outcome: OlderPeerOutcome = { serverHelloEncrypted: null, batches: 0, code: null, trailerCode: null }
  return new Promise(resolve => {
    const finish = (): void => {
      client.close()
      resolve(outcome)
    }
    call.on('data', (message: ReplicationMessage) => {
      if (message.hello) outcome.serverHelloEncrypted = message.hello.encrypted
      if (message.batch) outcome.batches++
    })
    call.on('error', (error: ServiceError) => {
      outcome.code = error.code
      const [trailer] = error.metadata?.get('sirannon-error-code') ?? []
      outcome.trailerCode = typeof trailer === 'string' ? trailer : null
      finish()
    })
    call.on('end', finish)
    call.write(helloWithoutEncryptedField(nodeId))
  })
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(binding).not.toBeNull()
  })
})

describe.skipIf(binding === null)('replication between encrypted and plaintext nodes', () => {
  it('sends a plaintext replica no row from an encrypted primary, which leaves the replica file without a single row', async () => {
    const key = randomBytes(32)
    const primary = await handle.scene.primary(PRIMARY, {
      driver: encryptingDriver(),
      encryptionKey: key,
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
    })
    const primaryErrors = recordErrors(primary)
    await writeOrders(primary)
    const replica = await handle.scene.replica(REPLICA, { port: primary.port })

    await eventually(() => refusals(primaryErrors).length >= 2, 'two refused connections of the plaintext replica')

    expect(refusals(primaryErrors)[0]?.peerId).toBe(REPLICA)
    expect(replica.engine.status().syncState?.phase).not.toBe('ready')
    expect(plaintextIn(replica.dbPath)).toBe(false)
    expect(readRows(replica.dbPath, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'orders'")).toEqual(
      [],
    )
    expect(plaintextIn(primary.dbPath)).toBe(false)
    expect(ordersUnderKey(primary.dbPath, key)).toHaveLength(ORDER_COUNT)
  })

  it('treats a peer whose Hello carries no encrypted field as plaintext, and ends its stream with ENCRYPTION_REQUIRED before any batch', async () => {
    const primary = await handle.scene.primary(PRIMARY, {
      driver: encryptingDriver(),
      encryptionKey: randomBytes(32),
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
    })
    await writeOrders(primary)

    const outcome = await dialAsOlderPeer(primary.port, REPLICA)

    expect(outcome).toEqual({
      serverHelloEncrypted: true,
      batches: 0,
      code: status.FAILED_PRECONDITION,
      trailerCode: 'ENCRYPTION_REQUIRED',
    })
  })

  it('makes a refused replica wait longer before each redial, and the replica never receives data', async () => {
    const primary = await handle.scene.primary(PRIMARY, {
      driver: encryptingDriver(),
      encryptionKey: randomBytes(32),
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
    })
    const primaryErrors = recordErrors(primary)
    await writeOrders(primary)
    const refusedAt: number[] = []
    primary.engine.on('replication-error', (event: ReplicationErrorEvent) => {
      if ((event.error as { code?: string }).code === 'ENCRYPTION_REQUIRED') refusedAt.push(Date.now())
    })
    const replica = await handle.scene.replica(REPLICA, { port: primary.port })

    await eventually(() => refusedAt.length >= 4, 'four refused connections', 15_000)
    const gaps = refusedAt.slice(1).map((at, index) => at - (refusedAt[index] ?? at))

    expect(refusals(primaryErrors).every(event => event.peerId === REPLICA)).toBe(true)
    expect(gaps[0]).toBeGreaterThanOrEqual(400)
    expect(gaps[1]).toBeGreaterThanOrEqual(900)
    expect(gaps[2]).toBeGreaterThanOrEqual(1_800)
    expect(plaintextIn(replica.dbPath)).toBe(false)
    expect(replica.engine.getAppliedSeq(PRIMARY)).toBe(0n)
  }, 30_000)

  it('lets a plaintext primary feed an encrypted replica, which holds every row only as ciphertext', async () => {
    const key = randomBytes(32)
    const primary = await handle.scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const primaryErrors = recordErrors(primary)
    await writeOrders(primary)
    const replica = await handle.scene.replica(REPLICA, {
      port: primary.port,
      driver: encryptingDriver(),
      encryptionKey: key,
    })
    await ready(replica)
    await eventually(
      () => replica.engine.getAppliedSeq(PRIMARY) >= primary.engine.getCurrentSeq(),
      'the encrypted replica to apply every order',
      READY_DEADLINE_MS,
    )

    const expected = Array.from({ length: ORDER_COUNT }, (_, index) => ({
      id: index + 1,
      customer: customerOf(index + 1),
    }))
    expect(await replica.db.query('SELECT id, customer FROM orders ORDER BY id')).toEqual(expected)
    expect(ordersUnderKey(replica.dbPath, key)).toEqual(expected)
    expect(plaintextIn(replica.dbPath)).toBe(false)
    expect(() => readRows(replica.dbPath, 'SELECT id FROM orders')).toThrow()
    expect(refusals(primaryErrors)).toEqual([])
  })
})
