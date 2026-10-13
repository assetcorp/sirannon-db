import { type ChildProcess, spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { createServer, type Server } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it, onTestFinished } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { PrimaryReplicaTopology, ReplicationEngine, type ReplicationErrorEvent } from '../../../replication/index.js'
import { GrpcReplicationTransport } from '../../../transport/grpc/index.js'
import {
  failingOutboundReadDriver,
  OUTBOUND_READ_FAILURE,
} from '../../helpers/fault-drivers/failing-outbound-read-driver.js'
import { decodeHlc } from '../lib/device-sync-oracle.js'
import type { ManagedNode } from '../lib/factory.js'
import {
  caughtUp,
  errorsOf,
  eventually,
  type Row,
  readRows,
  recordErrors,
  tableRows,
} from '../lib/replication-scene.js'
import { converged, INSERT_ORDER, order, PRIMARY, REPLICA, ready, SECOND_REPLICA, sceneForEachTest } from './harness.js'

const handle = sceneForEachTest()

const DURABLE_ORDERS_SCHEMA =
  'CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)'
const PACKAGE_ROOT = fileURLToPath(new URL('../../../../', import.meta.url))
const HERE = fileURLToPath(new URL('.', import.meta.url))
const SHUTDOWN_BOUND_MS = 5_000

function orders(from: number, count: number, label: string): Row[] {
  return Array.from({ length: count }, (_, at) => order(from + at, `${label}-${from + at}`, from + at))
}

async function writeOrders(node: ManagedNode, rows: Row[]): Promise<void> {
  for (const row of rows) await node.engine.execute(INSERT_ORDER, [row.id, row.customer, row.total])
}

function ackedBy(primary: ManagedNode, replicaId: string): bigint {
  return primary.engine.status().peers.find(peer => peer.nodeId === replicaId)?.lastAckedSeq ?? -1n
}

function spawnScript(script: string, args: string[], ipc: boolean): ChildProcess {
  const child = spawn(process.execPath, ['--import', 'tsx', join(HERE, script), ...args], {
    cwd: PACKAGE_ROOT,
    stdio: ipc ? ['ignore', 'pipe', 'pipe', 'ipc'] : ['ignore', 'pipe', 'pipe'],
  })
  onTestFinished(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
  })
  return child
}

function firstLine(child: ChildProcess): Promise<string> {
  return new Promise((resolve, reject) => {
    let buffered = ''
    let failure = ''
    child.stdout?.on('data', (chunk: Buffer) => {
      buffered += chunk.toString('utf8')
      const end = buffered.indexOf('\n')
      if (end >= 0) resolve(buffered.slice(0, end))
    })
    child.stderr?.on('data', (chunk: Buffer) => {
      failure += chunk.toString('utf8')
    })
    child.once('exit', code => reject(new Error(`The child process exited with ${code}: ${failure}`)))
  })
}

function exited(child: ChildProcess): Promise<number | null> {
  if (child.exitCode !== null) return Promise.resolve(child.exitCode)
  return new Promise(resolve => child.once('exit', code => resolve(code)))
}

async function listening(port: number, host: string): Promise<Server> {
  const server = createServer()
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(port, host, () => resolve())
  })
  return server
}

function portOf(server: Server): number {
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('The blocking server has no TCP port')
  return address.port
}

async function takenPort(): Promise<{ port: number; free: () => Promise<void> }> {
  const ipv4 = await listening(0, '127.0.0.1')
  const port = portOf(ipv4)
  const ipv6 = await listening(port, '::1')
  const free = async (): Promise<void> => {
    for (const server of [ipv4, ipv6]) {
      if (server.listening) await new Promise<void>(resolve => server.close(() => resolve()))
    }
  }
  return { port, free }
}

describe('replication through network faults and restarts', () => {
  it('redials on its own and delivers every write once when the stream drops while acknowledgements are held', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [DURABLE_ORDERS_SCHEMA],
      watch: ['orders'],
      config: { batchIntervalMs: 10, batchSize: 1, maxPendingBatches: 1, ackTimeoutMs: 150 },
    })
    const proxy = await scene.proxy(primary.port)
    const replica = await scene.replica(REPLICA, { port: proxy.port, config: { batchIntervalMs: 10 } })
    await ready(replica)

    proxy.delay('to-target', 60_000)
    const rows = orders(1, 3, 'held')
    await writeOrders(primary, rows)
    await eventually(() => tableRows(replica.dbPath, 'orders').length >= 1, 'the replica to apply the first write')
    const target = primary.engine.getCurrentSeq()
    expect(ackedBy(primary, REPLICA)).toBeLessThan(target)
    const connectionsBefore = proxy.opened
    proxy.cut()
    proxy.delay('to-target', 0)

    await eventually(() => ackedBy(primary, REPLICA) >= target, 'the primary to hear every acknowledgement')
    expect(proxy.opened).toBeGreaterThan(connectionsBefore)
    await converged(primary, [replica], 'orders', rows)
  })

  it('delivers every write once the link drops with several batches in flight and later writes follow the redial', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [DURABLE_ORDERS_SCHEMA],
      watch: ['orders'],
      config: { batchIntervalMs: 10, batchSize: 1, maxPendingBatches: 10, ackTimeoutMs: 4_000 },
    })
    const proxy = await scene.proxy(primary.port)
    const replica = await scene.replica(REPLICA, { port: proxy.port, config: { batchIntervalMs: 10 } })
    await ready(replica)

    proxy.stall()
    const lost = orders(1, 8, 'in-flight')
    await writeOrders(primary, lost)
    await eventually(
      () => (primary.engine.status().peers.find(peer => peer.nodeId === REPLICA)?.pendingBatches ?? 0) >= 5,
      'several batches to be in flight',
    )
    proxy.cut()
    proxy.resume()
    await eventually(
      () => replica.engine.status().peers.some(peer => peer.nodeId === PRIMARY && peer.connected),
      'the replica to redial',
    )
    const later = orders(9, 3, 'after-the-redial')
    await writeOrders(primary, later)

    await converged(primary, [replica], 'orders', [...lost, ...later])
  })

  it('redials a primary that restarts on the same endpoint and keeps the writes from before and after', async () => {
    const { scene } = handle
    const first = await scene.primary(PRIMARY, { schema: [DURABLE_ORDERS_SCHEMA], watch: ['orders'] })
    const replica = await scene.replica(REPLICA, { port: first.port })
    await ready(replica)
    const before = orders(1, 5, 'before-restart')
    await writeOrders(first, before)
    await caughtUp(replica, first)

    const port = first.port
    const storage = await scene.stop(first)
    await eventually(
      () => !replica.engine.status().peers.some(peer => peer.nodeId === PRIMARY && peer.connected),
      'the replica to see the primary leave',
    )
    const second = await scene.primary(PRIMARY, {
      schema: [DURABLE_ORDERS_SCHEMA],
      watch: ['orders'],
      storage,
      listenPort: port,
    })
    const after = orders(6, 5, 'after-restart')
    await writeOrders(second, after)
    await second.engine.execute('UPDATE orders SET total = 0 WHERE id = 1')

    await converged(second, [replica], 'orders', [{ ...before[0], total: 0 }, ...before.slice(1), ...after])
    expect(replica.engine.status().syncState?.phase).toBe('ready')
  })

  it('lets a replica that restarts resume from its applied position while a second replica keeps receiving', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [DURABLE_ORDERS_SCHEMA], watch: ['orders'] })
    const restarting = await scene.replica(REPLICA, { port: primary.port })
    const steady = await scene.replica(SECOND_REPLICA, { port: primary.port })
    await ready(restarting, steady)
    await writeOrders(primary, orders(1, 10, 'first'))
    await caughtUp(restarting, primary)

    const storage = await scene.stop(restarting)
    await writeOrders(primary, orders(11, 40, 'while-down'))
    await primary.engine.execute('DELETE FROM orders WHERE id <= 5')
    await primary.engine.execute("UPDATE orders SET customer = 'renamed' WHERE id = 20")
    await caughtUp(steady, primary)
    const resumed = await scene.replica(REPLICA, { port: primary.port, storage })

    expect(resumed.engine.status().syncState?.phase).toBe('ready')
    const expected = tableRows(primary.dbPath, 'orders')
    expect(expected).toHaveLength(45)
    await converged(primary, [steady, resumed], 'orders', expected)
    expect(errorsOf(resumed, 'sync-response-timeout')).toEqual([])
  })

  it('keeps on the primary every change that an offline replica has yet to acknowledge while a subscriber prunes the change log', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [DURABLE_ORDERS_SCHEMA],
      watch: ['orders'],
      registryOptions: { cdcRetention: 200 },
    })
    await primary.db.watch('orders')
    const seen: unknown[] = []
    primary.db.on('orders').subscribe(event => seen.push(event))
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)
    await writeOrders(primary, orders(1, 1, 'acknowledged'))
    const acknowledged = primary.engine.getCurrentSeq()
    await eventually(() => ackedBy(primary, REPLICA) >= acknowledged, 'the replica to acknowledge the first write')

    const storage = await scene.stop(replica)
    const missed = orders(2, 10, 'missed')
    await writeOrders(primary, missed)
    await eventually(
      () =>
        readRows(primary.dbPath, 'SELECT count(*) AS n FROM _sirannon_changes WHERE seq <= ?', [acknowledged])[0]?.n ===
        0,
      'the subscriber to prune the acknowledged change',
      20_000,
    )

    expect(seen.length).toBeGreaterThanOrEqual(11)
    expect(
      readRows(primary.dbPath, 'SELECT count(*) AS n FROM _sirannon_changes WHERE seq > ?', [acknowledged]),
    ).toEqual([{ n: missed.length }])
    const restarted = await scene.replica(REPLICA, { port: primary.port, storage })
    await converged(primary, [restarted], 'orders', orders(1, 1, 'acknowledged').concat(missed))
  })

  it('reports a failed change-log read as a recoverable sender error and sends the backlog once reads succeed', async () => {
    const { scene } = handle
    const faults = failingOutboundReadDriver()
    const primary = await scene.primary(PRIMARY, {
      schema: [DURABLE_ORDERS_SCHEMA],
      watch: ['orders'],
      driver: faults.driver,
    })
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)
    const seen = recordErrors(primary)

    faults.arm()
    const rows = orders(1, 5, 'after-a-failed-read')
    await writeOrders(primary, rows)
    await eventually(() => faults.failedReads() >= 3, 'the sender to fail three reads')
    faults.disarm()

    const failure = seen.find(event => event.operation === 'sender-loop')
    expect(failure?.recoverable).toBe(true)
    expect(failure?.error.message).toBe(OUTBOUND_READ_FAILURE)
    await converged(primary, [replica], 'orders', rows)
  })

  it('keeps replicating when a replication-error listener throws', async () => {
    const { scene } = handle
    const faults = failingOutboundReadDriver()
    const primary = await scene.primary(PRIMARY, {
      schema: [DURABLE_ORDERS_SCHEMA],
      watch: ['orders'],
      driver: faults.driver,
    })
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)
    let thrown = 0
    primary.engine.on('replication-error', (_event: ReplicationErrorEvent) => {
      thrown += 1
      throw new Error('listener failure')
    })

    faults.arm()
    await writeOrders(primary, orders(1, 1, 'first'))
    await eventually(() => thrown >= 2, 'the listener to throw twice')
    faults.disarm()
    const rows = orders(1, 4, 'first')
    await writeOrders(primary, rows.slice(1))

    expect(primary.engine.status().replicating).toBe(true)
    await converged(primary, [replica], 'orders', rows)
  })

  it('leaves the engine stopped after a start fails on a taken port, and starts cleanly once the port frees', async () => {
    const { scene, certs } = handle
    const { port, free } = await takenPort()
    const driver = betterSqlite3()
    const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-failed-start-'))
    const storage = { tempDir, dbPath: join(tempDir, `${PRIMARY}.db`) }
    const conn = await driver.open(storage.dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    await conn.exec(DURABLE_ORDERS_SCHEMA)
    const tracker = new ChangeTracker()
    await tracker.watch(conn, 'orders')
    const sirannon = new Sirannon({ driver })
    const db = await sirannon.open(PRIMARY, storage.dbPath)
    const cert = certs.certForNode(PRIMARY)
    const transport = new GrpcReplicationTransport({
      host: 'localhost',
      port,
      tlsCert: cert.certPath,
      tlsKey: cert.keyPath,
      tlsCaCert: certs.caCertPath,
    })
    const engine = new ReplicationEngine(db, conn, {
      nodeId: PRIMARY,
      topology: new PrimaryReplicaTopology('primary'),
      transport,
      changeTracker: tracker,
      batchIntervalMs: 30,
      snapshotConnectionFactory: () => driver.open(storage.dbPath, { readonly: true }),
    })
    onTestFinished(async () => {
      await engine.stop().catch(() => {})
      await sirannon.shutdown().catch(() => {})
      await conn.close().catch(() => {})
      await free()
      rmSync(tempDir, { recursive: true, force: true })
    })

    await expect(engine.start()).rejects.toThrow()
    expect(engine.status().replicating).toBe(false)
    await free()
    await engine.start()
    const replica = await scene.replica(REPLICA, { port })
    await ready(replica)
    await engine.execute(INSERT_ORDER, [1, 'after-a-failed-start', 1])

    expect(engine.status().replicating).toBe(true)
    await eventually(
      () => tableRows(replica.dbPath, 'orders').length === 1,
      'the replica to receive the write from the restarted engine',
    )
    expect(tableRows(replica.dbPath, 'orders')).toEqual([order(1, 'after-a-failed-start', 1)])
  })

  it('stops both nodes within a bounded time while the stream between them is stalled with batches in flight', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [DURABLE_ORDERS_SCHEMA], watch: ['orders'] })
    const proxy = await scene.proxy(primary.port)
    const replica = await scene.replica(REPLICA, { port: proxy.port })
    await ready(replica)

    proxy.stall()
    await writeOrders(primary, orders(1, 20, 'stalled'))
    await eventually(
      () => (primary.engine.status().peers.find(peer => peer.nodeId === REPLICA)?.pendingBatches ?? 0) > 0,
      'a batch to be in flight',
    )
    const startedAt = Date.now()
    await scene.stop(primary)
    await scene.stop(replica)

    expect(Date.now() - startedAt).toBeLessThan(SHUTDOWN_BOUND_MS)
  })

  it('brings level a replica that was offline for longer than its clock drift limit, applying the whole backlog without error', async () => {
    const { scene } = handle
    const driftLimitMs = 1_000
    const primary = await scene.primary(PRIMARY, { schema: [DURABLE_ORDERS_SCHEMA], watch: ['orders'] })
    const replica = await scene.replica(REPLICA, { port: primary.port, config: { maxClockDriftMs: driftLimitMs } })
    await ready(replica)
    const storage = await scene.stop(replica)
    const missed = orders(1, 5, 'while-offline')
    await writeOrders(primary, missed)
    const staleAt = Date.now() + driftLimitMs * 2
    await eventually(() => Date.now() >= staleAt, 'the backlog to age past the drift limit')

    const restarted = await scene.replica(REPLICA, {
      port: primary.port,
      storage,
      config: { maxClockDriftMs: driftLimitMs },
    })

    await converged(primary, [restarted], 'orders', missed)
    expect(errorsOf(restarted, 'batch-received')).toEqual([])
  })

  it('never stamps a change earlier than one stamped before a restart', async () => {
    const { scene } = handle
    const first = await scene.primary(PRIMARY, { schema: [DURABLE_ORDERS_SCHEMA], watch: ['orders'] })
    await writeOrders(first, orders(1, 30, 'before'))
    const storage = await scene.stop(first)
    const second = await scene.primary(PRIMARY, { schema: [DURABLE_ORDERS_SCHEMA], watch: ['orders'], storage })
    await writeOrders(second, orders(31, 30, 'after'))

    const stamps = readRows(second.dbPath, 'SELECT seq, hlc FROM _sirannon_changes ORDER BY seq').map(row =>
      decodeHlc(String(row.hlc)),
    )
    expect(stamps).toHaveLength(60)
    for (let at = 1; at < stamps.length; at += 1) {
      const previous = stamps[at - 1]
      const current = stamps[at]
      if (!previous || !current) throw new Error('A stamp is missing')
      const later =
        current.wallMs > previous.wallMs || (current.wallMs === previous.wallMs && current.logical > previous.logical)
      expect(later, `stamp ${at} follows stamp ${at - 1}`).toBe(true)
    }
  })

  it('brings a replica level with a primary that is killed mid-write and restarted from its file', async () => {
    const { scene, certs } = handle
    const placeholder = await scene.primary(PRIMARY, { schema: [DURABLE_ORDERS_SCHEMA], watch: ['orders'] })
    const storage = await scene.stop(placeholder)
    const port = placeholder.port
    const cert = certs.certForNode(PRIMARY)
    const child = spawnScript(
      'primary-process.ts',
      [
        PRIMARY,
        certs.caCertPath,
        cert.certPath,
        cert.keyPath,
        storage.tempDir,
        storage.dbPath,
        String(port),
        DURABLE_ORDERS_SCHEMA,
        INSERT_ORDER,
      ],
      false,
    )
    expect(JSON.parse(await firstLine(child))).toEqual({ port })
    const replica = await scene.replica(REPLICA, { port })
    await ready(replica)
    await eventually(() => tableRows(replica.dbPath, 'orders').length >= 50, 'the replica to hold fifty orders')

    child.kill('SIGKILL')
    await exited(child)
    const restarted = await scene.primary(PRIMARY, {
      schema: [DURABLE_ORDERS_SCHEMA],
      watch: ['orders'],
      storage,
      listenPort: port,
    })
    const survived = tableRows(restarted.dbPath, 'orders')
    await writeOrders(restarted, orders(survived.length + 1, 5, 'after-the-kill'))

    expect(survived.map(row => row.id)).toEqual(Array.from({ length: survived.length }, (_, at) => at + 1))
    expect(readRows(restarted.dbPath, 'PRAGMA integrity_check')).toEqual([{ integrity_check: 'ok' }])
    await converged(restarted, [replica], 'orders', tableRows(restarted.dbPath, 'orders'))
    expect(readRows(replica.dbPath, 'PRAGMA integrity_check')).toEqual([{ integrity_check: 'ok' }])
  })

  it('keeps its process alive while it waits to redial, and lets the process exit once it disconnects', async () => {
    const { scene, certs } = handle
    const unreachable = await scene.proxy(1)
    unreachable.refuse(true)
    const cert = certs.certForNode(REPLICA)
    const child = spawnScript(
      'dialling-replica-process.ts',
      [REPLICA, certs.caCertPath, cert.certPath, cert.keyPath, `localhost:${unreachable.port}`],
      true,
    )
    const messages: unknown[] = []
    child.on('message', message => messages.push(message))
    await eventually(() => messages.includes('dialling'), 'the child to start dialling')
    await eventually(() => unreachable.opened >= 3, 'the child to redial three times')

    expect(child.exitCode).toBeNull()
    child.send('disconnect')
    const code = await exited(child)

    expect(messages).toContain('disconnected')
    expect(code).toBe(0)
  })
})
