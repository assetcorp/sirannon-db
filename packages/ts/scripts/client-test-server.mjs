import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { generate } from 'selfsigned'
import { ChangeTracker, Sirannon, SirannonError } from '../dist/core/index.mjs'
import { betterSqlite3 } from '../dist/driver/better-sqlite3.mjs'
import { PrimaryReplicaTopology, ReplicationEngine } from '../dist/replication/index.mjs'
import { createServer } from '../dist/server/index.mjs'
import { GrpcReplicationTransport } from '../dist/transport/grpc.mjs'
import { sharedOperations } from './client-test-operations.mjs'

const settings = JSON.parse(process.env.SIRANNON_CLIENT_TEST_SERVER ?? '{}')
const HOST = '127.0.0.1'
const DATABASE_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,62}$/
const REPLICATED_DATABASE_ID = 'items'
const PRIMARY_NODE_ID = 'client-test-primary'
const REPLICA_NODE_ID = 'client-test-replica'
const READY_DEADLINE_MS = 30_000
const READY_POLL_MS = 20
const CERTIFICATE_LIFETIME_MS = 86_400_000
const IDLE_BACKUP_INTERVAL_MS = 3_600_000
const ITEMS_SCHEMA =
  'CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER, payload BLOB)'

const migrations = [{ version: 1, name: 'create_items', up: ITEMS_SCHEMA }]

function refusal(status, code, message) {
  return Object.assign(new Error(message), { status, code })
}

function authenticate(context) {
  const presented = context.headers.authorization ?? ''
  const offered = context.headers['sec-websocket-protocol'] ?? ''
  if (typeof settings.forbiddenToken === 'string' && presented === `Bearer ${settings.forbiddenToken}`) {
    throw refusal(403, 'FORBIDDEN_CALLER', 'This caller may not reach the database')
  }
  if (presented === `Bearer ${settings.token}` || offered.split(',').some(entry => entry.trim() === settings.token)) {
    return { user: 'client-test' }
  }
  throw refusal(401, 'TOKEN_REQUIRED', 'The request carries no valid token')
}

function memoryDestination() {
  const pieces = new Map()
  return {
    async writePiece(name, index, bytes) {
      pieces.set(`${name}/${index}`, Uint8Array.from(bytes))
    },
    async readPiece(name, index) {
      const stored = pieces.get(`${name}/${index}`)
      if (stored === undefined) throw new SirannonError(`No piece ${name}/${index}`, 'BACKUP_DESTINATION_ERROR')
      return stored
    },
    async listPieces(name) {
      const found = []
      for (const [key, bytes] of pieces) {
        if (key.startsWith(`${name}/`))
          found.push({ index: Number(key.slice(name.length + 1)), byteLength: bytes.length })
      }
      return found.sort((a, b) => a.index - b.index)
    },
  }
}

function serverOptions(extra) {
  const options = {
    host: HOST,
    port: settings.httpPort ?? 0,
    acceptSql: settings.acceptSql !== false,
    sharedOperations,
    ...extra,
  }
  if (typeof settings.cdcRetentionMs === 'number') options.cdcRetentionMs = settings.cdcRetentionMs
  if (typeof settings.token === 'string') {
    Object.assign(options, {
      authenticate,
      acceptDeviceSync: true,
      acceptBackupRestore: true,
      acceptEncryptionChange: true,
      encryptionKeys: async () => {
        throw new SirannonError('The client test server holds no keys', 'ENCRYPTION_KEY_UNAVAILABLE')
      },
    })
  }
  return options
}

async function startSingle() {
  const destination = memoryDestination()
  const sirannon = new Sirannon({
    driver: betterSqlite3(),
    migrations,
    lifecycle: {
      autoOpen: {
        resolver: id =>
          DATABASE_ID_PATTERN.test(id)
            ? {
                path: join(settings.directory, `${id}.db`),
                options: { backups: { destination, intervalMs: IDLE_BACKUP_INTERVAL_MS } },
              }
            : undefined,
      },
    },
  })
  const server = createServer(sirannon, serverOptions({}))
  await server.listen()
  return {
    ready: { port: server.listeningPort },
    async stop() {
      await server.close({ timeoutMs: 1_000 })
      await sirannon.shutdown()
    },
  }
}

async function issueCertificates(directory) {
  const caPath = join(directory, 'ca.pem')
  if (existsSync(caPath)) return
  mkdirSync(directory, { recursive: true })
  const notAfterDate = new Date(Date.now() + CERTIFICATE_LIFETIME_MS)
  const authority = await generate([{ name: 'commonName', value: 'Sirannon client test CA' }], {
    keySize: 2048,
    notAfterDate,
    extensions: [
      { name: 'basicConstraints', cA: true },
      { name: 'keyUsage', keyCertSign: true, cRLSign: true },
    ],
  })
  for (const nodeId of [PRIMARY_NODE_ID, REPLICA_NODE_ID]) {
    const pair = await generate([{ name: 'commonName', value: nodeId }], {
      keySize: 2048,
      notAfterDate,
      extensions: [
        {
          name: 'subjectAltName',
          altNames: [
            { type: 2, value: 'localhost' },
            { type: 7, ip: '127.0.0.1' },
          ],
        },
      ],
      ca: { key: authority.private, cert: authority.cert },
    })
    writeFileSync(join(directory, `${nodeId}.pem`), pair.cert)
    writeFileSync(join(directory, `${nodeId}-key.pem`), pair.private)
  }
  writeFileSync(caPath, authority.cert)
}

function grpcTransport(directory, nodeId, port) {
  return new GrpcReplicationTransport({
    port,
    host: 'localhost',
    tlsCert: join(directory, `${nodeId}.pem`),
    tlsKey: join(directory, `${nodeId}-key.pem`),
    tlsCaCert: join(directory, 'ca.pem'),
  })
}

async function waitUntilReady(engine) {
  const deadline = Date.now() + READY_DEADLINE_MS
  while (engine.status().syncState?.phase !== 'ready') {
    if (Date.now() >= deadline) throw new Error('The replica did not catch up with the primary in time')
    await new Promise(resolve => setTimeout(resolve, READY_POLL_MS))
  }
}

async function startReplicated(role) {
  const certificates = settings.certificates
  await issueCertificates(certificates)
  const driver = betterSqlite3()
  const dbPath = join(settings.directory, `${REPLICATED_DATABASE_ID}.db`)
  const conn = await driver.open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')
  const tracker = new ChangeTracker()
  if (role === 'primary') {
    await conn.exec(ITEMS_SCHEMA)
    await tracker.watch(conn, 'items')
  }
  const sirannon = new Sirannon({ driver })
  const db = await sirannon.open(REPLICATED_DATABASE_ID, dbPath)
  const nodeId = role === 'primary' ? PRIMARY_NODE_ID : REPLICA_NODE_ID
  const transport = grpcTransport(certificates, nodeId, role === 'primary' ? (settings.grpcPort ?? 0) : undefined)
  const shared = {
    nodeId,
    topology: new PrimaryReplicaTopology(role),
    transport,
    batchIntervalMs: 30,
    batchSize: 200,
    maxBatchChanges: 5_000,
    changeTracker: tracker,
    syncBatchSize: 500,
    syncAckTimeoutMs: 10_000,
  }
  const config =
    role === 'primary'
      ? { ...shared, initialSync: false, snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }) }
      : {
          ...shared,
          initialSync: true,
          transportConfig: { localRole: 'replica', endpoints: [`localhost:${settings.primaryGrpcPort}`] },
          catchUpDeadlineMs: 10_000,
          maxSyncLagBeforeReady: 10,
          writeForwarding: false,
        }
  const engine = new ReplicationEngine(db, conn, config)
  await engine.start()
  if (role === 'replica') await waitUntilReady(engine)
  const server = createServer(
    sirannon,
    serverOptions({ resolveExecutionTarget: id => (id === REPLICATED_DATABASE_ID ? engine : null) }),
  )
  await server.listen()
  return {
    ready: { port: server.listeningPort, grpcPort: role === 'primary' ? transport.getPort() : undefined },
    async stop() {
      await server.close({ timeoutMs: 1_000 })
      await engine.stop()
      await sirannon.shutdown()
      await conn.close()
    },
  }
}

const running =
  settings.mode === 'primary' || settings.mode === 'replica'
    ? await startReplicated(settings.mode)
    : await startSingle()
process.stdout.write(`READY ${JSON.stringify(running.ready)}\n`)

let stopping = false
async function stop() {
  if (stopping) return
  stopping = true
  try {
    await running.stop()
  } finally {
    process.exit(0)
  }
}

process.on('SIGTERM', () => void stop())
process.on('SIGINT', () => void stop())
