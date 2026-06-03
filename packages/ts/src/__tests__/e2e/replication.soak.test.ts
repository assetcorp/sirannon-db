import { existsSync, statSync } from 'node:fs'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import {
  attachDiagnostics,
  compareDatabases,
  createMtlsCerts,
  createPrimary,
  createReplica,
  type DiagnosticsHandle,
  type ManagedNode,
  type MtlsCerts,
  stopNode,
  waitForReady,
  waitForReplica,
} from './lib/index.js'

const PRIMARY_ID = 'primary-soak-aaaa'
const REPLICA_ID = 'replica-soak-bbbb'

const DEFAULT_DURATION_MS = 120_000
const DEFAULT_ITERATION_DELAY_MS = 25
const DEFAULT_CHECKPOINT_EVERY = 10
const DEFAULT_REPLICATION_TIMEOUT_MS = 20_000
const DEFAULT_MAX_HEAP_BYTES = 512 * 1024 * 1024
const DEFAULT_MAX_STORAGE_BYTES = 256 * 1024 * 1024

const SCHEMA = `
  CREATE TABLE soak_items (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    counter INTEGER NOT NULL,
    active INTEGER NOT NULL,
    note TEXT,
    payload BLOB
  );

  CREATE TABLE soak_events (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    FOREIGN KEY (item_id) REFERENCES soak_items(id)
  )
`

describe('E2E soak: sustained gRPC replication over mTLS', () => {
  let certs: MtlsCerts
  let primary: ManagedNode | null = null
  let replica: ManagedNode | null = null
  let diagnostics: DiagnosticsHandle | null = null

  beforeAll(async () => {
    certs = await createMtlsCerts([PRIMARY_ID, REPLICA_ID])
  })

  afterAll(() => {
    certs?.cleanup()
  })

  beforeEach(async () => {
    primary = await createPrimary({
      nodeId: PRIMARY_ID,
      certs,
      initialize: async (conn, tracker) => {
        await conn.exec(SCHEMA)
        await tracker.watch(conn, 'soak_items')
        await tracker.watch(conn, 'soak_events')
      },
      configOverrides: {
        batchIntervalMs: 20,
        batchSize: 100,
        maxBatchChanges: 2_000,
      },
    })

    replica = await createReplica({
      nodeId: REPLICA_ID,
      certs,
      primaryHost: 'localhost',
      primaryPort: primary.port,
      configOverrides: {
        batchIntervalMs: 20,
        batchSize: 100,
        maxBatchChanges: 2_000,
        writeForwarding: true,
      },
    })

    diagnostics = attachDiagnostics(primary, replica)
    await waitForReady(replica.engine, 20_000)
  })

  afterEach(async ctx => {
    diagnostics?.dumpIfFailed(ctx)
    if (replica) await stopNode(replica)
    if (primary) await stopNode(primary)
    diagnostics?.cleanup()
    primary = null
    replica = null
    diagnostics = null
  })

  it('keeps replicas converged under a sustained mixed workload', async () => {
    if (!primary || !replica) throw new Error('nodes not initialised')

    const options = readSoakOptions()
    const deadline = Date.now() + options.durationMs
    const startHeapBytes = process.memoryUsage().heapUsed
    let iteration = 0
    let categoryColumnAdded = false

    while (iteration === 0 || Date.now() < deadline) {
      iteration += 1
      await runWorkloadIteration(primary, replica, iteration, categoryColumnAdded)

      if (!categoryColumnAdded && iteration >= 5) {
        await primary.engine.execute("ALTER TABLE soak_items ADD COLUMN category TEXT DEFAULT 'general'")
        categoryColumnAdded = true
      }

      if (iteration % options.checkpointEvery === 0) {
        await assertHealthy(primary, replica, options)
      }

      if (Date.now() < deadline) {
        await sleep(options.iterationDelayMs)
      }
    }

    await assertHealthy(primary, replica, options)
    expect(process.memoryUsage().heapUsed).toBeLessThanOrEqual(Math.max(options.maxHeapBytes, startHeapBytes * 4))
    expect(totalNodeStorageBytes(primary)).toBeLessThanOrEqual(options.maxStorageBytes)
    expect(totalNodeStorageBytes(replica)).toBeLessThanOrEqual(options.maxStorageBytes)
  })
})

interface SoakOptions {
  durationMs: number
  iterationDelayMs: number
  checkpointEvery: number
  replicationTimeoutMs: number
  maxHeapBytes: number
  maxStorageBytes: number
}

function readSoakOptions(): SoakOptions {
  return {
    durationMs: readPositiveInteger('SIRANNON_SOAK_DURATION_MS', DEFAULT_DURATION_MS),
    iterationDelayMs: readNonNegativeInteger('SIRANNON_SOAK_ITERATION_DELAY_MS', DEFAULT_ITERATION_DELAY_MS),
    checkpointEvery: readPositiveInteger('SIRANNON_SOAK_CHECKPOINT_EVERY', DEFAULT_CHECKPOINT_EVERY),
    replicationTimeoutMs: readPositiveInteger('SIRANNON_SOAK_REPLICATION_TIMEOUT_MS', DEFAULT_REPLICATION_TIMEOUT_MS),
    maxHeapBytes: readPositiveInteger('SIRANNON_SOAK_MAX_HEAP_BYTES', DEFAULT_MAX_HEAP_BYTES),
    maxStorageBytes: readPositiveInteger('SIRANNON_SOAK_MAX_STORAGE_BYTES', DEFAULT_MAX_STORAGE_BYTES),
  }
}

async function runWorkloadIteration(
  primary: ManagedNode,
  replica: ManagedNode,
  iteration: number,
  categoryColumnAdded: boolean,
): Promise<void> {
  const payload = Buffer.from(`payload-${iteration}`)
  const categoryColumn = categoryColumnAdded ? ', category' : ''
  const categoryPlaceholder = categoryColumnAdded ? ', ?' : ''
  const categoryParam = categoryColumnAdded ? [`category-${iteration % 3}`] : []

  await primary.engine.execute(
    `INSERT INTO soak_items (id, name, counter, active, note, payload${categoryColumn})
     VALUES (?, ?, ?, ?, ?, ?${categoryPlaceholder})`,
    [iteration, `primary-${iteration}`, iteration, iteration % 2, `note-${iteration}`, payload, ...categoryParam],
  )

  if (iteration > 1) {
    await primary.engine.execute('UPDATE soak_items SET counter = counter + 1, note = ? WHERE id = ?', [
      `updated-${iteration}`,
      iteration - 1,
    ])
  }

  if (iteration % 3 === 0) {
    await primary.engine.transaction(async tx => {
      await tx.execute('INSERT INTO soak_events (id, item_id, kind, detail) VALUES (?, ?, ?, ?)', [
        iteration * 10,
        iteration,
        'primary-tx-a',
        `detail-a-${iteration}`,
      ])
      await tx.execute('INSERT INTO soak_events (id, item_id, kind, detail) VALUES (?, ?, ?, ?)', [
        iteration * 10 + 1,
        iteration,
        'primary-tx-b',
        `detail-b-${iteration}`,
      ])
    })
  }

  if (iteration % 4 === 0) {
    await replica.engine.execute('INSERT INTO soak_events (id, item_id, kind, detail) VALUES (?, ?, ?, ?)', [
      iteration * 10 + 2,
      iteration,
      'forwarded',
      `forwarded-${iteration}`,
    ])
  }

  if (iteration > 20 && iteration % 5 === 0) {
    const oldId = iteration - 20
    await primary.engine.execute('DELETE FROM soak_events WHERE item_id = ?', [oldId])
    await primary.engine.execute('DELETE FROM soak_items WHERE id = ?', [oldId])
  }
}

async function assertHealthy(primary: ManagedNode, replica: ManagedNode, options: SoakOptions): Promise<void> {
  const targetSeq = primary.engine.getCurrentSeq()
  await waitForReplica(replica.engine, primary.nodeId, targetSeq, options.replicationTimeoutMs)
  await compareDatabases(primary.conn, replica.conn)
  expect(primary.recentErrors).toEqual([])
  expect(replica.recentErrors).toEqual([])
}

function totalNodeStorageBytes(node: ManagedNode): number {
  return fileSizeBytes(node.dbPath) + fileSizeBytes(`${node.dbPath}-wal`) + fileSizeBytes(`${node.dbPath}-shm`)
}

function fileSizeBytes(path: string): number {
  if (!existsSync(path)) {
    return 0
  }
  return statSync(path).size
}

function readPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === '') {
    return fallback
  }

  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }

  return parsed
}

function readNonNegativeInteger(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === '') {
    return fallback
  }

  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`)
  }

  return parsed
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms)
  })
}
