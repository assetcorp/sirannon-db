import { existsSync, statSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import type { ManagedNode } from '../lib/factory.js'
import type { FaultProxy } from '../lib/fault-proxy.js'
import { caughtUp, eventually, type Row, tableRows } from '../lib/replication-scene.js'
import { PRIMARY, REPLICA, ready, SECOND_REPLICA, sceneForEachTest } from './harness.js'

const handle = sceneForEachTest()

const DEFAULT_DURATION_MS = 120_000
const DEFAULT_ITERATION_DELAY_MS = 25
const DEFAULT_CHECKPOINT_EVERY = 10
const DEFAULT_REPLICATION_TIMEOUT_MS = 20_000
const DEFAULT_MAX_HEAP_BYTES = 512 * 1024 * 1024
const DEFAULT_MAX_STORAGE_BYTES = 256 * 1024 * 1024
const CUT_EVERY_ITERATIONS = 37
const CATEGORY_ADDED_AT_ITERATION = 5
const SOAK_TUNING = { batchIntervalMs: 20, batchSize: 100, maxBatchChanges: 2_000 }

const ITEMS_SCHEMA =
  'CREATE TABLE soak_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, counter INTEGER NOT NULL, active INTEGER NOT NULL, note TEXT, payload BLOB)'
const EVENTS_SCHEMA =
  'CREATE TABLE soak_events (id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL REFERENCES soak_items(id), kind TEXT NOT NULL, detail TEXT NOT NULL)'
const INSERT_EVENT = 'INSERT INTO soak_events (id, item_id, kind, detail) VALUES (?, ?, ?, ?)'

interface SoakOptions {
  durationMs: number
  iterationDelayMs: number
  checkpointEvery: number
  replicationTimeoutMs: number
  maxHeapBytes: number
  maxStorageBytes: number
}

interface ExpectedRows {
  items: Map<number, Row>
  events: Map<number, Row>
  categoryAdded: boolean
}

function readInteger(name: string, fallback: number, minimum: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === '') return fallback
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed < minimum)
    throw new Error(`${name} must be an integer of at least ${minimum}`)
  return parsed
}

function readSoakOptions(): SoakOptions {
  return {
    durationMs: readInteger('SIRANNON_SOAK_DURATION_MS', DEFAULT_DURATION_MS, 1),
    iterationDelayMs: readInteger('SIRANNON_SOAK_ITERATION_DELAY_MS', DEFAULT_ITERATION_DELAY_MS, 0),
    checkpointEvery: readInteger('SIRANNON_SOAK_CHECKPOINT_EVERY', DEFAULT_CHECKPOINT_EVERY, 1),
    replicationTimeoutMs: readInteger('SIRANNON_SOAK_REPLICATION_TIMEOUT_MS', DEFAULT_REPLICATION_TIMEOUT_MS, 1),
    maxHeapBytes: readInteger('SIRANNON_SOAK_MAX_HEAP_BYTES', DEFAULT_MAX_HEAP_BYTES, 1),
    maxStorageBytes: readInteger('SIRANNON_SOAK_MAX_STORAGE_BYTES', DEFAULT_MAX_STORAGE_BYTES, 1),
  }
}

async function addEvent(node: ManagedNode, expected: ExpectedRows, event: Row): Promise<void> {
  await node.engine.execute(INSERT_EVENT, [event.id, event.item_id, event.kind, event.detail])
  expected.events.set(Number(event.id), event)
}

async function runIteration(
  primary: ManagedNode,
  forwarder: ManagedNode,
  expected: ExpectedRows,
  iteration: number,
): Promise<void> {
  const item: Row = {
    id: iteration,
    name: `primary-${iteration}`,
    counter: iteration,
    active: iteration % 2,
    note: `note-${iteration}`,
    payload: Buffer.from(`payload-${iteration}`),
  }
  if (expected.categoryAdded) item.category = `category-${iteration % 3}`
  const columns = Object.keys(item)
  await primary.engine.execute(
    `INSERT INTO soak_items (${columns.join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`,
    Object.values(item),
  )
  expected.items.set(iteration, item)

  const previous = expected.items.get(iteration - 1)
  if (previous !== undefined) {
    await primary.engine.execute('UPDATE soak_items SET counter = counter + 1, note = ? WHERE id = ?', [
      `updated-${iteration}`,
      iteration - 1,
    ])
    previous.counter = Number(previous.counter) + 1
    previous.note = `updated-${iteration}`
  }

  if (iteration % 3 === 0) {
    const pair = [
      { id: iteration * 10, item_id: iteration, kind: 'primary-tx-a', detail: `detail-a-${iteration}` },
      { id: iteration * 10 + 1, item_id: iteration, kind: 'primary-tx-b', detail: `detail-b-${iteration}` },
    ]
    await primary.engine.transaction(async tx => {
      for (const event of pair) await tx.execute(INSERT_EVENT, [event.id, event.item_id, event.kind, event.detail])
    })
    for (const event of pair) expected.events.set(event.id, event)
  }

  if (iteration % 4 === 0) {
    await addEvent(forwarder, expected, {
      id: iteration * 10 + 2,
      item_id: iteration,
      kind: 'forwarded',
      detail: `forwarded-${iteration}`,
    })
  }

  if (iteration > 20 && iteration % 5 === 0) {
    const oldId = iteration - 20
    await primary.engine.execute('DELETE FROM soak_events WHERE item_id = ?', [oldId])
    await primary.engine.execute('DELETE FROM soak_items WHERE id = ?', [oldId])
    for (const [id, event] of expected.events) if (event.item_id === oldId) expected.events.delete(id)
    expected.items.delete(oldId)
  }

  if (!expected.categoryAdded && iteration >= CATEGORY_ADDED_AT_ITERATION) {
    await primary.engine.execute("ALTER TABLE soak_items ADD COLUMN category TEXT DEFAULT 'general'")
    for (const row of expected.items.values()) row.category = 'general'
    expected.categoryAdded = true
  }
}

function sorted(rows: Iterable<Row>): Row[] {
  return [...rows].sort((left, right) => Number(left.id) - Number(right.id))
}

async function assertHealthy(
  primary: ManagedNode,
  replicas: ManagedNode[],
  expected: ExpectedRows,
  options: SoakOptions,
): Promise<void> {
  expect(tableRows(primary.dbPath, 'soak_items')).toEqual(sorted(expected.items.values()))
  expect(tableRows(primary.dbPath, 'soak_events')).toEqual(sorted(expected.events.values()))
  for (const replica of replicas) {
    await caughtUp(replica, primary, options.replicationTimeoutMs)
    expect(tableRows(replica.dbPath, 'soak_items')).toEqual(sorted(expected.items.values()))
    expect(tableRows(replica.dbPath, 'soak_events')).toEqual(sorted(expected.events.values()))
  }
  for (const node of [primary, ...replicas]) {
    expect(node.recentErrors.filter(event => !event.recoverable)).toEqual([])
  }
}

function storageBytes(node: ManagedNode): number {
  return [node.dbPath, `${node.dbPath}-wal`, `${node.dbPath}-shm`]
    .filter(path => existsSync(path))
    .reduce((total, path) => total + statSync(path).size, 0)
}

async function pause(ms: number): Promise<void> {
  const until = Date.now() + ms
  await eventually(() => Date.now() >= until, 'the pause between iterations', ms + 1_000)
}

describe('sustained replication over mTLS', () => {
  it('keeps two replicas equal to the rows written while one link is cut over and over', async () => {
    const { scene } = handle
    const options = readSoakOptions()
    const primary = await scene.primary(PRIMARY, {
      schema: [ITEMS_SCHEMA, EVENTS_SCHEMA],
      watch: ['soak_items', 'soak_events'],
      config: SOAK_TUNING,
    })
    const link: FaultProxy = await scene.proxy(primary.port)
    const direct = await scene.replica(REPLICA, { port: primary.port, config: SOAK_TUNING })
    const cut = await scene.replica(SECOND_REPLICA, { port: link.port, config: SOAK_TUNING })
    await ready(direct, cut)

    const expected: ExpectedRows = { items: new Map(), events: new Map(), categoryAdded: false }
    const deadline = Date.now() + options.durationMs
    const startHeapBytes = process.memoryUsage().heapUsed
    let iteration = 0

    while (iteration === 0 || Date.now() < deadline) {
      iteration += 1
      await runIteration(primary, direct, expected, iteration)
      if (iteration % CUT_EVERY_ITERATIONS === 0) link.cut()
      if (iteration % options.checkpointEvery === 0) await assertHealthy(primary, [direct, cut], expected, options)
      if (Date.now() < deadline) await pause(options.iterationDelayMs)
    }

    await assertHealthy(primary, [direct, cut], expected, options)
    expect(link.opened).toBeGreaterThanOrEqual(Math.floor(iteration / CUT_EVERY_ITERATIONS))
    expect(process.memoryUsage().heapUsed).toBeLessThanOrEqual(Math.max(options.maxHeapBytes, startHeapBytes * 4))
    for (const node of [primary, direct, cut]) expect(storageBytes(node)).toBeLessThanOrEqual(options.maxStorageBytes)
  })
})
