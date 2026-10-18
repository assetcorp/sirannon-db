import { afterAll, afterEach, beforeAll, expect } from 'vitest'
import { createMtlsCerts, type MtlsCerts } from '../lib/certs.js'
import type { ManagedNode } from '../lib/factory.js'
import { caughtUp, eventually, ReplicationScene, type Row, tableRows } from '../lib/replication-scene.js'
import { waitForReady } from '../lib/waitForReplica.js'

export const PRIMARY = 'primary-orders-east'
export const REPLICA = 'replica-orders-west'
export const SECOND_REPLICA = 'replica-orders-north'
export const THIRD_REPLICA = 'replica-orders-south'
export const INTRUDER = 'intruder-orders'

export const NODE_IDS = [PRIMARY, REPLICA, SECOND_REPLICA, THIRD_REPLICA, INTRUDER] as const

export const READY_DEADLINE_MS = 20_000

export const ORDERS_SCHEMA =
  'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)'
export const INSERT_ORDER = 'INSERT INTO orders (id, customer, total) VALUES (?, ?, ?)'

export interface SceneHandle {
  readonly certs: MtlsCerts
  readonly scene: ReplicationScene
}

export function sceneForEachTest(): SceneHandle {
  let certs: MtlsCerts | undefined
  let scene: ReplicationScene | undefined

  beforeAll(async () => {
    certs = await createMtlsCerts(NODE_IDS)
  })

  afterEach(async () => {
    await scene?.teardown()
    scene = undefined
  })

  afterAll(() => {
    certs?.cleanup()
  })

  return {
    get certs(): MtlsCerts {
      if (certs === undefined) throw new Error('The certificates are created in beforeAll')
      return certs
    },
    get scene(): ReplicationScene {
      if (certs === undefined) throw new Error('The certificates are created in beforeAll')
      scene ??= new ReplicationScene(certs)
      return scene
    },
  }
}

export async function ready(...replicas: ManagedNode[]): Promise<void> {
  for (const replica of replicas) {
    await waitForReady(replica.engine, READY_DEADLINE_MS).catch((error: unknown) => {
      const seen = replica.recentErrors.map(event => `${event.operation}: ${event.error.message}`)
      throw new Error(`${String(error)}; errors on ${replica.nodeId}: ${seen.join(' | ') || 'none'}`)
    })
  }
}

export async function converged(primary: ManagedNode, replicas: ManagedNode[], table: string, expected: Row[]) {
  expect(tableRows(primary.dbPath, table)).toEqual(expected)
  for (const replica of replicas) {
    await caughtUp(replica, primary)
    await eventually(
      () => JSON.stringify(tableRows(replica.dbPath, table), jsonSafe) === JSON.stringify(expected, jsonSafe),
      `${replica.nodeId} to hold the expected rows of ${table}`,
    )
    expect(tableRows(replica.dbPath, table)).toEqual(expected)
  }
}

function jsonSafe(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return `${value}n`
  if (value instanceof Uint8Array) return `blob:${Buffer.from(value).toString('hex')}`
  if (typeof value === 'object' && value !== null && (value as { type?: string }).type === 'Buffer') {
    return `blob:${Buffer.from((value as { data: number[] }).data).toString('hex')}`
  }
  return value
}

export function order(id: number, customer: string, total: number): Row {
  return { id, customer, total }
}
