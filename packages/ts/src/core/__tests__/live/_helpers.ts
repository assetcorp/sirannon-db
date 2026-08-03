import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { Database } from '../../database.js'
import type { LiveQuery, LiveQueryState, LiveUpdate } from '../../live/types.js'
import { Sirannon } from '../../sirannon.js'

export interface LiveHarness {
  sirannon: Sirannon
  db: Database
  dispose(): Promise<void>
}

export async function openHarness(schema: string, pollIntervalMs = 5): Promise<LiveHarness> {
  const dir = mkdtempSync(join(tmpdir(), 'sirannon-live-'))
  const sirannon = new Sirannon({ driver: betterSqlite3() })
  const db = await sirannon.open('shop', join(dir, 'shop.db'), { cdcPollInterval: pollIntervalMs })
  await db.execute(schema)

  return {
    sirannon,
    db,
    async dispose() {
      await sirannon.shutdown().catch(() => {})
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

export function readyRows<T>(query: LiveQuery<T>): readonly T[] {
  const state = query.getState()
  if (state.status !== 'ready') {
    throw new Error(`Expected a ready live query, found '${state.status}'`)
  }
  return state.rows
}

export function isRevalidating<T>(query: LiveQuery<T>): boolean {
  const state = query.getState()
  return state.status === 'ready' && state.revalidating
}

export async function waitForRows<T>(
  query: LiveQuery<T>,
  predicate: (rows: readonly T[]) => boolean,
  timeoutMs = 4000,
): Promise<readonly T[]> {
  const deadline = Date.now() + timeoutMs
  let last: LiveQueryState<T> = query.getState()

  while (Date.now() < deadline) {
    last = query.getState()
    if (last.status === 'error') throw last.error
    if (last.status === 'ready' && predicate(last.rows)) return last.rows
    await new Promise(resolve => setTimeout(resolve, 5))
  }

  throw new Error(`Live query never satisfied the expectation. Last state: ${JSON.stringify(describe(last))}`)
}

export interface UpdateRecorder<T> {
  kinds: LiveUpdate<T>['kind'][]
  stop(): void
}

export function recordUpdates<T>(query: LiveQuery<T>): UpdateRecorder<T> {
  const kinds: LiveUpdate<T>['kind'][] = []
  const unsubscribe = query.subscribe(update => {
    kinds.push(update.kind)
  })
  return { kinds, stop: unsubscribe }
}

function describe<T>(state: LiveQueryState<T>): unknown {
  if (state.status === 'error') return { status: 'error', message: state.error.message }
  if (state.status === 'ready') return { status: 'ready', rows: state.rows }
  return { status: state.status }
}
