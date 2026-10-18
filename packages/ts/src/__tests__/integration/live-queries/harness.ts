import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import type { Database } from '../../../core/database.js'
import type { LiveQuery, LiveUpdate, ResultOp } from '../../../core/live/types.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions, Params, QueryMetrics, SirannonOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

export const FAST_POLL_MS = 5
export const QUIET_PERIOD_MS = 150
const WAIT_STEP_MS = 5
const WAIT_LIMIT_MS = 6_000

export interface Workspace {
  path(name: string): string
  registry(options?: Partial<SirannonOptions>): Sirannon
  open(name: string, options?: DatabaseOptions, registryOptions?: Partial<SirannonOptions>): Promise<Database>
  dispose(): Promise<void>
}

export function createWorkspace(label: string): Workspace {
  const dir = mkdtempSync(join(tmpdir(), `sirannon-live-queries-${label}-`))
  const registries: Sirannon[] = []
  const registry = (options?: Partial<SirannonOptions>): Sirannon => {
    const sirannon = new Sirannon({ driver: betterSqlite3(), ...options })
    registries.push(sirannon)
    return sirannon
  }
  return {
    path: name => join(dir, `${name}.db`),
    registry,
    open: (name, options, registryOptions) =>
      registry(registryOptions).open(name, join(dir, `${name}.db`), { cdcPollInterval: FAST_POLL_MS, ...options }),
    dispose: async () => {
      for (const sirannon of registries.splice(0)) await sirannon.shutdown().catch(() => {})
      rmSync(dir, { recursive: true, force: true })
    },
  }
}

export function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function waitFor(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + WAIT_LIMIT_MS
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`)
    await pause(WAIT_STEP_MS)
  }
}

function canonicalValue(value: unknown): unknown {
  if (typeof value === 'bigint') {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : `${value}n`
  }
  if (value instanceof Uint8Array) return `x'${Buffer.from(value).toString('hex')}'`
  if (typeof value === 'number' && Object.is(value, -0)) return 0
  return value
}

export function canonicalRows(rows: readonly unknown[]): string[] {
  return rows.map(row =>
    JSON.stringify(
      Object.entries(row as Record<string, unknown>).map(([column, value]) => [column, canonicalValue(value)]),
    ),
  )
}

export function readyRows<T>(query: LiveQuery<T>): readonly T[] {
  const state = query.getState()
  if (state.status !== 'ready') throw new Error(`Expected a ready live query, found '${state.status}'`)
  return state.rows
}

export function rowsOrStatus<T>(query: LiveQuery<T>): string[] | string {
  const state = query.getState()
  if (state.status === 'ready' && !state.revalidating) return canonicalRows(state.rows)
  if (state.status === 'error') return `error: ${state.error.message}`
  return state.status
}

export interface Expectation {
  query: LiveQuery<unknown>
  rows: readonly unknown[]
  label: string
}

const MISMATCH_ROWS_SHOWN = 6

function describeMismatch(wanted: string[], held: string[] | string): string {
  if (!Array.isArray(held)) return `wanted ${wanted.length} rows, and the query is ${held}`
  let first = 0
  while (first < wanted.length && wanted[first] === held[first]) first++
  const window = (rows: string[]) => rows.slice(first, first + MISMATCH_ROWS_SHOWN).join(' ')
  return `wanted ${wanted.length} rows and the query holds ${held.length}; from row ${first}\nwanted ${window(wanted)}\nheld   ${window(held)}`
}

function mismatch(expectation: Expectation, wanted: string, problem: string): Error {
  return new Error(
    `${expectation.label}: ${problem}.\n${describeMismatch(JSON.parse(wanted) as string[], rowsOrStatus(expectation.query))}`,
  )
}

export async function allSettle(expectations: readonly Expectation[], quietMs = QUIET_PERIOD_MS): Promise<void> {
  const wanted = expectations.map(expectation => JSON.stringify(canonicalRows(expectation.rows)))
  const firstMismatch = (): number =>
    expectations.findIndex((expectation, index) => {
      const seen = rowsOrStatus(expectation.query)
      return !Array.isArray(seen) || JSON.stringify(seen) !== wanted[index]
    })
  const deadline = Date.now() + WAIT_LIMIT_MS
  for (let failing = firstMismatch(); failing !== -1; failing = firstMismatch()) {
    if (Date.now() > deadline) throw mismatch(expectations[failing], wanted[failing], 'the live query never matched')
    await pause(WAIT_STEP_MS)
  }
  if (quietMs === 0) return
  await pause(quietMs)
  const drifted = firstMismatch()
  if (drifted !== -1) throw mismatch(expectations[drifted], wanted[drifted], 'the live query moved away from it')
}

export async function settlesTo<T>(
  query: LiveQuery<T>,
  expected: readonly unknown[],
  label: string,
): Promise<readonly T[]> {
  await allSettle([{ query: query as LiveQuery<unknown>, rows: expected, label }])
  return readyRows(query)
}

export interface QueryCase {
  query: LiveQuery<unknown>
  sql: string
  params?: Params
}

export async function settleToQueries(db: Database, cases: readonly QueryCase[], quietMs?: number): Promise<void> {
  const expectations: Expectation[] = []
  for (const entry of cases) {
    expectations.push({ query: entry.query, rows: await db.query(entry.sql, entry.params), label: entry.sql })
  }
  await allSettle(expectations, quietMs)
}

export async function settlesToQuery<T>(
  query: LiveQuery<T>,
  db: Database,
  sql: string,
  params?: Params,
): Promise<readonly T[]> {
  return settlesTo(query, await db.query(sql, params), sql)
}

export function readRaw<T = Record<string, unknown>>(path: string, sql: string, params: unknown[] = []): T[] {
  const raw = new BetterSqlite3(path, { readonly: true, fileMustExist: true })
  try {
    raw.defaultSafeIntegers(true)
    return raw.prepare(sql).all(...params) as T[]
  } finally {
    raw.close()
  }
}

export interface Replica<T> {
  rows: T[]
  kinds: LiveUpdate<T>['kind'][]
  published: number[]
  stop(): void
}

function applyOp<T>(rows: T[], op: ResultOp<T>): void {
  if (op.op === 'insert') rows.splice(op.index, 0, op.row)
  else if (op.op === 'update') rows[op.index] = op.row
  else rows.splice(op.index, 1)
}

export function replicate<T>(query: LiveQuery<T>): Replica<T> {
  const replica: Replica<T> = { rows: [], kinds: [], published: [], stop: () => {} }
  const state = query.getState()
  if (state.status === 'ready') replica.rows = state.rows.slice()
  replica.stop = query.subscribe(update => {
    replica.kinds.push(update.kind)
    const current = query.getState()
    if (update.kind === 'ops') for (const op of update.ops) applyOp(replica.rows, op)
    if (update.kind === 'rows' && current.status === 'ready') replica.rows = current.rows.slice()
    if (current.status === 'ready' && !current.revalidating) replica.published.push(current.rows.length)
  })
  return replica
}

export interface LiveStatementLog {
  metrics: { onQueryComplete(metrics: QueryMetrics): void }
  returned(): number[]
}

export function logLiveStatements(): LiveStatementLog {
  const reported: QueryMetrics[] = []
  return {
    metrics: {
      onQueryComplete: metrics => {
        if (metrics.source === 'live') reported.push(metrics)
      },
    },
    returned: () => reported.map(metrics => metrics.rowsReturned ?? -1),
  }
}

export interface RejectionWatch {
  reasons: unknown[]
  stop(): void
}

export function watchUnhandledRejections(): RejectionWatch {
  const reasons: unknown[] = []
  const record = (reason: unknown) => reasons.push(reason)
  process.on('unhandledRejection', record)
  return { reasons, stop: () => process.off('unhandledRejection', record) }
}

export async function settleCallbacks(): Promise<void> {
  for (let turn = 0; turn < 3; turn++) await new Promise(resolve => setImmediate(resolve))
}
