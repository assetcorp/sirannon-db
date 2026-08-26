import type { DatabaseCdcController } from '../database-cdc.js'
import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { LIVE_PROBE_TABLE_PREFIX } from '../internal-tables.js'
import { selectTableSql, tableInfoRows } from '../system-catalog/index.js'
import type { Params } from '../types.js'
import { parseColumnCollations } from './column-collations.js'
import type { LiveQuerySource, PositionedRead } from './live-query.js'
import { MaintainedLiveQuery } from './live-query.js'
import type { HeldRow } from './live-result.js'
import { PrimedLiveDelivery } from './primed-delivery.js'
import { buildLivePlan, type LivePlan } from './query-plan.js'
import { encodeRowKey, rowidKey } from './row-keys.js'
import { RowProbe } from './row-probe.js'
import { toSortValue } from './sqlite-order.js'
import { analyseStatement } from './statement-shape.js'
import type { LiveQuery, LiveQueryOptions } from './types.js'

const DEFAULT_REREAD_JITTER_MS = 25
const DEFAULT_MAX_TRANSACTION_CHANGES = 10_000
const MAX_TRANSACTION_BYTES = 16 * 1_048_576

let nextProbeTable = 0

export interface DatabaseLiveDeps {
  cdc: DatabaseCdcController
  watch(table: string): Promise<void>
}

export async function openLiveQuery<T>(
  deps: DatabaseLiveDeps,
  sql: string,
  params?: Params,
  options?: LiveQueryOptions,
): Promise<LiveQuery<T>> {
  const shape = analyseStatement(sql)
  await deps.watch(shape.table)

  const { conn, run } = await deps.cdc.liveConnection()
  const info = await run(() => tableInfoRows(conn, shape.table))
  if (info.length === 0) {
    throw new CDCError(`Cannot open a live query for '${sql}': table '${shape.table}' has no columns`)
  }
  const ddl = await run(() => selectTableSql(conn, shape.table))

  const plan = buildLivePlan(
    shape,
    {
      columns: info.map(column => ({ name: column.name, type: column.type })),
      collations: ddl === null ? new Map() : parseColumnCollations(ddl),
      pkColumns: info
        .filter(column => column.pk > 0)
        .sort((left, right) => left.pk - right.pk)
        .map(column => column.name),
    },
    params,
  )

  const probeTable = `${LIVE_PROBE_TABLE_PREFIX}${nextProbeTable++}`
  const probe = await RowProbe.open(conn, probeTable, plan, params, run)

  try {
    return await MaintainedLiveQuery.open<T>(buildSource<T>(deps, conn, run, plan, probe, params, options))
  } catch (err) {
    await probe.close().catch(() => {})
    throw err
  }
}

function buildSource<T>(
  deps: DatabaseLiveDeps,
  conn: SQLiteConnection,
  run: <R>(operation: () => Promise<R>) => Promise<R>,
  plan: LivePlan,
  probe: RowProbe,
  params: Params | undefined,
  options: LiveQueryOptions | undefined,
): LiveQuerySource<T> {
  const delivery = new PrimedLiveDelivery(deps.cdc, conn, run, plan.shape.table)

  return {
    plan,
    probe,
    rereadJitterMs: options?.rereadJitterMs ?? DEFAULT_REREAD_JITTER_MS,
    maxTransactionChanges: options?.maxTransactionChanges ?? DEFAULT_MAX_TRANSACTION_CHANGES,
    maxTransactionBytes: MAX_TRANSACTION_BYTES,
    onError: options?.onError,
    wait: ms => new Promise<void>(resolve => setTimeout(resolve, ms)),
    read: () => readAtPosition<T>(deps.cdc, plan, params),
    start: (handlers, sinceSeq) => delivery.start(handlers, sinceSeq),
    release: () => probe.close(),
  }
}

async function readAtPosition<T>(
  cdc: DatabaseCdcController,
  plan: LivePlan,
  params: Params | undefined,
): Promise<PositionedRead<T>> {
  const captured = await cdc.readAtPositionWith(async conn => {
    const stmt = await conn.prepare(plan.readSql)
    return stmt.all<Record<string, unknown>>(...bindRead(params))
  })
  return { rows: captured.value.map(row => toHeldRow<T>(plan, row)), seq: captured.seq }
}

function bindRead(params: Params | undefined): unknown[] {
  if (params === undefined) return []
  return Array.isArray(params) ? params : [params]
}

function toHeldRow<T>(plan: LivePlan, row: Record<string, unknown>): HeldRow<T> {
  const key = plan.usesRowid
    ? rowidKey(row[plan.keyColumnAliases[0]])
    : encodeRowKey(plan.keyColumnAliases.map(alias => row[alias]))
  const sort = plan.sortColumns.map((column, index) => toSortValue(row[column], plan.sortPlan.collations[index]))

  for (const alias of plan.keyColumnAliases) delete row[alias]
  for (const column of plan.sortColumns) delete row[column]

  return { key, row: row as T, sort }
}
