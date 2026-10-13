import type { DatabaseCdcController } from '../database-cdc.js'
import type { DatabaseObserver } from '../database-observability.js'
import { allMetered } from '../driver/row-counts.js'
import type { SQLiteConnection } from '../driver/types.js'
import { CDCError } from '../errors.js'
import { LIVE_PROBE_TABLE_PREFIX } from '../internal-tables.js'
import { readOutcome } from '../metrics/outcomes.js'
import { asQueryError } from '../query-executor.js'
import { selectTableSql, tableColumnsWithGenerated, tableIndexList } from '../system-catalog/index.js'
import type { ChangeEvent, Params } from '../types.js'
import { type ColumnDefinition, parseColumnDefinitions } from './column-definitions.js'
import type { LiveQuerySource, PositionedRead } from './live-query.js'
import { MaintainedLiveQuery } from './live-query.js'
import type { HeldRow } from './live-result.js'
import { PrimedLiveDelivery } from './primed-delivery.js'
import { buildLivePlan, type LivePlan } from './query-plan.js'
import { encodeRowKey, rowidKey } from './row-keys.js'
import { type LiveReadMeter, RowProbe } from './row-probe.js'
import { toSortValue } from './sqlite-order.js'
import { analyseStatement, type StatementShape } from './statement-shape.js'
import type { LiveQuery, LiveQueryOptions } from './types.js'

const DEFAULT_REREAD_JITTER_MS = 25
const DEFAULT_MAX_TRANSACTION_CHANGES = 10_000
const MAX_TRANSACTION_BYTES = 16 * 1_048_576
const SQLITE_HIDDEN_VIRTUAL_TABLE_COLUMN = 1
const PRIMARY_KEY_INDEX_ORIGIN = 'pk'

let nextProbeTable = 0

export interface DatabaseLiveDeps {
  cdc: DatabaseCdcController
  observer: DatabaseObserver
  watch(table: string): Promise<void>
}

interface LiveConnection {
  conn: SQLiteConnection
  run: <R>(operation: () => Promise<R>) => Promise<R>
}

interface PlannedProbe {
  plan: LivePlan
  probe: RowProbe
  columns: ReadonlySet<string>
}

export async function openLiveQuery<T>(
  deps: DatabaseLiveDeps,
  sql: string,
  params?: Params,
  options?: LiveQueryOptions,
): Promise<LiveQuery<T>> {
  const shape = analyseStatement(sql)
  try {
    const live = await deps.cdc.liveConnection()
    const meter: LiveReadMeter = read => deps.observer.track(sql, read, readOutcome, undefined, 'live')
    const planned = await planProbe(live, shape, params, meter)
    const source = buildSource<T>(deps, live, shape, params, options, meter, planned)
    try {
      await deps.watch(shape.table)
      return await MaintainedLiveQuery.open<T>(source)
    } catch (err) {
      await source.release().catch(() => {})
      throw err
    }
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

async function planProbe(
  live: LiveConnection,
  shape: StatementShape,
  params: Params | undefined,
  meter: LiveReadMeter,
): Promise<PlannedProbe> {
  const { conn, run } = live
  const info = (await run(() => tableColumnsWithGenerated(conn, shape.table))).filter(
    column => column.hidden !== SQLITE_HIDDEN_VIRTUAL_TABLE_COLUMN,
  )
  if (info.length === 0) {
    throw new CDCError(`Cannot open a live query for '${shape.sql}': table '${shape.table}' has no columns`)
  }
  const ddl = await run(() => selectTableSql(conn, shape.table))
  const definitions = ddl === null ? new Map<string, ColumnDefinition>() : parseColumnDefinitions(ddl)
  const collations = new Map<string, string>()
  for (const [name, definition] of definitions) {
    if (definition.collation !== null) collations.set(name, definition.collation)
  }

  const pkColumns = info
    .filter(column => column.pk > 0)
    .sort((left, right) => left.pk - right.pk)
    .map(column => column.name)
  const indexes = await run(() => tableIndexList(conn, shape.table))
  const pkIsRowid = pkColumns.length === 1 && !indexes.some(index => index.origin === PRIMARY_KEY_INDEX_ORIGIN)

  const plan = buildLivePlan(
    shape,
    {
      columns: info.map(column => ({
        name: column.name,
        type: column.type,
        generatedAs: column.hidden === 0 ? null : generatedExpression(shape, column.name, definitions),
      })),
      collations,
      pkColumns,
      rowidAlias: pkIsRowid ? pkColumns[0] : null,
    },
    params,
  )

  const probeTable = `${LIVE_PROBE_TABLE_PREFIX}${nextProbeTable++}`
  const probe = await RowProbe.open(conn, probeTable, plan, params, run, meter)
  const captured = info.filter(column => column.hidden === 0).map(column => column.name)
  return { plan, probe, columns: new Set(captured) }
}

function generatedExpression(
  shape: StatementShape,
  column: string,
  definitions: ReadonlyMap<string, ColumnDefinition>,
): string {
  const expression = definitions.get(column)?.generatedAs
  if (expression === null || expression === undefined) {
    throw new CDCError(
      `Cannot open a live query for '${shape.sql}': the expression of generated column '${column}' could not be read from the table definition`,
    )
  }
  return expression
}

function buildSource<T>(
  deps: DatabaseLiveDeps,
  live: LiveConnection,
  shape: StatementShape,
  params: Params | undefined,
  options: LiveQueryOptions | undefined,
  meter: LiveReadMeter,
  initial: PlannedProbe,
): LiveQuerySource<T> {
  const delivery = new PrimedLiveDelivery(deps.cdc, live.conn, live.run, shape.table)
  let planned = initial

  return {
    get plan() {
      return planned.plan
    },
    get probe() {
      return planned.probe
    },
    rereadJitterMs: options?.rereadJitterMs ?? DEFAULT_REREAD_JITTER_MS,
    maxTransactionChanges: options?.maxTransactionChanges ?? DEFAULT_MAX_TRANSACTION_CHANGES,
    maxTransactionBytes: MAX_TRANSACTION_BYTES,
    onError: options?.onError,
    wait: ms => new Promise<void>(resolve => setTimeout(resolve, ms)),
    read: () => readAtPosition<T>(deps.cdc, planned.plan, params, meter),
    start: (handlers, sinceSeq) => delivery.start(handlers, sinceSeq),
    shapeChanged: events => events.some(event => !capturedWithColumns(event, planned.columns)),
    async replan() {
      const replaced = planned
      try {
        planned = await planProbe(live, shape, params, meter)
      } catch (err) {
        throw asQueryError(err, shape.sql)
      }
      await replaced.probe.close()
    },
    release: () => planned.probe.close(),
  }
}

function capturedWithColumns(event: ChangeEvent, columns: ReadonlySet<string>): boolean {
  if (event.type !== 'delete' && !payloadHasColumns(event.row, columns)) return false
  return event.oldRow === undefined || payloadHasColumns(event.oldRow, columns)
}

function payloadHasColumns(payload: Record<string, unknown>, columns: ReadonlySet<string>): boolean {
  let count = 0
  for (const column in payload) {
    if (!columns.has(column)) return false
    count++
  }
  return count === columns.size
}

async function readAtPosition<T>(
  cdc: DatabaseCdcController,
  plan: LivePlan,
  params: Params | undefined,
  meter: LiveReadMeter,
): Promise<PositionedRead<T>> {
  try {
    const captured = await cdc.readAtPositionWith(async conn => {
      const stmt = await conn.prepare(plan.readSql)
      return (await meter(() => allMetered<Record<string, unknown>>(stmt, bindRead(params)))).value
    })
    return { rows: captured.value.map(row => toHeldRow<T>(plan, row)), seq: captured.seq }
  } catch (err) {
    throw asQueryError(err, plan.shape.sql)
  }
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
