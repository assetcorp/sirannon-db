import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import {
  dropLiveProbeTable,
  ensureLiveProbeTable,
  prepareDeleteLiveProbeRows,
  prepareInsertLiveProbeRow,
} from '../system-catalog/live-probe-table.js'
import type { Params } from '../types.js'
import { KEY_COLUMN, type LivePlan, probeNamedParams } from './query-plan.js'
import { type SortValue, toSortValue } from './sqlite-order.js'

export interface ProbeCandidate {
  slot: number
  payload: Record<string, unknown>
}

export interface ProbeMatch {
  row: Record<string, unknown>
  sort: SortValue[]
}

export class RowProbe {
  private closed = false

  private constructor(
    private readonly conn: SQLiteConnection,
    private readonly probeTable: string,
    private readonly plan: LivePlan,
    private readonly run: <R>(operation: () => Promise<R>) => Promise<R>,
    private readonly insert: SQLiteStatement,
    private readonly clear: SQLiteStatement,
    private readonly match: SQLiteStatement,
    private readonly matchParams: unknown[],
  ) {}

  static async open(
    conn: SQLiteConnection,
    probeTable: string,
    plan: LivePlan,
    params: Params | undefined,
    run: <R>(operation: () => Promise<R>) => Promise<R>,
  ): Promise<RowProbe> {
    const sql = plan.probeSql(probeTable)
    try {
      return await run(async () => {
        await ensureLiveProbeTable(conn, probeTable, plan.probeColumns)
        const insert = await prepareInsertLiveProbeRow(conn, probeTable, plan.probeColumns)
        const clear = await prepareDeleteLiveProbeRows(conn, probeTable)
        const match = await conn.prepare(sql)
        const named = probeNamedParams(sql, params)
        const matchParams = named === undefined || Array.isArray(named) ? plan.probeParams(params) : [named]
        return new RowProbe(conn, probeTable, plan, run, insert, clear, match, matchParams)
      })
    } catch (err) {
      await run(() => dropLiveProbeTable(conn, probeTable)).catch(() => {})
      throw err
    }
  }

  async evaluate(candidates: readonly ProbeCandidate[]): Promise<Map<number, ProbeMatch>> {
    const matches = new Map<number, ProbeMatch>()
    if (candidates.length === 0 || this.closed) return matches

    await this.run(async () => {
      try {
        for (const candidate of candidates) {
          const values = this.plan.probeColumns.map(column => candidate.payload[column.name] ?? null)
          await this.insert.run(candidate.slot, ...values)
        }

        const rows = await this.match.all<Record<string, unknown>>(...this.matchParams)
        for (const row of rows) {
          matches.set(Number(row[KEY_COLUMN]), this.take(row))
        }
      } finally {
        await this.clear.run()
      }
    })

    return matches
  }

  private take(row: Record<string, unknown>): ProbeMatch {
    const sort = this.plan.sortColumns.map((column, index) =>
      toSortValue(row[column], this.plan.sortPlan.collations[index]),
    )
    delete row[KEY_COLUMN]
    for (const column of this.plan.sortColumns) delete row[column]
    return { row, sort }
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    try {
      await this.run(() => dropLiveProbeTable(this.conn, this.probeTable))
    } catch {}
  }
}
