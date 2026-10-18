import { allMetered, type Metered } from '../driver/row-counts.js'
import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { asQueryError } from '../query-executor.js'
import {
  dropLiveProbeTable,
  ensureLiveProbeTable,
  type LiveProbeColumn,
  prepareDeleteLiveProbeRows,
  prepareInsertLiveProbeRow,
  storedLiveProbeColumns,
} from '../system-catalog/live-probe-table.js'
import type { Params } from '../types.js'
import { KEY_COLUMN, type LivePlan, probeNamedParams } from './query-plan.js'
import { type SortValue, toSortValue } from './sqlite-order.js'

export interface ProbeCandidate {
  slot: number
  payload: Record<string, unknown>
  rowid: unknown
}

export interface ProbeMatch {
  row: Record<string, unknown>
  sort: SortValue[]
}

export type LiveReadMeter = <T>(read: () => Promise<Metered<T[]>>) => Promise<Metered<T[]>>

function roundsWithDistinctRowids(candidates: readonly ProbeCandidate[]): ProbeCandidate[][] {
  const rounds: ProbeCandidate[][] = []
  const seen = new Map<string, number>()
  for (const candidate of candidates) {
    const key = String(candidate.rowid)
    const round = seen.get(key) ?? 0
    seen.set(key, round + 1)
    if (rounds[round] === undefined) rounds[round] = []
    rounds[round].push(candidate)
  }
  return rounds
}

export class RowProbe {
  private closed = false
  private readonly stored: readonly LiveProbeColumn[]

  private constructor(
    private readonly conn: SQLiteConnection,
    private readonly probeTable: string,
    private readonly plan: LivePlan,
    private readonly run: <R>(operation: () => Promise<R>) => Promise<R>,
    private readonly insert: SQLiteStatement,
    private readonly clear: SQLiteStatement,
    private readonly match: SQLiteStatement,
    private readonly matchParams: unknown[],
    private readonly meter: LiveReadMeter,
  ) {
    this.stored = storedLiveProbeColumns(plan.probeColumns)
  }

  static async open(
    conn: SQLiteConnection,
    probeTable: string,
    plan: LivePlan,
    params: Params | undefined,
    run: <R>(operation: () => Promise<R>) => Promise<R>,
    meter: LiveReadMeter,
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
        return new RowProbe(conn, probeTable, plan, run, insert, clear, match, matchParams, meter)
      })
    } catch (err) {
      await run(() => dropLiveProbeTable(conn, probeTable)).catch(() => {})
      throw err
    }
  }

  async evaluate(candidates: readonly ProbeCandidate[]): Promise<Map<number, ProbeMatch>> {
    const matches = new Map<number, ProbeMatch>()
    if (candidates.length === 0 || this.closed) return matches

    try {
      await this.run(async () => {
        if (!this.plan.namesRowid) {
          await this.evaluateBySlot(candidates, matches)
          return
        }
        for (const round of roundsWithDistinctRowids(candidates)) {
          await this.evaluateByRowid(round, matches)
        }
      })
    } catch (err) {
      throw asQueryError(err, this.plan.shape.sql)
    }

    return matches
  }

  private async evaluateBySlot(candidates: readonly ProbeCandidate[], matches: Map<number, ProbeMatch>): Promise<void> {
    try {
      for (const candidate of candidates) {
        await this.insert.run(candidate.slot, ...this.storedValues(candidate))
      }

      const matched = await this.meter(() => allMetered<Record<string, unknown>>(this.match, this.matchParams))
      for (const row of matched.value) {
        matches.set(Number(row[KEY_COLUMN]), this.take(row))
      }
    } finally {
      await this.clear.run()
    }
  }

  private async evaluateByRowid(round: readonly ProbeCandidate[], matches: Map<number, ProbeMatch>): Promise<void> {
    const slotByRowid = new Map<string, number>()
    try {
      for (const candidate of round) {
        slotByRowid.set(String(candidate.rowid), candidate.slot)
        await this.insert.run(candidate.rowid, ...this.storedValues(candidate))
      }

      const matched = await this.meter(() => allMetered<Record<string, unknown>>(this.match, this.matchParams))
      for (const row of matched.value) {
        const slot = slotByRowid.get(String(row[KEY_COLUMN]))
        if (slot !== undefined) matches.set(slot, this.take(row))
      }
    } finally {
      await this.clear.run()
    }
  }

  private storedValues(candidate: ProbeCandidate): unknown[] {
    return this.stored.map(column => candidate.payload[column.name] ?? null)
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
