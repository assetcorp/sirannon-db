import { describe, expect, it } from 'vitest'
import type { LiveQuerySource, PositionedRead } from '../../live/live-query.js'
import { MaintainedLiveQuery } from '../../live/live-query.js'
import type { HeldRow } from '../../live/live-result.js'
import type { LivePlan } from '../../live/query-plan.js'
import type { ProbeCandidate, ProbeMatch, RowProbe } from '../../live/row-probe.js'
import { buildSortKeyPlan, toSortValue } from '../../live/sqlite-order.js'
import type { ChangeEvent } from '../../types.js'
import { readyRows } from './_helpers.js'

interface Row {
  id: number
}

const sortPlan = buildSortKeyPlan([{ expression: 'id', direction: 'asc', nulls: 'first', collation: null }])

function plan(limit: number | null = null, offset = 0): LivePlan {
  return {
    shape: { table: 'items' } as LivePlan['shape'],
    sourceName: 'items',
    probeColumns: [],
    keyColumns: ['id'],
    usesRowid: false,
    sortPlan,
    sortColumns: ['_sirannon_s0'],
    keyColumnAliases: ['_sirannon_k0'],
    readSql: 'SELECT 1',
    probeSql: () => 'SELECT 1',
    probeParams: () => [],
    limit,
    offset,
  }
}

function held(id: number): HeldRow<Row> {
  return { key: `2:n${id}`, row: { id }, sort: [toSortValue(id, 'binary')] }
}

function match(id: number): ProbeMatch {
  return { row: { id }, sort: [toSortValue(id, 'binary')] }
}

function insertEvent(id: number, seq: bigint, txId: string, txEnd?: boolean): ChangeEvent {
  return { type: 'insert', table: 'items', row: { id }, seq, timestamp: 0, txId, rowId: String(id), txEnd }
}

interface Harness {
  query: MaintainedLiveQuery<Row>
  deliver(event: ChangeEvent): void
  reads(): number
  probes(): number
  released(): number
  starts(): number
  lose(): void
}

async function openHarness(options?: {
  rows?: HeldRow<Row>[]
  livePlan?: LivePlan
  readFails?: () => boolean
  probeDelayMs?: number
}): Promise<Harness> {
  let reads = 0
  let probes = 0
  let released = 0
  let starts = 0
  let seq = 0n
  let onEvent: (event: ChangeEvent) => void = () => {}
  let onLost: () => void = () => {}

  const probe = {
    async evaluate(candidates: readonly ProbeCandidate[]): Promise<Map<number, ProbeMatch>> {
      probes++
      if (options?.probeDelayMs !== undefined) {
        await new Promise(resolve => setTimeout(resolve, options.probeDelayMs))
      }
      const result = new Map<number, ProbeMatch>()
      for (const candidate of candidates) {
        result.set(candidate.slot, match(Number(candidate.payload.id)))
      }
      return result
    },
  } as unknown as RowProbe

  const source: LiveQuerySource<Row> = {
    plan: options?.livePlan ?? plan(),
    probe,
    rereadJitterMs: 0,
    maxTransactionChanges: 1000,
    maxTransactionBytes: 1_000_000,
    wait: () => Promise.resolve(),
    async read(): Promise<PositionedRead<Row>> {
      reads++
      if (options?.readFails?.() === true) throw new Error('read failed')
      return { rows: (options?.rows ?? []).slice(), seq }
    },
    async start(handlers, sinceSeq) {
      starts++
      seq = sinceSeq
      onEvent = handlers.onEvent
      onLost = handlers.onLost
      return () => {
        onEvent = () => {}
      }
    },
    async release() {
      released++
    },
  }

  const query = await MaintainedLiveQuery.open<Row>(source)
  return {
    query,
    deliver: event => onEvent(event),
    reads: () => reads,
    probes: () => probes,
    released: () => released,
    starts: () => starts,
    lose: () => onLost(),
  }
}

async function settle(): Promise<void> {
  for (let i = 0; i < 10; i++) await new Promise(resolve => setTimeout(resolve, 1))
}

describe('MaintainedLiveQuery', () => {
  it('holds a transaction that arrives across two deliveries until its last change', async () => {
    const harness = await openHarness({ rows: [held(1), held(2), held(3)] })
    const sizes: number[] = []
    harness.query.subscribe(() => {
      const state = harness.query.getState()
      if (state.status === 'ready') sizes.push(state.rows.length)
    })

    harness.deliver(insertEvent(4, 1n, 'tx-1'))
    harness.deliver(insertEvent(5, 2n, 'tx-1'))
    await settle()
    expect(sizes).toEqual([])

    harness.deliver(insertEvent(6, 3n, 'tx-1', true))
    await settle()

    expect(sizes).toEqual([6])
    expect(readyRows(harness.query).map(row => row.id)).toEqual([1, 2, 3, 4, 5, 6])
    expect(harness.reads()).toBe(1)
    await harness.query.close()
  })

  it('publishes nothing after close while a change is still being evaluated', async () => {
    const harness = await openHarness({ rows: [held(1)], probeDelayMs: 20 })
    let notifications = 0
    harness.query.subscribe(() => notifications++)

    harness.deliver(insertEvent(2, 1n, 'tx-1', true))
    await new Promise(resolve => setTimeout(resolve, 2))
    await harness.query.close()
    await new Promise(resolve => setTimeout(resolve, 60))

    expect(notifications).toBe(0)
    expect(harness.released()).toBe(1)
  })

  it('reports a failed re-read instead of leaving stale rows in place', async () => {
    let fail = false
    const harness = await openHarness({ rows: [held(1), held(2)], readFails: () => fail })
    fail = true

    harness.deliver(insertEvent(3, 1n, 'tx-1'))
    harness.deliver(insertEvent(4, 2n, 'tx-1'))
    harness.deliver(insertEvent(5, 3n, 'tx-1', true))
    await settle()

    const state = harness.query.getState()
    expect(state.status).toBe('error')
    expect(state.status === 'error' && state.error.message).toBe('read failed')
    await harness.query.close()
  })

  it('starts delivery again after it is lost', async () => {
    const harness = await openHarness({ rows: [held(1)] })
    expect(harness.starts()).toBe(1)

    harness.lose()
    await settle()

    expect(harness.starts()).toBe(2)
    expect(harness.reads()).toBe(2)
    expect(harness.query.getState().status).toBe('ready')
    await harness.query.close()
  })

  it('releases its resources once, however many times it is closed', async () => {
    const harness = await openHarness({ rows: [] })
    await harness.query.close()
    await harness.query.close()
    expect(harness.released()).toBe(1)
  })

  it('applies each transaction separately when two arrive back to back', async () => {
    const harness = await openHarness({ rows: [held(1)] })
    const publications: number[][] = []
    harness.query.subscribe(() => {
      const state = harness.query.getState()
      if (state.status === 'ready') publications.push(state.rows.map(row => row.id))
    })

    harness.deliver(insertEvent(2, 1n, 'tx-1', true))
    harness.deliver(insertEvent(3, 2n, 'tx-2', true))
    await settle()

    expect(publications).toEqual([
      [1, 2],
      [1, 2, 3],
    ])
    expect(harness.reads()).toBe(1)
    await harness.query.close()
  })
})
