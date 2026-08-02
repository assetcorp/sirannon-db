import type { LivePlan } from './query-plan.js'
import type { ProbeMatch } from './row-probe.js'
import { lowerBoundIndex, placementIndex, type SortValue } from './sqlite-order.js'
import type { ResultOp } from './types.js'

export interface HeldRow<T> {
  key: string
  row: T
  sort: SortValue[]
}

export interface RowChange {
  key: string
  before: ProbeMatch | null
  after: ProbeMatch | null
}

export class LiveResult<T> {
  private rows: HeldRow<T>[] = []
  private index = new Map<string, HeldRow<T>>()
  private windowTruncated = false

  constructor(private readonly plan: LivePlan) {}

  get size(): number {
    return this.rows.length
  }

  snapshot(): T[] {
    return this.rows.map(held => held.row)
  }

  reset(rows: HeldRow<T>[]): void {
    this.rows = rows
    this.index = new Map(rows.map(held => [held.key, held]))
    this.windowTruncated = this.plan.limit !== null && rows.length >= this.plan.limit
  }

  apply(changes: readonly RowChange[]): readonly ResultOp<T>[] | null {
    const draft = new Draft<T>(this.plan, this.rows.slice(), new Map(this.index), this.windowTruncated)

    for (const change of changes) {
      if (!draft.apply(change)) return null
    }
    if (!draft.windowIsWhole()) return null

    this.rows = draft.rows
    this.index = draft.index
    this.windowTruncated = draft.truncated
    return draft.ops
  }
}

class Draft<T> {
  readonly ops: ResultOp<T>[] = []

  constructor(
    private readonly plan: LivePlan,
    readonly rows: HeldRow<T>[],
    readonly index: Map<string, HeldRow<T>>,
    public truncated: boolean,
  ) {}

  apply(change: RowChange): boolean {
    const held = this.index.get(change.key)

    if (change.after === null) {
      if (change.before === null) return true
      return held === undefined ? this.plan.offset === 0 : this.remove(held)
    }

    const entering: HeldRow<T> = { key: change.key, row: change.after.row as T, sort: change.after.sort }

    if (held !== undefined) {
      if (this.plan.sortPlan.compare(held.sort, entering.sort) === 0) return this.replace(held, entering)
      if (!this.remove(held)) return false
    } else if (change.before !== null && this.plan.offset > 0) {
      return false
    }

    return this.insert(entering)
  }

  windowIsWhole(): boolean {
    if (this.plan.limit === null || !this.truncated) return true
    return this.rows.length >= this.plan.limit
  }

  private replace(held: HeldRow<T>, replacement: HeldRow<T>): boolean {
    const at = this.locate(held)
    if (at === -1) return false
    this.rows[at] = replacement
    this.index.set(replacement.key, replacement)
    this.ops.push({ op: 'update', index: at, row: replacement.row })
    return true
  }

  private remove(held: HeldRow<T>): boolean {
    if (this.plan.offset > 0) return false
    const at = this.locate(held)
    if (at === -1) return false
    this.rows.splice(at, 1)
    this.index.delete(held.key)
    this.ops.push({ op: 'delete', index: at })
    return true
  }

  private insert(held: HeldRow<T>): boolean {
    if (this.plan.offset > 0) {
      if (this.rows.length === 0) return false
      if (this.plan.sortPlan.compare(held.sort, this.rows[0].sort) <= 0) return false
    }

    const at = placementIndex(this.rows, held.sort, this.plan.sortPlan)
    if (this.plan.limit !== null && at >= this.plan.limit) {
      this.truncated = true
      return true
    }

    this.rows.splice(at, 0, held)
    this.index.set(held.key, held)
    this.ops.push({ op: 'insert', index: at, row: held.row })

    if (this.plan.limit !== null && this.rows.length > this.plan.limit) {
      const dropped = this.rows.pop()
      if (dropped !== undefined) this.index.delete(dropped.key)
      this.ops.push({ op: 'delete', index: this.rows.length })
      this.truncated = true
    }
    return true
  }

  private locate(held: HeldRow<T>): number {
    let at = lowerBoundIndex(this.rows, held.sort, this.plan.sortPlan)
    while (at < this.rows.length && this.rows[at].key !== held.key) {
      if (this.plan.sortPlan.compare(this.rows[at].sort, held.sort) !== 0) return -1
      at++
    }
    return at < this.rows.length ? at : -1
  }
}
