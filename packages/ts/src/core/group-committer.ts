import type { GroupRunError, SQLiteConnection } from './driver/types.js'
import { bindParams, execute, executeGroup, type GroupOutcome, type GroupStatement } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'
import type { WriterLock } from './writer-lock.js'

export const DEFAULT_MAX_GROUP_SIZE = 1000

const DML_PREFIX_RE = /^\s*(?:INSERT|UPDATE|DELETE|REPLACE)\b/i

/**
 * Only plain DML is folded into a shared transaction; DDL, PRAGMA, and anything
 * else runs alone exactly as before, so transaction-hostile statements and CDC
 * trigger reinstatement keep their existing one-statement-at-a-time behaviour.
 */
function isGroupable(sql: string): boolean {
  return DML_PREFIX_RE.test(sql)
}

/** A caller that groups a transaction this rejects loses the CDC trigger reinstatement DDL needs. */
export function canGroupTransaction(statements: readonly GroupStatement[]): boolean {
  for (const statement of statements) {
    if (!isGroupable(statement.sql)) return false
  }
  return true
}

interface Job {
  statements: readonly GroupStatement[]
  resolve: (values: ExecuteResult[]) => void
  reject: (error: unknown) => void
}

interface GroupCommitterHooks {
  acquireWriter: () => SQLiteConnection
  afterCommit: (writer: SQLiteConnection, sql: string) => Promise<void>
}

/**
 * Coalesces the writes waiting on the writer into one transaction so a single
 * commit fsync covers the whole group. A unit is one lone write or one whole
 * transaction, and the group treats both the same way. Work that arrives while a
 * group is committing forms the next group, so the accumulation window is the
 * commit's own duration and no artificial delay is needed. Each unit still
 * resolves with its own results or rejects with its own error, and only after
 * the group's commit, so durability is unchanged.
 */
export class GroupCommitter {
  private readonly pending: Job[] = []
  private running = false
  private scheduled = false
  private loopPromise: Promise<void> = Promise.resolve()

  constructor(
    private readonly writerLock: WriterLock,
    private readonly hooks: GroupCommitterHooks,
    private readonly maxGroup: number = DEFAULT_MAX_GROUP_SIZE,
  ) {}

  submit(sql: string, params?: Params): Promise<ExecuteResult> {
    return new Promise<ExecuteResult>((resolve, reject) => {
      this.pending.push({
        statements: [{ sql, params }],
        resolve: values => resolve(values[0]),
        reject,
      })
      this.schedule()
    })
  }

  submitTransaction(statements: readonly GroupStatement[]): Promise<ExecuteResult[]> {
    return new Promise<ExecuteResult[]>((resolve, reject) => {
      this.pending.push({ statements, resolve, reject })
      this.schedule()
    })
  }

  async drain(): Promise<void> {
    while (this.running || this.pending.length > 0) {
      if (!this.running) this.schedule()
      await this.loopPromise
    }
  }

  private schedule(): void {
    if (this.running || this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => {
      this.scheduled = false
      this.runLoop()
    })
  }

  private runLoop(): void {
    if (this.running) return
    this.running = true
    this.loopPromise = (async () => {
      try {
        while (this.pending.length > 0) {
          await this.flush(this.takeBatch())
        }
      } finally {
        this.running = false
        if (this.pending.length > 0) this.schedule()
      }
    })()
  }

  private takeBatch(): Job[] {
    const pending = this.pending
    if (!canGroupTransaction(pending[0].statements)) return pending.splice(0, 1)
    let n = 1
    let statements = pending[0].statements.length
    while (n < pending.length && canGroupTransaction(pending[n].statements)) {
      const size = pending[n].statements.length
      if (statements + size > this.maxGroup) break
      statements += size
      n++
    }
    return pending.splice(0, n)
  }

  private async flush(batch: Job[]): Promise<void> {
    const lone = batch[0]
    if (batch.length === 1 && lone.statements.length === 1) {
      try {
        lone.resolve([await this.writerLock.run(() => this.runSingle(lone.statements[0]))])
      } catch (err) {
        lone.reject(err)
      }
      return
    }

    let outcomes: GroupOutcome[]
    try {
      outcomes = await this.writerLock.run(() => this.runGroup(batch))
    } catch (err) {
      for (const job of batch) job.reject(err)
      return
    }
    for (let i = 0; i < batch.length; i++) {
      const outcome = outcomes[i]
      if (outcome.ok) batch[i].resolve(outcome.values)
      else batch[i].reject(outcome.error)
    }
  }

  private async runSingle(statement: GroupStatement): Promise<ExecuteResult> {
    const writer = this.hooks.acquireWriter()
    const result = await execute(writer, statement.sql, statement.params)
    await this.hooks.afterCommit(writer, statement.sql)
    return result
  }

  private async runGroup(batch: Job[]): Promise<GroupOutcome[]> {
    const writer = this.hooks.acquireWriter()
    const units = batch.map(job => ({
      statements: job.statements.map(statement => ({ sql: statement.sql, params: bindParams(statement.params) })),
    }))
    if (writer.runGroup) {
      const raw = await writer.runGroup(units)
      return raw.map(outcome =>
        outcome.ok ? { ok: true, values: outcome.results } : { ok: false, error: toError(outcome.error) },
      )
    }
    return executeGroup(writer, units)
  }
}

function toError(error: GroupRunError): Error {
  const err = new Error(error.message)
  if (error.name) err.name = error.name
  if (error.code) (err as { code?: string }).code = error.code
  return err
}
