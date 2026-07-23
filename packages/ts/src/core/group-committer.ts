import type { GroupRunError, SQLiteConnection } from './driver/types.js'
import { SirannonError } from './errors.js'
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
  stampStatements: (options?: { persistClock?: boolean }) => readonly GroupStatement[] | null
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

  runUngrouped(sql: string, params?: Params): Promise<ExecuteResult> {
    return this.writerLock.run(() => this.runSingle({ sql, params }))
  }

  private schedule(): void {
    if (this.running || this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => {
      this.scheduled = false
      this.writerLock.detached(() => this.runLoop())
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
    if (batch.length === 1 && lone.statements.length === 1 && !this.needsStamp(lone.statements[0].sql)) {
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
      if (outcome.ok) batch[i].resolve(outcome.values.slice(0, batch[i].statements.length))
      else batch[i].reject(outcome.error)
    }
  }

  private needsStamp(sql: string): boolean {
    if (!isGroupable(sql)) return false
    const stamps = this.hooks.stampStatements()
    return stamps !== null && stamps.length > 0
  }

  private async runSingle(statement: GroupStatement): Promise<ExecuteResult> {
    const writer = this.hooks.acquireWriter()
    const stamps = isGroupable(statement.sql) ? this.hooks.stampStatements() : null
    const result =
      stamps === null || stamps.length === 0
        ? await execute(writer, statement.sql, statement.params)
        : await this.runStampedAlone(writer, statement, stamps)
    await this.hooks.afterCommit(writer, statement.sql)
    return result
  }

  private async runStampedAlone(
    writer: SQLiteConnection,
    statement: GroupStatement,
    stamps: readonly GroupStatement[],
  ): Promise<ExecuteResult> {
    await writer.exec('SAVEPOINT sirannon_stamp')
    try {
      const result = await execute(writer, statement.sql, statement.params)
      for (const stamp of stamps) {
        await execute(writer, stamp.sql, stamp.params, stamp.trusted === true)
      }
      await writer.exec('RELEASE sirannon_stamp')
      return result
    } catch (err) {
      try {
        await writer.exec('ROLLBACK TO sirannon_stamp')
        await writer.exec('RELEASE sirannon_stamp')
      } catch {
        /* the outer error is the one to surface */
      }
      throw err
    }
  }

  private async runGroup(batch: Job[]): Promise<GroupOutcome[]> {
    const writer = this.hooks.acquireWriter()
    const units = batch.map((job, index) => {
      const stamps = this.hooks.stampStatements({ persistClock: index === batch.length - 1 }) ?? []
      return {
        statements: [...job.statements, ...stamps].map(statement => ({
          sql: statement.sql,
          params: bindParams(statement.params),
          ...(statement.trusted === true ? { trusted: true } : {}),
        })),
      }
    })
    if (writer.runGroup) {
      const raw = await writer.runGroup(units)
      return raw.map(outcome =>
        outcome.ok ? { ok: true, values: outcome.results } : { ok: false, error: toError(outcome.error) },
      )
    }
    return executeGroup(writer, units)
  }
}

/**
 * Every error a group produces is raised by this package and carries a code the
 * server maps to a status, so the code has to survive the worker's structured
 * clone as a {@link SirannonError} rather than as a bare `Error` the server can
 * only report as a 500.
 */
function toError(error: GroupRunError): Error {
  const err = error.code ? new SirannonError(error.message, error.code) : new Error(error.message)
  if (error.name) err.name = error.name
  return err
}
