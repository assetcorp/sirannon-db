import { type RowCounts, rowCountsOf, withRowCounts } from './driver/row-counts.js'
import type { GroupRunError, GroupRunOutcome, SQLiteConnection } from './driver/types.js'
import { QueryError, SirannonError, TransactionError } from './errors.js'
import { executeGroup, type GroupOutcome, type GroupStatement } from './group-executor.js'
import { bindParams, execute } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'
import type { WriterLock } from './writer-lock.js'

export const DEFAULT_MAX_GROUP_SIZE = 1000

const DML_PREFIX_RE = /^\s*(?:INSERT|UPDATE|DELETE|REPLACE)\b/i

function isGroupable(sql: string): boolean {
  return DML_PREFIX_RE.test(sql)
}

export function canGroupTransaction(statements: readonly GroupStatement[]): boolean {
  for (const statement of statements) {
    if (!isGroupable(statement.sql)) return false
  }
  return true
}

export interface FailedWriteRowCounts {
  statements?: RowCounts[]
}

interface Job {
  statements: readonly GroupStatement[]
  resolve: (values: ExecuteResult[]) => void
  reject: (error: unknown) => void
  failedRowCounts?: FailedWriteRowCounts
}

interface GroupCommitterHooks {
  acquireWriter: () => SQLiteConnection
  afterCommit: (writer: SQLiteConnection, sql: string) => Promise<void>
  runUngroupable: (writer: SQLiteConnection, sql: string, run: () => Promise<ExecuteResult>) => Promise<ExecuteResult>
  stampStatements: (options?: { persistClock?: boolean }) => readonly GroupStatement[] | null
  stampOpening: () => GroupStatement | null
}

function rejectWithRowCounts(job: Job, error: unknown, statements: RowCounts[] | undefined): void {
  if (job.failedRowCounts && statements) job.failedRowCounts.statements = statements.slice(0, job.statements.length)
  job.reject(error)
}

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

  submit(sql: string, params?: Params, failedRowCounts?: FailedWriteRowCounts): Promise<ExecuteResult> {
    return new Promise<ExecuteResult>((resolve, reject) => {
      this.pending.push({
        statements: [{ sql, params }],
        resolve: values => resolve(values[0]),
        reject,
        failedRowCounts,
      })
      this.schedule()
    })
  }

  submitTransaction(
    statements: readonly GroupStatement[],
    failedRowCounts?: FailedWriteRowCounts,
  ): Promise<ExecuteResult[]> {
    return new Promise<ExecuteResult[]>((resolve, reject) => {
      this.pending.push({ statements, resolve, reject, failedRowCounts })
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
        const counts = rowCountsOf(err)
        rejectWithRowCounts(lone, err, counts ? [counts] : undefined)
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
      else rejectWithRowCounts(batch[i], outcome.error, outcome.statementRowCounts)
    }
  }

  private needsStamp(sql: string): boolean {
    if (!isGroupable(sql)) return false
    const stamps = this.hooks.stampStatements()
    return stamps !== null && stamps.length > 0
  }

  private async runSingle(statement: GroupStatement): Promise<ExecuteResult> {
    const writer = this.hooks.acquireWriter()
    const groupable = isGroupable(statement.sql)
    const stamps = groupable ? this.hooks.stampStatements() : null
    const result = !groupable
      ? await this.hooks.runUngroupable(writer, statement.sql, () => execute(writer, statement.sql, statement.params))
      : stamps === null || stamps.length === 0
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
    let result: ExecuteResult | undefined
    try {
      result = await execute(writer, statement.sql, statement.params)
      for (const stamp of stamps) {
        await execute(writer, stamp.sql, stamp.params, stamp.trusted === true)
      }
      await writer.exec('RELEASE sirannon_stamp')
      return result
    } catch (err) {
      try {
        await writer.exec('ROLLBACK TO sirannon_stamp')
        await writer.exec('RELEASE sirannon_stamp')
      } catch {}
      throw result === undefined ? err : withRowCounts(err, rowCountsOf(result))
    }
  }

  private async runGroup(batch: Job[]): Promise<GroupOutcome[]> {
    const writer = this.hooks.acquireWriter()
    const opening = this.hooks.stampOpening()
    const leading = opening === null ? [] : [opening]
    const units = batch.map((job, index) => {
      const stamps = this.hooks.stampStatements({ persistClock: index === batch.length - 1 }) ?? []
      return {
        statements: [...leading, ...job.statements, ...stamps].map(statement => ({
          sql: statement.sql,
          params: bindParams(statement.params),
          ...(statement.trusted === true ? { trusted: true } : {}),
        })),
      }
    })
    const outcomes = writer.runGroup ? fromRawOutcomes(await writer.runGroup(units)) : await executeGroup(writer, units)
    return opening === null ? outcomes : outcomes.map(withoutLeadingStatement)
  }
}

function fromRawOutcomes(raw: readonly GroupRunOutcome[]): GroupOutcome[] {
  return raw.map<GroupOutcome>(outcome => {
    if (outcome.ok) return { ok: true, values: outcome.results }
    const error = toError(outcome.error)
    return outcome.statementRowCounts
      ? { ok: false, error, statementRowCounts: outcome.statementRowCounts }
      : { ok: false, error }
  })
}

function withoutLeadingStatement(outcome: GroupOutcome): GroupOutcome {
  if (outcome.ok) return { ok: true, values: outcome.values.slice(1) }
  return outcome.statementRowCounts
    ? { ok: false, error: outcome.error, statementRowCounts: outcome.statementRowCounts.slice(1) }
    : outcome
}

function toError(error: GroupRunError): Error {
  return withRowCounts(rebuildError(error), rowCountsOf(error))
}

function rebuildError(error: GroupRunError): Error {
  if (error.code === 'QUERY_ERROR' && error.sql !== undefined) return new QueryError(error.message, error.sql)
  if (error.code === 'TRANSACTION_ERROR') return new TransactionError(error.message)
  const err = error.code ? new SirannonError(error.message, error.code) : new Error(error.message)
  if (error.name) err.name = error.name
  return err
}
