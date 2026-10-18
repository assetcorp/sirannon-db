import { type RowCounts, rowCountsOf, withRowCounts } from './driver/row-counts.js'
import type { GroupRunError, GroupRunOutcome, SQLiteConnection } from './driver/types.js'
import { QueryError, SirannonError, TransactionError } from './errors.js'
import { executeGroup, type GroupOutcome, type GroupStatement } from './group-executor.js'
import {
  type LateGroupOutcomes,
  lateGroupOutcomesOf,
  withLateRowCounts,
  withoutLateRowCounts,
} from './metrics/late-row-counts.js'
import { bindParams, execute } from './query-executor.js'
import { runsInsideTransaction } from './sql-statement-start.js'
import type { ExecuteResult, Params } from './types.js'
import type { WriterLock } from './writer-lock.js'

export const DEFAULT_MAX_GROUP_SIZE = 1000

const STAMPED_WRITE_SAVEPOINT = 'sirannon_stamp'

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
  stamps: () => boolean
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
    return this.writerLock.run(() => this.runSingle({ sql, params }, true))
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
    if (batch.length === 1 && lone.statements.length === 1 && !this.groupsToStamp(lone.statements[0].sql)) {
      try {
        lone.resolve([await this.writerLock.run(() => this.runSingle(lone.statements[0], false))])
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
      const late = lateGroupOutcomesOf(err)
      batch.forEach((job, index) => {
        job.reject(late === undefined ? err : unitFailure(err, late, index, job.statements.length))
      })
      return
    }
    for (let i = 0; i < batch.length; i++) {
      const outcome = outcomes[i]
      if (outcome.ok) batch[i].resolve(outcome.values.slice(0, batch[i].statements.length))
      else rejectWithRowCounts(batch[i], outcome.error, outcome.statementRowCounts)
    }
  }

  private groupsToStamp(sql: string): boolean {
    return isGroupable(sql) && this.hooks.stamps()
  }

  private async runSingle(statement: GroupStatement, joinsOpenTransaction: boolean): Promise<ExecuteResult> {
    const writer = this.hooks.acquireWriter()
    const run = () => execute(writer, statement.sql, statement.params)
    const write = isGroupable(statement.sql) ? run : () => this.hooks.runUngroupable(writer, statement.sql, run)
    const stamped = !joinsOpenTransaction && this.hooks.stamps() && runsInsideTransaction(statement.sql)
    const opening = stamped ? this.hooks.stampOpening() : null
    const result = opening !== null ? await this.runStampedAlone(writer, opening, write) : await write()
    await this.hooks.afterCommit(writer, statement.sql)
    return result
  }

  private async runStampedAlone(
    writer: SQLiteConnection,
    opening: GroupStatement,
    write: () => Promise<ExecuteResult>,
  ): Promise<ExecuteResult> {
    await writer.exec(`SAVEPOINT ${STAMPED_WRITE_SAVEPOINT}`)
    let result: ExecuteResult | undefined
    let writing = false
    try {
      await execute(writer, opening.sql, opening.params, true)
      writing = true
      result = await write()
      writing = false
      for (const stamp of this.hooks.stampStatements() ?? []) {
        await execute(writer, stamp.sql, stamp.params, stamp.trusted === true)
      }
      await writer.exec(`RELEASE ${STAMPED_WRITE_SAVEPOINT}`)
      return result
    } catch (err) {
      try {
        await writer.exec(`ROLLBACK TO ${STAMPED_WRITE_SAVEPOINT}`)
        await writer.exec(`RELEASE ${STAMPED_WRITE_SAVEPOINT}`)
      } catch {}
      if (!writing) withoutLateRowCounts(err)
      throw result === undefined ? err : withRowCounts(err, rowCountsOf(result))
    }
  }

  private async runGroup(batch: Job[]): Promise<GroupOutcome[]> {
    const writer = this.hooks.acquireWriter()
    const stampOpening = this.hooks.stampOpening()
    const opening = stampOpening === null ? undefined : boundStatement(stampOpening)
    const units = batch.map((job, index) => {
      const stamps = this.hooks.stampStatements({ persistClock: index === batch.length - 1 }) ?? []
      return { statements: [...job.statements, ...stamps].map(boundStatement) }
    })
    return writer.runGroup
      ? fromRawOutcomes(await writer.runGroup(units, opening))
      : await executeGroup(writer, units, opening)
  }
}

function unitCounts(outcome: GroupRunOutcome | undefined, statements: number): (RowCounts | undefined)[] {
  if (outcome === undefined) return []
  if (outcome.ok) return outcome.results.slice(0, statements).map(rowCountsOf)
  return outcome.statementRowCounts?.slice(0, statements) ?? [rowCountsOf(outcome.error)]
}

function unitFailure(err: unknown, late: LateGroupOutcomes, index: number, statements: number): Error {
  const source = err instanceof Error ? err : new Error(String(err))
  const copy =
    source instanceof SirannonError ? new SirannonError(source.message, source.code) : new Error(source.message)
  copy.name = source.name
  return withLateRowCounts(
    copy,
    late.then(outcomes => (outcomes === null ? null : unitCounts(outcomes[index], statements))),
  )
}

function boundStatement(statement: GroupStatement): GroupStatement & { params: unknown[] } {
  return {
    sql: statement.sql,
    params: bindParams(statement.params),
    ...(statement.trusted === true ? { trusted: true } : {}),
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
