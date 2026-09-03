import { Worker } from 'node:worker_threads'
import type {
  DatabaseCopyStep,
  DriverWorkerEntry,
  GroupRunOutcome,
  OpenOptions,
  SQLiteConnection,
} from '../driver/types.js'
import { SirannonError } from '../errors.js'
import {
  deserializeError,
  WORKER_CANCELLED_CODE,
  WORKER_COPY_STOPPED_CODE,
  type WorkerRequest,
  type WorkerRequestBody,
  type WorkerResponse,
} from './protocol.js'
import { resolveWorkerScript } from './resolve-entry.js'

export const DEFAULT_WRITE_TIMEOUT_MS = 30_000
const DEFAULT_MAX_RESTARTS = 5
const CLOSE_GRACE_MS = 1_000

export interface WorkerHostOptions {
  writeTimeoutMs?: number
  maxRestarts?: number
}

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: NodeJS.Timeout | null
  graceTimer: NodeJS.Timeout | null
  deadlineMs: number
  cancellable: boolean
  kind: WorkerRequestBody['kind']
  onStep?: (step: DatabaseCopyStep) => void
}

type OpenRequest = Extract<WorkerRequest, { kind: 'open' }>

export class WriterWorker {
  private worker: Worker | null = null
  private readonly pending = new Map<number, PendingRequest>()
  private nextId = 1
  private ready: Promise<void> = Promise.resolve()
  private starting = true
  private closed = false
  private fatal: Error | null = null
  private restarts = 0
  private readonly loadedExtensions: string[] = []
  readonly connection: SQLiteConnection

  private constructor(
    private readonly openRequest: Omit<OpenRequest, 'id'>,
    private readonly timeoutMs: number,
    private readonly maxRestarts: number,
  ) {
    this.connection = this.makeConnection()
  }

  static async start(
    entry: DriverWorkerEntry,
    path: string,
    options: OpenOptions,
    workerOptions?: WorkerHostOptions,
  ): Promise<WriterWorker> {
    const worker = new WriterWorker(
      { kind: 'open', entry, path, options },
      workerOptions?.writeTimeoutMs ?? DEFAULT_WRITE_TIMEOUT_MS,
      workerOptions?.maxRestarts ?? DEFAULT_MAX_RESTARTS,
    )
    worker.spawn()
    try {
      await worker.ready
    } catch (err) {
      await worker.close()
      throw err
    }
    worker.starting = false
    return worker
  }

  private spawn(): void {
    const script = resolveWorkerScript()
    const worker = new Worker(script.url, { execArgv: script.execArgv })
    this.worker = worker
    worker.on('message', (res: WorkerResponse) => {
      if (this.worker === worker) this.onResponse(res)
    })
    worker.on('error', err => {
      if (this.worker === worker) this.fault(err)
    })
    worker.on('exit', code => {
      if (this.worker === worker && !this.closed) {
        this.fault(new SirannonError(`Writer worker exited with code ${code}`, 'WRITER_WORKER_EXIT'))
      }
    })
    this.ready = this.send(this.openRequest).then(() => this.reloadExtensionsOntoRestartedConnection())
    this.ready.catch(() => {})
  }

  private async reloadExtensionsOntoRestartedConnection(): Promise<void> {
    for (const path of this.loadedExtensions) {
      await this.send({ kind: 'loadExtension', path })
    }
  }

  private onResponse(res: WorkerResponse): void {
    if ('kind' in res) {
      this.onCopyStep(res.id, res.step)
      return
    }
    const entry = this.pending.get(res.id)
    if (!entry) return
    this.pending.delete(res.id)
    clearPendingTimers(entry)
    if (res.ok) {
      entry.resolve(res.value)
      return
    }
    if (res.error.code === WORKER_CANCELLED_CODE) {
      entry.reject(
        new SirannonError(
          `The writer worker could not take this operation within ${entry.deadlineMs}ms; it was not applied and is safe to retry`,
          'WRITE_OVERLOADED',
        ),
      )
      return
    }
    if (res.error.code === WORKER_COPY_STOPPED_CODE) {
      entry.reject(this.unresponsiveError(entry.deadlineMs, entry.kind))
      return
    }
    entry.reject(deserializeError(res.error))
  }

  private onCopyStep(id: number, step: DatabaseCopyStep): void {
    const entry = this.pending.get(id)
    if (!entry) return
    if (entry.timer) {
      clearTimeout(entry.timer)
      entry.timer = entry.deadlineMs > 0 ? setTimeout(() => this.onDeadline(id), entry.deadlineMs) : null
      entry.timer?.unref?.()
    }
    try {
      entry.onStep?.(step)
    } catch (err) {
      this.pending.delete(id)
      clearPendingTimers(entry)
      try {
        this.worker?.postMessage({ kind: 'cancel', id })
      } catch {}
      entry.reject(err instanceof Error ? err : new Error(String(err)))
    }
  }

  private rejectPending(id: number, err: Error): void {
    const entry = this.pending.get(id)
    if (!entry) return
    this.pending.delete(id)
    clearPendingTimers(entry)
    entry.reject(err)
  }

  private unresponsiveError(waitedMs: number, kind?: WorkerRequestBody['kind']): SirannonError {
    if (kind === 'copyDatabase') {
      return new SirannonError(
        `The writer worker moved no page of the copy for ${waitedMs}ms, so the copy was stopped and nothing it wrote is usable`,
        'BACKUP_STALLED',
      )
    }
    return new SirannonError(
      `Writer worker did not respond within ${waitedMs}ms; the operation's outcome is unknown`,
      'WRITER_WORKER_TIMEOUT',
    )
  }

  private onDeadline(id: number): void {
    const entry = this.pending.get(id)
    if (!entry) return
    const worker = this.worker
    if (!entry.cancellable || !worker) {
      this.rejectPending(id, this.unresponsiveError(entry.deadlineMs, entry.kind))
      return
    }
    try {
      worker.postMessage({ kind: 'cancel', id })
    } catch {
      this.rejectPending(id, this.unresponsiveError(entry.deadlineMs, entry.kind))
      return
    }
    entry.graceTimer = setTimeout(() => {
      this.rejectPending(id, this.unresponsiveError(entry.deadlineMs * 2, entry.kind))
    }, entry.deadlineMs)
    entry.graceTimer.unref?.()
  }

  private fault(errLike: unknown): void {
    if (this.closed || this.fatal) return
    const err = errLike instanceof Error ? errLike : new SirannonError(String(errLike), 'WRITER_WORKER_ERROR')
    const dead = this.worker
    this.worker = null
    for (const entry of this.pending.values()) {
      clearPendingTimers(entry)
      entry.reject(err)
    }
    this.pending.clear()
    dead?.terminate().catch(() => {})

    if (this.starting) {
      this.fatal = err
      this.ready = Promise.reject(err)
      this.ready.catch(() => {})
      return
    }

    this.restarts++
    if (this.restarts > this.maxRestarts) {
      this.fatal = new SirannonError(
        `Writer worker failed ${this.restarts} times and will not restart: ${err.message}`,
        'WRITER_WORKER_FATAL',
      )
      this.ready = Promise.reject(this.fatal)
      this.ready.catch(() => {})
      return
    }
    this.spawn()
  }

  private send(request: WorkerRequestBody, onStep?: (step: DatabaseCopyStep) => void): Promise<unknown> {
    const worker = this.worker
    if (!worker) {
      return Promise.reject(
        this.fatal ?? new SirannonError('Writer worker is unavailable', 'WRITER_WORKER_UNAVAILABLE'),
      )
    }
    const id = this.nextId++
    const message = { ...request, id } as WorkerRequest
    const deadlineMs = copyStallDeadline(request) ?? this.timeoutMs
    return new Promise<unknown>((resolve, reject) => {
      let timer: NodeJS.Timeout | null = null
      if (deadlineMs > 0) {
        timer = setTimeout(() => this.onDeadline(id), deadlineMs)
        timer.unref?.()
      }
      const cancellable = request.kind !== 'open' && request.kind !== 'close' && request.kind !== 'loadExtension'
      this.pending.set(id, {
        resolve,
        reject,
        timer,
        graceTimer: null,
        deadlineMs,
        cancellable,
        kind: request.kind,
        ...(onStep ? { onStep } : {}),
      })
      try {
        worker.postMessage(message)
      } catch (err) {
        this.pending.delete(id)
        if (timer) clearTimeout(timer)
        reject(
          new SirannonError(
            `Failed to hand work to the writer worker: ${err instanceof Error ? err.message : String(err)}`,
            'WRITER_WORKER_POST_FAILED',
          ),
        )
      }
    })
  }

  private request(request: WorkerRequestBody, onStep?: (step: DatabaseCopyStep) => void): Promise<unknown> {
    if (this.fatal) return Promise.reject(this.fatal)
    if (this.closed) return Promise.reject(new SirannonError('Writer worker is closed', 'WRITER_WORKER_CLOSED'))
    return this.ready
      .then(() => this.send(request, onStep))
      .then(value => {
        this.restarts = 0
        return value
      })
  }

  private makeConnection(): SQLiteConnection {
    const conn: SQLiteConnection = {
      exec: sql => this.request({ kind: 'exec', sql }) as Promise<void>,
      prepare: async sql => ({
        all: <T = unknown>(...params: unknown[]) => this.request({ kind: 'all', sql, params }) as Promise<T[]>,
        get: <T = unknown>(...params: unknown[]) =>
          this.request({ kind: 'get', sql, params }) as Promise<T | undefined>,
        run: (...params: unknown[]) =>
          this.request({ kind: 'run', sql, params }) as Promise<{ changes: number; lastInsertRowId: number | bigint }>,
        allRaw: <T = unknown>(...params: unknown[]) => this.request({ kind: 'allRaw', sql, params }) as Promise<T[]>,
      }),
      runBatch: (sql, paramsBatch) =>
        this.request({ kind: 'runBatch', sql, paramsBatch: paramsBatch as unknown[][] }) as Promise<
          { changes: number; lastInsertRowId: number | bigint }[]
        >,
      runBatchSummary: (sql, paramsBatch) =>
        this.request({ kind: 'runBatchSummary', sql, paramsBatch: paramsBatch as unknown[][] }) as Promise<{
          rowsLoaded: number
          changes: number
        }>,
      runGroup: units =>
        this.request({
          kind: 'runGroup',
          units: units.map(unit => ({
            statements: unit.statements.map(statement => ({
              sql: statement.sql,
              params: statement.params ? [...statement.params] : [],
              ...(statement.trusted === true ? { trusted: true } : {}),
            })),
          })),
        }) as Promise<GroupRunOutcome[]>,
      copyRunsOffCallerThread: true,
      copyDatabase: request =>
        this.request(
          {
            kind: 'copyDatabase',
            destPath: request.destPath,
            pagesPerStep: request.pagesPerStep,
            ...(request.stallTimeoutMs === undefined ? {} : { stallTimeoutMs: request.stallTimeoutMs }),
          },
          request.onStep,
        ) as Promise<DatabaseCopyStep>,
      loadExtension: async (extensionPath: string) => {
        await this.request({ kind: 'loadExtension', path: extensionPath })
        if (!this.loadedExtensions.includes(extensionPath)) this.loadedExtensions.push(extensionPath)
      },
      transaction: async fn => {
        await conn.exec('BEGIN')
        try {
          const result = await fn(conn)
          await conn.exec('COMMIT')
          return result
        } catch (err) {
          try {
            await conn.exec('ROLLBACK')
          } catch {}
          throw err
        }
      },
      close: () => this.close(),
    }
    return conn
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    const worker = this.worker
    if (worker) {
      try {
        await Promise.race([
          this.send({ kind: 'close' }),
          new Promise((_, reject) => {
            const timer = setTimeout(() => reject(new Error('close timed out')), CLOSE_GRACE_MS)
            timer.unref?.()
          }),
        ])
      } catch {}
      await worker.terminate().catch(() => {})
    }
    this.worker = null
    for (const entry of this.pending.values()) {
      clearPendingTimers(entry)
      entry.reject(new SirannonError('Writer worker is closed', 'WRITER_WORKER_CLOSED'))
    }
    this.pending.clear()
  }
}

function copyStallDeadline(request: WorkerRequestBody): number | undefined {
  return request.kind === 'copyDatabase' ? request.stallTimeoutMs : undefined
}

function clearPendingTimers(entry: PendingRequest): void {
  if (entry.timer) clearTimeout(entry.timer)
  if (entry.graceTimer) clearTimeout(entry.graceTimer)
}
