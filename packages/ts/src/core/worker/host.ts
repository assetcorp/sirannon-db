import { Worker } from 'node:worker_threads'
import type { DriverWorkerEntry, GroupRunOutcome, OpenOptions, SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { deserializeError, type WorkerRequest, type WorkerRequestBody, type WorkerResponse } from './protocol.js'
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
}

type OpenRequest = Extract<WorkerRequest, { kind: 'open' }>

/**
 * Owns one writer worker thread and presents it as a {@link SQLiteConnection}.
 * A natural worker crash or non-zero exit rejects every in-flight request and
 * respawns. A per-operation deadline instead rejects only the stalled caller
 * and leaves the worker running, because a thread inside a synchronous native
 * SQLite call cannot be interrupted: terminating it would leak the connection's
 * file lock and can abort the whole process. A rejected write has an
 * indeterminate outcome, since its commit may still reach disk, so a caller
 * must reconcile state before retrying a non-idempotent write.
 */
export class WriterWorker {
  private worker: Worker | null = null
  private readonly pending = new Map<number, PendingRequest>()
  private nextId = 1
  private ready: Promise<void> = Promise.resolve()
  private starting = true
  private closed = false
  private fatal: Error | null = null
  private restarts = 0
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
      if (this.worker === worker && !this.closed && code !== 0) {
        this.fault(new SirannonError(`Writer worker exited with code ${code}`, 'WRITER_WORKER_EXIT'))
      }
    })
    this.ready = this.send(this.openRequest).then(() => {
      this.restarts = 0
    })
    this.ready.catch(() => {})
  }

  private onResponse(res: WorkerResponse): void {
    const entry = this.pending.get(res.id)
    if (!entry) return
    this.pending.delete(res.id)
    if (entry.timer) clearTimeout(entry.timer)
    if (res.ok) entry.resolve(res.value)
    else entry.reject(deserializeError(res.error))
  }

  private rejectPending(id: number, err: Error): void {
    const entry = this.pending.get(id)
    if (!entry) return
    this.pending.delete(id)
    if (entry.timer) clearTimeout(entry.timer)
    entry.reject(err)
  }

  private fault(errLike: unknown): void {
    if (this.closed || this.fatal) return
    const err = errLike instanceof Error ? errLike : new SirannonError(String(errLike), 'WRITER_WORKER_ERROR')
    const dead = this.worker
    this.worker = null
    for (const entry of this.pending.values()) {
      if (entry.timer) clearTimeout(entry.timer)
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

  private send(request: WorkerRequestBody): Promise<unknown> {
    const worker = this.worker
    if (!worker) {
      return Promise.reject(
        this.fatal ?? new SirannonError('Writer worker is unavailable', 'WRITER_WORKER_UNAVAILABLE'),
      )
    }
    const id = this.nextId++
    const message = { ...request, id } as WorkerRequest
    return new Promise<unknown>((resolve, reject) => {
      let timer: NodeJS.Timeout | null = null
      if (this.timeoutMs > 0) {
        timer = setTimeout(() => {
          this.rejectPending(
            id,
            new SirannonError(
              `Writer worker did not respond within ${this.timeoutMs}ms; the operation's outcome is unknown`,
              'WRITER_WORKER_TIMEOUT',
            ),
          )
        }, this.timeoutMs)
        timer.unref?.()
      }
      this.pending.set(id, { resolve, reject, timer })
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

  private request(request: WorkerRequestBody): Promise<unknown> {
    if (this.fatal) return Promise.reject(this.fatal)
    if (this.closed) return Promise.reject(new SirannonError('Writer worker is closed', 'WRITER_WORKER_CLOSED'))
    return this.ready.then(() => this.send(request))
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
      runGroup: batch =>
        this.request({
          kind: 'runGroup',
          batch: batch.map(job => ({ sql: job.sql, params: job.params ? [...job.params] : [] })),
        }) as Promise<GroupRunOutcome[]>,
      transaction: async fn => {
        await conn.exec('BEGIN')
        try {
          const result = await fn(conn)
          await conn.exec('COMMIT')
          return result
        } catch (err) {
          try {
            await conn.exec('ROLLBACK')
          } catch {
            /* ROLLBACK failure is secondary; preserve the original error */
          }
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
      } catch {
        /* fall through to a hard terminate */
      }
      await worker.terminate().catch(() => {})
    }
    this.worker = null
    for (const entry of this.pending.values()) {
      if (entry.timer) clearTimeout(entry.timer)
      entry.reject(new SirannonError('Writer worker is closed', 'WRITER_WORKER_CLOSED'))
    }
    this.pending.clear()
  }
}
