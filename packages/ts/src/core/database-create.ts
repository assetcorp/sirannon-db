import { ConnectionPool } from './connection-pool.js'
import type { SQLiteDriver } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { DatabaseOptions } from './types.js'
import { resolveWriterWorkerConfig } from './worker/config.js'
import { WriteGate } from './worker/gate.js'

export interface DatabaseRuntime {
  pool: ConnectionPool
  writeGate: WriteGate
}

export async function createDatabaseRuntime(
  id: string,
  path: string,
  driver: SQLiteDriver,
  options?: DatabaseOptions,
): Promise<DatabaseRuntime> {
  const writerWorker = resolveWriterWorkerConfig(options?.writerWorker)
  const readOnly = options?.readOnly ?? false
  if (writerWorker.enabled && !readOnly && !driver.startWriterHost) {
    throw new SirannonError(
      `writerWorker is enabled for database '${id}' but the driver does not carry a worker entry; use a driver that supports worker offload or disable writerWorker`,
      'WRITER_WORKER_UNSUPPORTED',
    )
  }

  const pool = await ConnectionPool.create({
    driver,
    path,
    readOnly: options?.readOnly,
    readPoolSize: options?.readPoolSize ?? 4,
    walMode: options?.walMode ?? true,
    synchronous: options?.synchronous,
    useWriterWorker: writerWorker.enabled && !readOnly,
    workerHostOptions: writerWorker.host,
  })

  const writeGate = new WriteGate(writerWorker.enabled ? writerWorker.maxPendingWrites : 0, writerWorker.retryAfterMs)
  return { pool, writeGate }
}
