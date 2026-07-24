import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import { ConnectionPool } from './connection-pool.js'
import { DatabaseBackupController } from './database-backup.js'
import { DatabaseCdcController } from './database-cdc.js'
import { DatabaseObserver } from './database-observability.js'
import { DatabaseSyncController } from './database-sync.js'
import type { SQLiteDriver } from './driver/types.js'
import { SirannonError } from './errors.js'
import { GroupCommitter } from './group-committer.js'
import { HookRegistry } from './hooks/registry.js'
import type { MetricsCollector } from './metrics/collector.js'
import { snapshotLoadPending } from './sync/snapshot-apply.js'
import type { DatabaseOptions } from './types.js'
import { resolveWriterWorkerConfig } from './worker/config.js'
import { WriteGate } from './worker/gate.js'
import { WriterLock } from './writer-lock.js'

export interface DatabaseInternals {
  parentHooks?: HookRegistry
  metrics?: MetricsCollector
}

export interface DatabaseRuntime {
  pool: ConnectionPool
  writeGate: WriteGate
  writerLock: WriterLock
  hookRegistry: HookRegistry
  observer: DatabaseObserver
  backups: DatabaseBackupController
  cdc: DatabaseCdcController
  sync: DatabaseSyncController
  groupCommitter: GroupCommitter
}

export async function createDatabaseRuntime(
  id: string,
  path: string,
  driver: SQLiteDriver,
  options?: DatabaseOptions,
  internals?: DatabaseInternals,
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
  const writerLock = new WriterLock(driver.createWriterContext?.())
  const hookRegistry = new HookRegistry()
  const observer = new DatabaseObserver(id, hookRegistry, internals?.parentHooks ?? null, internals?.metrics ?? null)
  const backups = new DatabaseBackupController(
    op => writerLock.run(op),
    () => pool.acquireWriter(),
    driver.createBackupEngine?.(),
  )
  const cdc = new DatabaseCdcController(
    op => writerLock.run(op),
    () => pool.acquireWriter(),
    options?.cdcPollInterval ?? 50,
    options?.cdcRetention ?? 3_600_000,
  )
  const sync = new DatabaseSyncController(
    op => writeGate.run(() => writerLock.run(op)),
    () => pool.acquireWriter(),
    cdc,
  )
  if (!readOnly && (await snapshotLoadPending(pool.acquireWriter()))) {
    sync.seedSnapshotGate()
  }
  const groupCommitter = new GroupCommitter(writerLock, {
    acquireWriter: () => pool.acquireWriter(),
    afterCommit: (writer, sql) => applyDdlSideEffectsIfRelevant(cdc.changeTracker, writer, sql),
    stampStatements: stampOptions => cdc.stampStatements(stampOptions),
  })

  return { pool, writeGate, writerLock, hookRegistry, observer, backups, cdc, sync, groupCommitter }
}
