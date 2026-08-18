import { applyDdlSideEffectsIfRelevant } from './cdc/ddl-handler.js'
import { ConnectionPool } from './connection-pool.js'
import { assertChangeLogCaptureSupported, DatabaseBackupController } from './database-backup.js'
import { DatabaseCdcController } from './database-cdc.js'
import { DatabaseMigrationController } from './database-migrations.js'
import { DatabaseObserver } from './database-observability.js'
import type { DatabaseReadDeps } from './database-reads.js'
import { DatabaseSyncController } from './database-sync.js'
import { DatabaseWriteController } from './database-writes.js'
import { DEFAULT_SYNCHRONOUS } from './driver/synchronous.js'
import type { SQLiteDriver } from './driver/types.js'
import { SirannonError } from './errors.js'
import { GroupCommitter } from './group-committer.js'
import { HookRegistry } from './hooks/registry.js'
import { LoadedExtensions } from './loaded-extensions.js'
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
  writes: DatabaseWriteController
  reads: DatabaseReadDeps
  migrations: DatabaseMigrationController
  loadExtension: (extensionPath: string) => Promise<void>
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

  if (options?.backups && !readOnly) {
    assertChangeLogCaptureSupported(driver, id, path, options.walMode ?? true)
  }

  const pool = await ConnectionPool.create({
    driver,
    path,
    readOnly: options?.readOnly,
    readPoolSize: options?.readPoolSize ?? 4,
    walMode: options?.walMode ?? true,
    synchronous: options?.synchronous,
    ...(options?.backups ? { walAutoCheckpoint: 0 } : {}),
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
    driver.capabilities,
    id,
    path,
    driver.createBackupEngine?.(),
  )
  const canOpenSnapshotConnection = driver.capabilities.multipleConnections && path !== ':memory:'
  const extensions = new LoadedExtensions(driver)
  const openSnapshotConnection = () => extensions.open(() => driver.open(path, { readonly: true, walMode: false }))
  const cdc = new DatabaseCdcController(
    op => writerLock.run(op),
    () => pool.acquireWriter(),
    options?.cdcPollInterval ?? 50,
    options?.cdcRetention ?? 3_600_000,
    canOpenSnapshotConnection ? openSnapshotConnection : null,
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

  const writes = new DatabaseWriteController({
    pool,
    writeGate,
    writerLock,
    groupCommitter,
    cdc,
    observer,
    synchronous: options?.synchronous ?? DEFAULT_SYNCHRONOUS,
    walMode: options?.walMode ?? true,
    capturesChangeLog: options?.backups !== undefined && !readOnly,
  })

  const migrations = new DatabaseMigrationController({
    pool,
    writerLock,
    changeTracker: () => cdc.changeTracker,
  })

  if (options?.backups && !readOnly) backups.startCycle(options.backups)

  return {
    pool,
    writeGate,
    writerLock,
    hookRegistry,
    observer,
    backups,
    cdc,
    sync,
    groupCommitter,
    writes,
    reads: { pool, writerLock, observer },
    migrations,
    loadExtension: extensionPath => writerLock.run(() => extensions.load(pool.connections(), extensionPath)),
  }
}

export async function closeDatabaseRuntime(
  runtime: DatabaseRuntime,
  closeListeners: readonly (() => void | Promise<void>)[],
): Promise<void> {
  runtime.cdc.stop()
  runtime.backups.cancelAll()

  let poolError: unknown
  try {
    await runtime.cdc.closeLiveConnection()
    await runtime.writes.drain()
    await runtime.writerLock.settle()
    await runtime.backups.stopCycle()
    await runtime.pool.close()
  } catch (err) {
    poolError = err
  }

  for (const fn of closeListeners) {
    try {
      await fn()
    } catch {}
  }

  if (poolError) {
    throw poolError
  }
}
