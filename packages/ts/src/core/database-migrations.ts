import type { ChangeTracker } from './cdc/change-tracker.js'
import { columnDropAwareConnection } from './cdc/column-drop.js'
import type { ConnectionPool } from './connection-pool.js'
import type { SQLiteConnection } from './driver/types.js'
import { MIGRATIONS_TABLE } from './internal-tables.js'
import { MigrationRunner } from './migrations/runner.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import { type AppliedMigrationRow, selectAppliedMigrations, selectTableExists } from './system-catalog/index.js'
import type { WriterLock } from './writer-lock.js'

async function refreshTriggersAfterSchemaChange(tracker: ChangeTracker | null, conn: SQLiteConnection): Promise<void> {
  if (!tracker || tracker.watchedTables.size === 0) return
  await tracker.refreshAllTriggersUsingConnection(conn)
}

export async function migrateWithTriggerRefresh(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
  migrations: Migration[],
): Promise<MigrationResult> {
  return MigrationRunner.run(migrationConnection(tracker, conn), migrations, txConn =>
    refreshTriggersAfterSchemaChange(tracker, txConn),
  )
}

function migrationConnection(tracker: ChangeTracker | null, conn: SQLiteConnection): SQLiteConnection {
  return tracker === null ? conn : columnDropAwareConnection(tracker, conn)
}

export async function rollbackWithTriggerRefresh(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
  migrations: Migration[],
  version?: number,
): Promise<RollbackResult> {
  const result = await MigrationRunner.rollback(migrationConnection(tracker, conn), migrations, version)
  if (result.rolledBack.length > 0) await refreshTriggersAfterSchemaChange(tracker, conn)
  return result
}

export async function readAppliedMigrations(conn: SQLiteConnection): Promise<AppliedMigrationRow[]> {
  if (!(await selectTableExists(conn, MIGRATIONS_TABLE))) return []
  return selectAppliedMigrations(conn)
}

export interface DatabaseMigrationDeps {
  pool: ConnectionPool
  writerLock: WriterLock
  changeTracker: () => ChangeTracker | null
}

export class DatabaseMigrationController {
  constructor(private readonly deps: DatabaseMigrationDeps) {}

  migrate(migrations: Migration[]): Promise<MigrationResult> {
    const { writerLock, pool, changeTracker } = this.deps
    return writerLock.run(() => migrateWithTriggerRefresh(pool.acquireWriter(), changeTracker(), migrations))
  }

  rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    const { writerLock, pool, changeTracker } = this.deps
    return writerLock.run(() => rollbackWithTriggerRefresh(pool.acquireWriter(), changeTracker(), migrations, version))
  }

  applied(): Promise<AppliedMigrationRow[]> {
    const { writerLock, pool } = this.deps
    if (pool.isReadOnly) return readAppliedMigrations(pool.acquireReader())
    return writerLock.run(() => readAppliedMigrations(pool.acquireWriter()))
  }
}
