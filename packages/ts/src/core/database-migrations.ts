import type { ChangeTracker } from './cdc/change-tracker.js'
import { columnDropAwareConnection } from './cdc/column-drop.js'
import type { ConnectionPool } from './connection-pool.js'
import type { SQLiteConnection } from './driver/types.js'
import { MIGRATIONS_TABLE } from './internal-tables.js'
import { MigrationRunner } from './migrations/runner.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import type { SyncStamper } from './sync/stamper.js'
import { type AppliedMigrationRow, selectAppliedMigrations, selectTableExists } from './system-catalog/index.js'
import type { WriterLock } from './writer-lock.js'

async function refreshTriggersAfterSchemaChange(tracker: ChangeTracker | null, conn: SQLiteConnection): Promise<void> {
  if (!tracker || tracker.watchedTables.size === 0) return
  await tracker.refreshAllTriggersUsingConnection(conn)
}

/**
 * Applies the pending migrations on the writer and returns what the runner applied, refreshing the change-capture
 * triggers after each one.
 *
 * When the database stamps its changes, every migration transaction first stamps the changes that another process
 * wrote, then leaves its own changes unstamped and raises the stamp floor above them, because each device applies the
 * registry's migrations itself.
 *
 * @param conn - The writer connection.
 * @param tracker - The change tracker, or `null` when the database watches nothing.
 * @param migrations - The registry's migrations.
 * @param stamper - The stamper, or `null` when the database stamps nothing.
 * @returns The migrations that the runner applied.
 * @internal
 */
export async function migrateWithTriggerRefresh(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
  migrations: Migration[],
  stamper: SyncStamper | null = null,
): Promise<MigrationResult> {
  return MigrationRunner.run(migrationConnection(tracker, conn, stamper), migrations, txConn =>
    refreshTriggersAfterSchemaChange(tracker, txConn),
  )
}

function migrationConnection(
  tracker: ChangeTracker | null,
  conn: SQLiteConnection,
  stamper: SyncStamper | null,
): SQLiteConnection {
  const aware = tracker === null ? conn : columnDropAwareConnection(tracker, conn)
  if (stamper === null) return aware
  return {
    ...aware,
    transaction: fn =>
      aware.transaction(async txConn => {
        await stamper.openStamps(txConn)
        const value = await fn(txConn)
        await stamper.claimWithoutStamping(txConn)
        return value
      }),
  }
}

/**
 * Rolls back applied migrations on the writer and returns what the runner rolled back, refreshing the change-capture
 * triggers afterwards, and treats the changes that the rollback writes as {@link migrateWithTriggerRefresh} treats a
 * migration's.
 *
 * @param conn - The writer connection.
 * @param tracker - The change tracker, or `null` when the database watches nothing.
 * @param migrations - The registry's migrations.
 * @param version - The version to roll back to, or `undefined` for the newest migration alone.
 * @param stamper - The stamper, or `null` when the database stamps nothing.
 * @returns The migrations that the runner rolled back.
 * @internal
 */
export async function rollbackWithTriggerRefresh(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
  migrations: Migration[],
  version?: number,
  stamper: SyncStamper | null = null,
): Promise<RollbackResult> {
  const result = await MigrationRunner.rollback(migrationConnection(tracker, conn, stamper), migrations, version)
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
  stamper: () => SyncStamper | null
}

export class DatabaseMigrationController {
  constructor(private readonly deps: DatabaseMigrationDeps) {}

  migrate(migrations: Migration[]): Promise<MigrationResult> {
    const { writerLock, pool, changeTracker, stamper } = this.deps
    return writerLock.run(() => migrateWithTriggerRefresh(pool.acquireWriter(), changeTracker(), migrations, stamper()))
  }

  rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    const { writerLock, pool, changeTracker, stamper } = this.deps
    return writerLock.run(() =>
      rollbackWithTriggerRefresh(pool.acquireWriter(), changeTracker(), migrations, version, stamper()),
    )
  }

  applied(): Promise<AppliedMigrationRow[]> {
    const { writerLock, pool } = this.deps
    if (pool.isReadOnly) return readAppliedMigrations(pool.acquireReader())
    return writerLock.run(() => readAppliedMigrations(pool.acquireWriter()))
  }
}
