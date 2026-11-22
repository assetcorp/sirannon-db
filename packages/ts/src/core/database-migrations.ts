import type { ChangeTracker } from './cdc/change-tracker.js'
import {
  captureKeepingConnection,
  migrationCaptureTracker,
  migrationSchemaConnection,
  restoreMigrationCapture,
} from './cdc/migration-capture.js'
import type { ConnectionPool } from './connection-pool.js'
import type { SQLiteConnection } from './driver/types.js'
import { MIGRATIONS_TABLE } from './internal-tables.js'
import { MigrationRunner } from './migrations/runner.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import { SyncStamper } from './sync/stamper.js'
import { type AppliedMigrationRow, selectAppliedMigrations, selectTableExists } from './system-catalog/index.js'
import type { WriterLock } from './writer-lock.js'

async function refreshTriggersAfterSchemaChange(
  tracker: ChangeTracker | null,
  capture: ChangeTracker | null,
  conn: SQLiteConnection,
): Promise<void> {
  if (capture) await restoreMigrationCapture(capture, conn)
  if (!tracker || tracker.watchedTables.size === 0) return
  await tracker.refreshAllTriggersUsingConnection(migrationSchemaConnection(conn))
}

/**
 * Applies the pending migrations on the writer and returns what the runner applied, keeping change capture on every
 * table whose writes reach the change log.
 *
 * Capture covers each table that the database watches and each table that carries change-capture triggers from
 * another process or an earlier session. A migration that drops a column of such a table, or copies it into a new
 * table, leaves its triggers in place for every write after that statement, while the rows that the migration copies
 * into a table that it creates stay out of the change log.
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
  const migration = migrationConnection(conn, tracker, stamper)
  return MigrationRunner.run(migration.conn, migrations, migration.refresh)
}

function migrationConnection(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
  stamper: SyncStamper | null,
): { conn: SQLiteConnection; refresh: (txConn: SQLiteConnection) => Promise<void> } {
  const refreshers = new WeakMap<SQLiteConnection, () => Promise<void>>()
  return {
    conn: {
      ...conn,
      transaction: fn =>
        conn.transaction(async txConn => {
          const capture = await migrationCaptureTracker(txConn, tracker)
          const stamping = stamper ?? (capture === null ? null : await SyncStamper.init(txConn, capture.changeLogTable))
          const aware = capture === null ? txConn : captureKeepingConnection(capture, txConn)
          refreshers.set(aware, () => refreshTriggersAfterSchemaChange(tracker, capture, txConn))
          try {
            await stamping?.openStamps(txConn)
            const value = await fn(aware)
            await stamping?.claimWithoutStamping(txConn)
            return value
          } finally {
            refreshers.delete(aware)
          }
        }),
    },
    refresh: async txConn => {
      await refreshers.get(txConn)?.()
    },
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
  const migration = migrationConnection(conn, tracker, stamper)
  return MigrationRunner.rollback(migration.conn, migrations, version, migration.refresh)
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
    const { pool, changeTracker, stamper } = this.deps
    return this.holdWriter(migrations, 'up', () =>
      migrateWithTriggerRefresh(pool.acquireWriter(), changeTracker(), migrations, stamper()),
    )
  }

  rollback(migrations: Migration[], version?: number): Promise<RollbackResult> {
    const { pool, changeTracker, stamper } = this.deps
    return this.holdWriter(migrations, 'down', () =>
      rollbackWithTriggerRefresh(pool.acquireWriter(), changeTracker(), migrations, version, stamper()),
    )
  }

  private holdWriter<T>(migrations: Migration[], step: 'up' | 'down', operation: () => Promise<T>): Promise<T> {
    const { writerLock } = this.deps
    const callsApplication =
      Array.isArray(migrations) && migrations.some(migration => typeof migration?.[step] === 'function')
    return callsApplication ? writerLock.runCallback(operation) : writerLock.run(operation)
  }

  applied(): Promise<AppliedMigrationRow[]> {
    const { writerLock, pool } = this.deps
    if (pool.isReadOnly) return readAppliedMigrations(pool.acquireReader())
    return writerLock.run(() => readAppliedMigrations(pool.acquireWriter()))
  }
}
