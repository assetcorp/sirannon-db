import type { ChangeTracker } from './cdc/change-tracker.js'
import type { SQLiteConnection } from './driver/types.js'
import { MIGRATIONS_TABLE } from './internal-tables.js'
import { MigrationRunner } from './migrations/runner.js'
import type { Migration, MigrationResult, RollbackResult } from './migrations/types.js'
import { type AppliedMigrationRow, appliedMigrationRows, tableExists } from './system-catalog/index.js'

async function refreshTriggersAfterSchemaChange(tracker: ChangeTracker | null, conn: SQLiteConnection): Promise<void> {
  if (!tracker || tracker.watchedTables.size === 0) return
  await tracker.refreshAllTriggersUsingConnection(conn)
}

export async function migrateWithTriggerRefresh(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
  migrations: Migration[],
): Promise<MigrationResult> {
  const result = await MigrationRunner.run(conn, migrations)
  if (result.applied.length > 0) await refreshTriggersAfterSchemaChange(tracker, conn)
  return result
}

export async function rollbackWithTriggerRefresh(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
  migrations: Migration[],
  version?: number,
): Promise<RollbackResult> {
  const result = await MigrationRunner.rollback(conn, migrations, version)
  if (result.rolledBack.length > 0) await refreshTriggersAfterSchemaChange(tracker, conn)
  return result
}

export async function readAppliedMigrations(conn: SQLiteConnection): Promise<AppliedMigrationRow[]> {
  if (!(await tableExists(conn, MIGRATIONS_TABLE))) return []
  return appliedMigrationRows(conn)
}
