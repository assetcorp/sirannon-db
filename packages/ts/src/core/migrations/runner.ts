import type { SQLiteConnection } from '../driver/types.js'
import { MigrationError, SirannonError } from '../errors.js'
import { MIGRATIONS_TABLE } from '../internal-tables.js'
import {
  ensureMigrationsTable,
  prepareDeleteMigration,
  prepareInsertMigration,
  prepareUpdateMigrationChecksum,
  selectAppliedMigrations,
  selectAppliedMigrationsNewestFirst,
  selectTableExists,
} from '../system-catalog/index.js'
import { Transaction } from '../transaction.js'
import { planPendingMigrations, resolveEffectiveBaseline } from './baseline.js'
import { type AppliedChecksumRow, migrationContentChecksum, reconcileMigrationChecksums } from './checksum.js'
import { isConcurrentMigrationConflict } from './concurrency.js'
import { assertForeignKeysHold, runsWithoutForeignKeys, withForeignKeysOff } from './foreign-keys.js'
import { LAZY_DOWN_SQL, type LazyDownMigration } from './lazy-down.js'
import { mirrorSchemaVersion, syncSchemaVersion } from './schema-version.js'
import type { AppliedMigrationEntry, Migration, MigrationResult, RollbackResult } from './types.js'
import { validateMigrations } from './validation.js'

interface AppliedRow extends AppliedChecksumRow {
  name: string
}

async function committedOrMigrationError<T>(
  what: string,
  version: number,
  code: string,
  run: () => Promise<T>,
): Promise<T> {
  try {
    return await run()
  } catch (err) {
    if (err instanceof SirannonError || isConcurrentMigrationConflict(err)) throw err
    throw new MigrationError(`${what} failed: ${err instanceof Error ? err.message : String(err)}`, version, code)
  }
}

/**
 * Applies and rolls back migrations against one connection.
 *
 * @internal
 */
// biome-ignore lint/complexity/noStaticOnlyClass: exported as a class namespace
export class MigrationRunner {
  static async run(
    conn: SQLiteConnection,
    migrations: Migration[],
    afterEachMigration?: (txConn: SQLiteConnection) => Promise<void>,
  ): Promise<MigrationResult> {
    const validated = validateMigrations(migrations)
    const effectiveBaseline = resolveEffectiveBaseline(validated)
    const appliedEntries: AppliedMigrationEntry[] = []

    await MigrationRunner.retryOnConcurrentConflict(() =>
      MigrationRunner.applyPending(conn, validated, effectiveBaseline, appliedEntries, afterEachMigration),
    )
    return { applied: appliedEntries, skipped: validated.length - appliedEntries.length }
  }

  private static async retryOnConcurrentConflict<T>(operation: () => Promise<T>): Promise<T> {
    try {
      return await operation()
    } catch (err) {
      if (!isConcurrentMigrationConflict(err)) throw err
    }

    try {
      return await operation()
    } catch (err) {
      if (!isConcurrentMigrationConflict(err)) throw err
      throw new MigrationError(
        'Another process is writing to the migration history and the busy timeout elapsed; retry once it finishes',
        0,
        'MIGRATION_CONCURRENT',
      )
    }
  }

  private static async applyPending(
    conn: SQLiteConnection,
    validated: Migration[],
    effectiveBaseline: Migration | undefined,
    appliedEntries: AppliedMigrationEntry[],
    afterEachMigration: ((txConn: SQLiteConnection) => Promise<void>) | undefined,
  ): Promise<void> {
    const pending = await conn.transaction(async txConn => {
      await ensureMigrationsTable(txConn)
      const applied = await MigrationRunner.getAppliedRows(txConn)

      const backfills = reconcileMigrationChecksums(validated, applied)
      if (backfills.length > 0) {
        const updateStmt = await prepareUpdateMigrationChecksum(txConn)
        for (const backfill of backfills) {
          await updateStmt.run(backfill.checksum, backfill.version)
        }
      }

      const planned = planPendingMigrations(validated, effectiveBaseline, new Set(applied.keys()))
      if (planned.length === 0) {
        await syncSchemaVersion(txConn, MigrationRunner.highestVersion(applied.keys()))
      }
      return planned
    })

    for (const migration of pending) {
      const withoutForeignKeys = runsWithoutForeignKeys(migration, migration.up)
      const label = `${migration.version}_${migration.name}`
      const apply = () =>
        committedOrMigrationError(`Migration ${label}`, migration.version, 'MIGRATION_ERROR', () =>
          conn.transaction(txConn =>
            MigrationRunner.applyOne(txConn, migration, afterEachMigration, withoutForeignKeys),
          ),
        )
      const appliedHere = withoutForeignKeys
        ? await withForeignKeysOff(conn, migration.version, 'MIGRATION_ERROR', apply)
        : await apply()
      if (appliedHere) appliedEntries.push({ version: migration.version, name: migration.name })
    }
  }

  private static async applyOne(
    txConn: SQLiteConnection,
    migration: Migration,
    afterEachMigration: ((txConn: SQLiteConnection) => Promise<void>) | undefined,
    withoutForeignKeys: boolean,
  ): Promise<boolean> {
    const applied = await MigrationRunner.getAppliedRows(txConn)
    if (applied.has(migration.version)) return false

    try {
      if (typeof migration.up === 'string') {
        await txConn.exec(migration.up)
      } else {
        const tx = new Transaction(txConn)
        try {
          const result = migration.up(tx)
          if (result instanceof Promise) await result
        } finally {
          tx.end()
        }
      }
    } catch (err) {
      if (err instanceof MigrationError) throw err
      if (isConcurrentMigrationConflict(err)) throw err
      throw new MigrationError(
        `Migration ${migration.version}_${migration.name} failed: ${err instanceof Error ? err.message : String(err)}`,
        migration.version,
      )
    }
    if (withoutForeignKeys) {
      await assertForeignKeysHold(
        txConn,
        `${migration.version}_${migration.name}`,
        migration.version,
        'MIGRATION_ERROR',
      )
    }
    const insertStmt = await prepareInsertMigration(txConn)
    await insertStmt.run(migration.version, migration.name, migrationContentChecksum(migration))
    await mirrorSchemaVersion(txConn, Math.max(MigrationRunner.highestVersion(applied.keys()), migration.version))
    if (afterEachMigration) await afterEachMigration(txConn)
    return true
  }

  static async rollback(
    conn: SQLiteConnection,
    migrations: Migration[],
    version?: number,
    afterRollback?: (txConn: SQLiteConnection) => Promise<void>,
  ): Promise<RollbackResult> {
    if (version !== undefined && (!Number.isSafeInteger(version) || version < 0)) {
      throw new MigrationError(
        `Invalid rollback target version: ${version}`,
        typeof version === 'number' && Number.isFinite(version) ? version : 0,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    validateMigrations(migrations)
    const inputByVersion = new Map(migrations.map(m => [m.version, m]))

    return MigrationRunner.retryOnConcurrentConflict(async () => {
      const withoutForeignKeys = await MigrationRunner.rollbackRunsWithoutForeignKeys(conn, inputByVersion, version)
      const run = () =>
        committedOrMigrationError('Rollback', version ?? 0, 'MIGRATION_ROLLBACK_ERROR', () =>
          conn.transaction(txConn =>
            MigrationRunner.rollbackInTransaction(txConn, inputByVersion, version, withoutForeignKeys, afterRollback),
          ),
        )
      return withoutForeignKeys ? withForeignKeysOff(conn, version ?? 0, 'MIGRATION_ROLLBACK_ERROR', run) : run()
    })
  }

  private static rollbackSetOf(
    appliedRows: AppliedMigrationEntry[],
    version: number | undefined,
  ): AppliedMigrationEntry[] {
    if (appliedRows.length === 0) return []
    return version === undefined ? [appliedRows[0]] : appliedRows.filter(row => row.version > version)
  }

  private static downsRunWithoutForeignKeys(downs: Iterable<Migration>): boolean {
    for (const migration of downs) {
      if (runsWithoutForeignKeys(migration, migration.down)) return true
    }
    return false
  }

  private static async rollbackRunsWithoutForeignKeys(
    conn: SQLiteConnection,
    inputByVersion: Map<number, Migration>,
    version: number | undefined,
  ): Promise<boolean> {
    if (!(await selectTableExists(conn, MIGRATIONS_TABLE))) return false
    const downs: Migration[] = []
    for (const entry of MigrationRunner.rollbackSetOf(await selectAppliedMigrationsNewestFirst(conn), version)) {
      const migration = inputByVersion.get(entry.version)
      if (migration === undefined) continue
      if (migration.withoutForeignKeys === true) return true
      if (migration.down === undefined && (migration as LazyDownMigration)[LAZY_DOWN_SQL] === undefined) continue
      downs.push(MigrationRunner.resolveDownMigration(migration, entry.version))
    }
    return MigrationRunner.downsRunWithoutForeignKeys(downs)
  }

  private static async rollbackInTransaction(
    txConn: SQLiteConnection,
    inputByVersion: Map<number, Migration>,
    version: number | undefined,
    withoutForeignKeys: boolean,
    afterRollback: ((txConn: SQLiteConnection) => Promise<void>) | undefined,
  ): Promise<RollbackResult> {
    await ensureMigrationsTable(txConn)
    const appliedRows = await selectAppliedMigrationsNewestFirst(txConn)
    const rollbackSet = MigrationRunner.rollbackSetOf(appliedRows, version)

    if (rollbackSet.length === 0) {
      return { rolledBack: [] }
    }

    const downByVersion = new Map<number, Migration>()
    for (const entry of rollbackSet) {
      downByVersion.set(
        entry.version,
        MigrationRunner.resolveDownMigration(inputByVersion.get(entry.version), entry.version),
      )
    }
    if (!withoutForeignKeys && MigrationRunner.downsRunWithoutForeignKeys(downByVersion.values())) {
      throw new MigrationError(
        'The migration history changed after the rollback chose whether to turn foreign keys off; retry the rollback',
        version ?? 0,
        'MIGRATION_CONCURRENT',
      )
    }

    const rolledBackEntries: AppliedMigrationEntry[] = []
    const deleteStmt = await prepareDeleteMigration(txConn)

    for (const entry of rollbackSet) {
      const migration = downByVersion.get(entry.version)
      if (!migration) {
        throw new MigrationError(
          `No down migration found for version ${entry.version}`,
          entry.version,
          'MIGRATION_ROLLBACK_ERROR',
        )
      }

      try {
        if (typeof migration.down === 'string') {
          await txConn.exec(migration.down)
        } else {
          const tx = new Transaction(txConn)
          try {
            const result = migration.down?.(tx)
            if (result instanceof Promise) await result
          } finally {
            tx.end()
          }
        }
      } catch (err) {
        if (err instanceof MigrationError) throw err
        if (isConcurrentMigrationConflict(err)) throw err
        throw new MigrationError(
          `Rollback of migration ${entry.version}_${entry.name} failed: ${err instanceof Error ? err.message : String(err)}`,
          entry.version,
          'MIGRATION_ROLLBACK_ERROR',
        )
      }
      await deleteStmt.run(entry.version)
      if (afterRollback) await afterRollback(txConn)
      rolledBackEntries.push({ version: entry.version, name: entry.name })
    }

    if (withoutForeignKeys) {
      await assertForeignKeysHold(
        txConn,
        `rollback to ${version ?? 'the previous version'}`,
        version ?? 0,
        'MIGRATION_ROLLBACK_ERROR',
      )
    }
    const rolledBackVersions = new Set(rollbackSet.map(r => r.version))
    const remaining = appliedRows.find(row => !rolledBackVersions.has(row.version))
    await mirrorSchemaVersion(txConn, remaining === undefined ? 0 : remaining.version)

    return { rolledBack: rolledBackEntries }
  }

  private static resolveDownMigration(migration: Migration | undefined, version: number): Migration {
    if (!migration) {
      throw new MigrationError(`Migration version ${version} has no down migration`, version, 'MIGRATION_NO_DOWN')
    }

    if (migration.down !== undefined) {
      return migration
    }

    const readDown = (migration as LazyDownMigration)[LAZY_DOWN_SQL]
    if (readDown === undefined) {
      throw new MigrationError(`Migration version ${version} has no down migration`, version, 'MIGRATION_NO_DOWN')
    }

    return { version: migration.version, name: migration.name, up: migration.up, down: readDown() }
  }

  private static highestVersion(versions: Iterable<number>): number {
    let highest = 0
    for (const version of versions) {
      if (version > highest) highest = version
    }
    return highest
  }

  private static async getAppliedRows(conn: SQLiteConnection): Promise<Map<number, AppliedRow>> {
    const rows = await selectAppliedMigrations(conn)
    return new Map(rows.map(r => [r.version, { name: r.name, checksum: r.checksum }]))
  }
}
