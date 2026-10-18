import type { SQLiteConnection } from '../driver/types.js'
import { MigrationError } from '../errors.js'
import {
  ensureMigrationsTable,
  prepareDeleteMigration,
  prepareInsertMigration,
  prepareUpdateMigrationChecksum,
  selectAppliedMigrations,
  selectAppliedMigrationsNewestFirst,
} from '../system-catalog/index.js'
import { Transaction } from '../transaction.js'
import { planPendingMigrations, resolveEffectiveBaseline, SQLITE_USER_VERSION_MAX } from './baseline.js'
import { type AppliedChecksumRow, migrationContentChecksum, reconcileMigrationChecksums } from './checksum.js'
import { isConcurrentMigrationConflict } from './concurrency.js'
import { LAZY_DOWN_SQL, type LazyDownMigration } from './lazy-down.js'
import { mirrorSchemaVersion, syncSchemaVersion } from './schema-version.js'
import {
  type AppliedMigrationEntry,
  MIGRATION_NAME_RE,
  type Migration,
  type MigrationResult,
  type RollbackResult,
} from './types.js'

interface AppliedRow extends AppliedChecksumRow {
  name: string
}

function isMigrationStep(step: unknown): boolean {
  return typeof step === 'string' || typeof step === 'function'
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
    const validated = MigrationRunner.validateMigrations(migrations)
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
      const appliedHere = await conn.transaction(txConn =>
        MigrationRunner.applyOne(txConn, migration, afterEachMigration),
      )
      if (appliedHere) appliedEntries.push({ version: migration.version, name: migration.name })
    }
  }

  private static async applyOne(
    txConn: SQLiteConnection,
    migration: Migration,
    afterEachMigration: ((txConn: SQLiteConnection) => Promise<void>) | undefined,
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
    const insertStmt = await prepareInsertMigration(txConn)
    await insertStmt.run(migration.version, migration.name, migrationContentChecksum(migration))
    await mirrorSchemaVersion(txConn, Math.max(MigrationRunner.highestVersion(applied.keys()), migration.version))
    if (afterEachMigration) await afterEachMigration(txConn)
    return true
  }

  static async rollback(conn: SQLiteConnection, migrations: Migration[], version?: number): Promise<RollbackResult> {
    if (version !== undefined && (!Number.isSafeInteger(version) || version < 0)) {
      throw new MigrationError(
        `Invalid rollback target version: ${version}`,
        typeof version === 'number' && Number.isFinite(version) ? version : 0,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    MigrationRunner.validateMigrations(migrations)
    const inputByVersion = new Map(migrations.map(m => [m.version, m]))

    return MigrationRunner.retryOnConcurrentConflict(() =>
      conn.transaction(async txConn => {
        await ensureMigrationsTable(txConn)
        const appliedRows = await selectAppliedMigrationsNewestFirst(txConn)

        if (appliedRows.length === 0) {
          return { rolledBack: [] }
        }

        let rollbackSet: AppliedMigrationEntry[]
        if (version === undefined) {
          rollbackSet = [appliedRows[0]]
        } else {
          rollbackSet = appliedRows.filter(row => row.version > version)
        }

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
          rolledBackEntries.push({ version: entry.version, name: entry.name })
        }

        const rolledBackVersions = new Set(rollbackSet.map(r => r.version))
        const remaining = appliedRows.find(row => !rolledBackVersions.has(row.version))
        await mirrorSchemaVersion(txConn, remaining === undefined ? 0 : remaining.version)

        return { rolledBack: rolledBackEntries }
      }),
    )
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

  private static validateMigrations(migrations: Migration[]): Migration[] {
    if (!Array.isArray(migrations)) {
      throw new MigrationError('Migrations must be an array of migrations', 0, 'MIGRATION_VALIDATION_ERROR')
    }

    const seenVersions = new Map<number, string>()

    for (const m of migrations) {
      if (typeof m !== 'object' || m === null) {
        throw new MigrationError(
          `Each migration must be an object with a version, a name, and an up; received ${m === null ? 'null' : typeof m}`,
          0,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (!Number.isSafeInteger(m.version) || m.version <= 0 || m.version > SQLITE_USER_VERSION_MAX) {
        throw new MigrationError(
          `Invalid migration version: ${m.version}. Versions must be integers from 1 to ${SQLITE_USER_VERSION_MAX} so they mirror to PRAGMA user_version`,
          typeof m.version === 'number' && Number.isFinite(m.version) ? m.version : 0,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (typeof m.name !== 'string' || !MIGRATION_NAME_RE.test(m.name)) {
        throw new MigrationError(`Invalid migration name: '${String(m.name)}'`, m.version, 'MIGRATION_VALIDATION_ERROR')
      }

      const existing = seenVersions.get(m.version)
      if (existing) {
        throw new MigrationError(
          `Duplicate migration version ${m.version}: '${existing}' and '${m.name}'`,
          m.version,
          'MIGRATION_DUPLICATE_VERSION',
        )
      }
      seenVersions.set(m.version, m.name)

      if (!isMigrationStep(m.up)) {
        throw new MigrationError(
          `Migration ${m.version}_${m.name} must have an up that is SQL text or a function`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (typeof m.up === 'string' && m.up.trim().length === 0) {
        throw new MigrationError(
          `Migration ${m.version}_${m.name} has empty up SQL`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (m.down !== undefined && !isMigrationStep(m.down)) {
        throw new MigrationError(
          `Migration ${m.version}_${m.name} must leave down unset or set it to SQL text or a function`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (typeof m.down === 'string' && m.down.trim().length === 0) {
        throw new MigrationError(
          `Migration ${m.version}_${m.name} has empty down SQL`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (m.baseline !== undefined && (typeof m.baseline !== 'object' || m.baseline === null)) {
        throw new MigrationError(
          `Migration ${m.version}_${m.name} must set baseline to an object with a through version`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }
    }

    return [...migrations].sort((a, b) => a.version - b.version)
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
