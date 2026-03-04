import type Database from 'better-sqlite3'
import { MigrationError } from '../errors.js'
import { Transaction } from '../transaction.js'
import { scanDirectory, readUpMigrations, readDownMigrations } from './scanner.js'
import type { Migration, MigrationResult, RollbackResult, AppliedMigrationEntry } from './types.js'

type SqliteDb = InstanceType<typeof Database>

const CREATE_TRACKING_TABLE = `
  CREATE TABLE IF NOT EXISTS _sirannon_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at REAL NOT NULL DEFAULT (unixepoch('subsec'))
  )
`

export class MigrationRunner {
  static run(db: SqliteDb, input: string | Migration[]): MigrationResult {
    db.exec(CREATE_TRACKING_TABLE)

    const migrations =
      typeof input === 'string'
        ? readUpMigrations(scanDirectory(input))
        : MigrationRunner.validateMigrations(input)

    const applied = MigrationRunner.getAppliedVersions(db)
    const pending = migrations.filter(m => !applied.has(m.version))

    if (pending.length === 0) {
      return { applied: [], skipped: migrations.length }
    }

    const insertMigration = db.prepare('INSERT INTO _sirannon_migrations (version, name) VALUES (?, ?)')
    const appliedEntries: AppliedMigrationEntry[] = []

    db.transaction(() => {
      for (const migration of pending) {
        try {
          if (typeof migration.up === 'string') {
            db.exec(migration.up)
          } else {
            migration.up(new Transaction(db))
          }
        } catch (err) {
          if (err instanceof MigrationError) throw err
          throw new MigrationError(
            `Migration ${migration.version}_${migration.name} failed: ${err instanceof Error ? err.message : String(err)}`,
            migration.version,
          )
        }
        insertMigration.run(migration.version, migration.name)
        appliedEntries.push({ version: migration.version, name: migration.name })
      }
    })()

    return {
      applied: appliedEntries,
      skipped: migrations.length - pending.length,
    }
  }

  static rollback(db: SqliteDb, input: string | Migration[], version?: number): RollbackResult {
    if (version !== undefined && (!Number.isSafeInteger(version) || version < 0)) {
      throw new MigrationError(
        `Invalid rollback target version: ${version}`,
        typeof version === 'number' && Number.isFinite(version) ? version : 0,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    db.exec(CREATE_TRACKING_TABLE)

    const appliedRows = db
      .prepare('SELECT version, name FROM _sirannon_migrations ORDER BY version DESC')
      .all() as AppliedMigrationEntry[]

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

    const rollbackVersions = rollbackSet.map(r => r.version)

    let downByVersion: Map<number, Migration>
    if (typeof input === 'string') {
      const scanned = scanDirectory(input)
      const downMigrations = readDownMigrations(scanned, rollbackVersions)
      downByVersion = new Map(downMigrations.map(m => [m.version, m]))
    } else {
      const inputByVersion = new Map(input.map(m => [m.version, m]))
      downByVersion = new Map()
      for (const v of rollbackVersions) {
        const m = inputByVersion.get(v)
        if (!m || m.down === undefined) {
          const name = rollbackSet.find(r => r.version === v)?.name ?? 'unknown'
          throw new MigrationError(
            `Migration version ${v} (${name}) has no down migration`,
            v,
            'MIGRATION_NO_DOWN',
          )
        }
        downByVersion.set(v, m)
      }
    }

    const deleteMigration = db.prepare('DELETE FROM _sirannon_migrations WHERE version = ?')
    const rolledBackEntries: AppliedMigrationEntry[] = []

    db.transaction(() => {
      for (const entry of rollbackSet) {
        const migration = downByVersion.get(entry.version)!

        try {
          if (typeof migration.down === 'string') {
            db.exec(migration.down)
          } else {
            migration.down!(new Transaction(db))
          }
        } catch (err) {
          if (err instanceof MigrationError) throw err
          throw new MigrationError(
            `Rollback of migration ${entry.version}_${entry.name} failed: ${err instanceof Error ? err.message : String(err)}`,
            entry.version,
            'MIGRATION_ROLLBACK_ERROR',
          )
        }
        deleteMigration.run(entry.version)
        rolledBackEntries.push({ version: entry.version, name: entry.name })
      }
    })()

    return { rolledBack: rolledBackEntries }
  }

  private static validateMigrations(migrations: Migration[]): Migration[] {
    const seenVersions = new Map<number, string>()

    for (const m of migrations) {
      if (!Number.isSafeInteger(m.version) || m.version <= 0) {
        throw new MigrationError(
          `Invalid migration version: ${m.version}`,
          typeof m.version === 'number' && Number.isFinite(m.version) ? m.version : 0,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (!/^\w+$/.test(m.name)) {
        throw new MigrationError(
          `Invalid migration name: '${m.name}'`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
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

      if (typeof m.up === 'string' && m.up.trim().length === 0) {
        throw new MigrationError(
          `Migration ${m.version}_${m.name} has empty up SQL`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }

      if (m.down !== undefined && typeof m.down === 'string' && m.down.trim().length === 0) {
        throw new MigrationError(
          `Migration ${m.version}_${m.name} has empty down SQL`,
          m.version,
          'MIGRATION_VALIDATION_ERROR',
        )
      }
    }

    return [...migrations].sort((a, b) => a.version - b.version)
  }

  private static getAppliedVersions(db: SqliteDb): Set<number> {
    const rows = db.prepare('SELECT version FROM _sirannon_migrations ORDER BY version').all() as { version: number }[]
    return new Set(rows.map(r => r.version))
  }
}
