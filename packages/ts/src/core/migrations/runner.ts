import type { SQLiteConnection } from '../driver/types.js'
import { MigrationError } from '../errors.js'
import { Transaction } from '../transaction.js'
import type { AppliedMigrationEntry, Migration, MigrationResult, RollbackResult } from './types.js'

const CREATE_TRACKING_TABLE = `
  CREATE TABLE IF NOT EXISTS _sirannon_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at REAL NOT NULL DEFAULT (unixepoch('subsec'))
  )
`

// biome-ignore lint/complexity/noStaticOnlyClass: public API exported as a class namespace
export class MigrationRunner {
  static async run(conn: SQLiteConnection, migrations: Migration[]): Promise<MigrationResult> {
    await conn.exec(CREATE_TRACKING_TABLE)

    const validated = MigrationRunner.validateMigrations(migrations)
    const applied = await MigrationRunner.getAppliedVersions(conn)
    const pending = validated.filter(m => !applied.has(m.version))

    if (pending.length === 0) {
      return { applied: [], skipped: validated.length }
    }

    const appliedEntries: AppliedMigrationEntry[] = []

    await conn.transaction(async txConn => {
      const insertStmt = await txConn.prepare('INSERT INTO _sirannon_migrations (version, name) VALUES (?, ?)')

      for (const migration of pending) {
        try {
          if (typeof migration.up === 'string') {
            await txConn.exec(migration.up)
          } else {
            const result = migration.up(new Transaction(txConn))
            if (result instanceof Promise) await result
          }
        } catch (err) {
          if (err instanceof MigrationError) throw err
          throw new MigrationError(
            `Migration ${migration.version}_${migration.name} failed: ${err instanceof Error ? err.message : String(err)}`,
            migration.version,
          )
        }
        await insertStmt.run(migration.version, migration.name)
        appliedEntries.push({ version: migration.version, name: migration.name })
      }
    })

    return {
      applied: appliedEntries,
      skipped: validated.length - pending.length,
    }
  }

  static async rollback(conn: SQLiteConnection, migrations: Migration[], version?: number): Promise<RollbackResult> {
    if (version !== undefined && (!Number.isSafeInteger(version) || version < 0)) {
      throw new MigrationError(
        `Invalid rollback target version: ${version}`,
        typeof version === 'number' && Number.isFinite(version) ? version : 0,
        'MIGRATION_VALIDATION_ERROR',
      )
    }

    await conn.exec(CREATE_TRACKING_TABLE)

    const selectStmt = await conn.prepare('SELECT version, name FROM _sirannon_migrations ORDER BY version DESC')
    const appliedRows = (await selectStmt.all()) as AppliedMigrationEntry[]

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

    MigrationRunner.validateMigrations(migrations)

    const rollbackVersions = rollbackSet.map(r => r.version)
    const inputByVersion = new Map(migrations.map(m => [m.version, m]))
    const downByVersion = new Map<number, Migration>()
    for (const v of rollbackVersions) {
      const m = inputByVersion.get(v)
      if (!m || m.down === undefined) {
        throw new MigrationError(`Migration version ${v} has no down migration`, v, 'MIGRATION_NO_DOWN')
      }
      downByVersion.set(v, m)
    }

    const rolledBackEntries: AppliedMigrationEntry[] = []

    await conn.transaction(async txConn => {
      const deleteStmt = await txConn.prepare('DELETE FROM _sirannon_migrations WHERE version = ?')

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
            const result = migration.down?.(new Transaction(txConn))
            if (result instanceof Promise) await result
          }
        } catch (err) {
          if (err instanceof MigrationError) throw err
          throw new MigrationError(
            `Rollback of migration ${entry.version}_${entry.name} failed: ${err instanceof Error ? err.message : String(err)}`,
            entry.version,
            'MIGRATION_ROLLBACK_ERROR',
          )
        }
        await deleteStmt.run(entry.version)
        rolledBackEntries.push({ version: entry.version, name: entry.name })
      }
    })

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
        throw new MigrationError(`Invalid migration name: '${m.name}'`, m.version, 'MIGRATION_VALIDATION_ERROR')
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

  private static async getAppliedVersions(conn: SQLiteConnection): Promise<Set<number>> {
    const stmt = await conn.prepare('SELECT version FROM _sirannon_migrations ORDER BY version')
    const rows = (await stmt.all()) as { version: number }[]
    return new Set(rows.map(r => r.version))
  }
}
