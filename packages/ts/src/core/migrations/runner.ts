import type { SQLiteConnection } from '../driver/types.js'
import { MigrationError } from '../errors.js'
import { MIGRATIONS_TABLE } from '../internal-tables.js'
import { Transaction } from '../transaction.js'
import { LAZY_DOWN_SQL, type LazyDownMigration } from './lazy-down.js'
import type { AppliedMigrationEntry, Migration, MigrationResult, RollbackResult } from './types.js'

const CREATE_TRACKING_TABLE = `
  CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
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
      const insertStmt = await txConn.prepare(`INSERT INTO ${MIGRATIONS_TABLE} (version, name) VALUES (?, ?)`)

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

    const selectStmt = await conn.prepare(`SELECT version, name FROM ${MIGRATIONS_TABLE} ORDER BY version DESC`)
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
      downByVersion.set(v, MigrationRunner.resolveDownMigration(inputByVersion.get(v), v))
    }

    const rolledBackEntries: AppliedMigrationEntry[] = []

    await conn.transaction(async txConn => {
      const deleteStmt = await txConn.prepare(`DELETE FROM ${MIGRATIONS_TABLE} WHERE version = ?`)

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
    const stmt = await conn.prepare(`SELECT version FROM ${MIGRATIONS_TABLE} ORDER BY version`)
    const rows = (await stmt.all()) as { version: number }[]
    return new Set(rows.map(r => r.version))
  }
}
