import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'
import type Database from 'better-sqlite3'
import { MigrationError } from '../errors.js'
import type { MigrationFile, MigrationResult } from '../types.js'
import type { AppliedMigration } from './types.js'

type SqliteDb = InstanceType<typeof Database>

/**
 * Pattern for valid migration filenames: one or more digits, an underscore,
 * one or more word characters (letters, digits, underscores), ending in `.sql`.
 *
 * Examples: `001_create_users.sql`, `2_add_email.sql`, `10_drop_legacy_table.sql`
 */
const MIGRATION_FILENAME_PATTERN = /^(\d+)_(\w+)\.sql$/

const CREATE_TRACKING_TABLE = `
  CREATE TABLE IF NOT EXISTS _sirannon_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at REAL NOT NULL DEFAULT (unixepoch('subsec'))
  )
`

export class MigrationRunner {
  /**
   * Run all pending migrations from the given directory against the database.
   *
   * Creates the `_sirannon_migrations` tracking table if it doesn't exist,
   * scans the directory for `NNN_name.sql` files, skips already-applied
   * versions, and runs the rest in ascending version order inside a single
   * transaction. Each applied migration is recorded in the tracking table
   * before the transaction commits.
   *
   * @throws {MigrationError} if the path doesn't exist, contains duplicate
   *   versions, or a migration statement fails.
   */
  static run(db: SqliteDb, migrationsPath: string): MigrationResult {
    const resolvedPath = resolve(migrationsPath)

    let stat: ReturnType<typeof statSync> | undefined
    try {
      stat = statSync(resolvedPath)
    } catch {
      throw new MigrationError(`Migrations path does not exist: ${resolvedPath}`, 0)
    }

    if (!stat.isDirectory()) {
      throw new MigrationError(`Migrations path is not a directory: ${resolvedPath}`, 0)
    }

    db.exec(CREATE_TRACKING_TABLE)

    const files = MigrationRunner.scanMigrations(resolvedPath)
    const applied = MigrationRunner.getAppliedVersions(db)
    const pending = files.filter(f => !applied.has(f.version))

    if (pending.length === 0) {
      return { applied: [], skipped: files.length }
    }

    const insertMigration = db.prepare('INSERT INTO _sirannon_migrations (version, name) VALUES (?, ?)')

    db.transaction(() => {
      for (const migration of pending) {
        try {
          db.exec(migration.sql)
        } catch (err) {
          throw new MigrationError(
            `Migration ${migration.version}_${migration.name} failed: ${err instanceof Error ? err.message : String(err)}`,
            migration.version,
          )
        }
        insertMigration.run(migration.version, migration.name)
      }
    })()

    return {
      applied: pending,
      skipped: files.length - pending.length,
    }
  }

  /**
   * Scan a directory for migration files, parse them, validate for duplicates,
   * and return them sorted by version in ascending order.
   */
  private static scanMigrations(dirPath: string): MigrationFile[] {
    const entries = readdirSync(dirPath, { withFileTypes: true })
    const migrations: MigrationFile[] = []
    const seenVersions = new Map<number, string>()

    for (const entry of entries) {
      if (!entry.isFile()) continue

      const match = MIGRATION_FILENAME_PATTERN.exec(entry.name)
      if (!match) continue

      const version = parseInt(match[1], 10)
      const name = match[2]

      const existing = seenVersions.get(version)
      if (existing) {
        throw new MigrationError(`Duplicate migration version ${version}: '${existing}' and '${entry.name}'`, version)
      }
      seenVersions.set(version, entry.name)

      const filePath = join(dirPath, entry.name)
      const sql = readFileSync(filePath, 'utf-8').trim()

      if (sql.length === 0) {
        throw new MigrationError(`Migration file is empty: ${entry.name}`, version)
      }

      migrations.push({ version, name, sql })
    }

    migrations.sort((a, b) => a.version - b.version)
    return migrations
  }

  /**
   * Read the set of already-applied migration versions from the tracking table.
   */
  private static getAppliedVersions(db: SqliteDb): Set<number> {
    const rows = db.prepare('SELECT version FROM _sirannon_migrations ORDER BY version').all() as Pick<
      AppliedMigration,
      'version'
    >[]

    return new Set(rows.map(r => r.version))
  }
}
