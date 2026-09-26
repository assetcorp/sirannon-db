import type { Database } from '../database.js'
import { MigrationError } from '../errors.js'
import type { Migration, MigrationSource } from './types.js'

/**
 * Holds the migrations that a registry applies to every database that it opens.
 *
 * When the migrations come from a function, the set calls that function once
 * and returns the same promise to every open, so that ten databases opening
 * together load the migration files once. When the load fails, the set clears
 * the cached promise, so the next open calls the function again.
 *
 * @internal
 */
export class RegistryMigrationSet {
  private loading: Promise<Migration[]> | null = null

  constructor(private readonly source: MigrationSource | undefined) {}

  /**
   * Returns the migrations, and calls the source function on the first request when the migrations come from a function.
   *
   * @returns The migrations, or an empty list when the registry has none.
   * @throws A `MigrationError` with code `MIGRATION_SOURCE_INVALID` when the function returns something other than an array.
   */
  load(): Promise<Migration[]> {
    const source = this.source
    if (source === undefined || Array.isArray(source)) {
      return Promise.resolve(source ?? [])
    }
    if (this.loading) return this.loading

    const loading = (async () => {
      const set = await source()
      if (!Array.isArray(set)) {
        throw new MigrationError(
          'The migrations source must return an array of migrations',
          0,
          'MIGRATION_SOURCE_INVALID',
        )
      }
      return set
    })()

    loading.catch(() => {
      if (this.loading === loading) this.loading = null
    })
    this.loading = loading
    return loading
  }

  /**
   * Applies the migrations to one database, and skips a read-only database.
   *
   * @param db - The database to migrate.
   */
  async applyTo(db: Database): Promise<void> {
    if (this.source === undefined || db.readOnly) return
    const migrations = await this.load()
    if (migrations.length === 0) return
    await db.migrate(migrations)
  }
}
