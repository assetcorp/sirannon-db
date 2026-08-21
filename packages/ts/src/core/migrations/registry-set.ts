import type { Database } from '../database.js'
import { MigrationError } from '../errors.js'
import type { Migration, MigrationSource } from './types.js'

/**
 * The migrations a registry applies to every database it opens.
 *
 * A registry given a function loads the set once and returns the same promise
 * to every open behind it, so that ten databases opening together read the
 * migration files once between them. A load that fails leaves nothing cached,
 * and the next open tries again.
 *
 * @internal
 */
export class RegistryMigrationSet {
  private loading: Promise<Migration[]> | null = null

  constructor(private readonly source: MigrationSource | undefined) {}

  /**
   * Reads the set, loading it where a function supplies it.
   *
   * @returns The migrations, which is an empty list where the registry has none.
   * @throws When the function returns anything but a list.
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
   * Applies the set to one database. A database that refuses writes takes none.
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
