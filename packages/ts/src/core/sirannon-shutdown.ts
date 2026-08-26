import type { Database } from './database.js'
import { SirannonError } from './errors.js'

/**
 * Closes every database in a registry and empties it, keeping what each close
 * threw so that one failure still leaves the rest closed.
 *
 * @param databases - The registry's open databases, emptied once every close has returned.
 * @throws When one or more closes threw, reporting how many.
 *
 * @internal
 */
export async function closeEveryDatabase(databases: Map<string, Database>): Promise<void> {
  const errors: unknown[] = []

  for (const database of [...databases.values()]) {
    try {
      await database.close()
    } catch (err) {
      errors.push(err)
    }
  }

  databases.clear()

  if (errors.length > 0) {
    throw new SirannonError(`Shutdown completed with ${errors.length} error(s)`, 'SHUTDOWN_ERROR')
  }
}
