import type { Database } from './database.js'
import { SirannonError } from './errors.js'

/**
 * Closes every database in a registry and empties it, collecting each close
 * error so that one failure still leaves the rest closed.
 *
 * @param databases - The registry's open databases, which this function empties once every close returns.
 * @throws A `SHUTDOWN_ERROR` that gives the number of failed closes, when one or more closes threw.
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
