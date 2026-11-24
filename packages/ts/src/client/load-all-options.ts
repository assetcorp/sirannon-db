import type { BulkLoadDurability } from '../core/types.js'

/** Options for {@link RemoteDatabase.loadAll}.
 * @public
 */
export interface LoadAllOptions {
  /**
   * The number of rows in each batch that the client sends to the server. Each
   * batch is one request that must fit under the server's `maxBodyBytes`, so for
   * wide rows, raise that limit or lower this value. Defaults to 1000.
   */
  batchSize?: number
  /** The writer's durability level during the load. Defaults to 'off'. */
  durability?: BulkLoadDurability
}
