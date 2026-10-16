import type { ChangeTracker } from '../cdc/change-tracker.js'
import type { SQLiteConnection } from '../driver/types.js'

/**
 * Names whether a node records each schema change that it applies from a peer in its own change log. A node in a
 * coordinator group uses `'logged'`, so that its change-log history holds every schema change that it applied, and
 * every other node uses `'unlogged'`.
 *
 * @internal
 */
export type AppliedSchemaChanges = 'logged' | 'unlogged'

/**
 * Configures a batch applier beyond the connection, the node, and the clock that every applier needs.
 *
 * @internal
 */
export interface BatchApplierOptions {
  /** Holds the change tracker whose triggers the applier refreshes after a schema change. */
  tracker?: ChangeTracker
  /** Names the change-log table, which defaults to the standard one. */
  changesTable?: string
  /** Runs inside each apply transaction before the applier writes any row. */
  beforeApply?: (tx: SQLiteConnection) => Promise<void>
  /** Names whether the applier records each applied schema change in the change log, which defaults to `'unlogged'`. */
  appliedSchemaChanges?: AppliedSchemaChanges
}
