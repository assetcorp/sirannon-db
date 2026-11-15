import {
  deleteRolledBackChanges,
  ensureRolledBackChangesTable,
  type RolledBackChange,
  selectRolledBackChanges,
} from '../core/system-catalog/index.js'
import type { ReplicationEngine } from './engine/engine.js'

/**
 * Returns every change that this node undid when it reconciled its history with a new primary's, oldest first, so
 * that an operator can review the writes that only this node held and apply again the ones that still matter.
 *
 * A node keeps these changes after it gives up primary duty with writes that no other voter received, and after a
 * full copy replaces rows that it wrote later than the copy. The node keeps them until
 * {@link clearRolledBackChanges} deletes them.
 *
 * @param engine - The engine of the node whose rolled-back changes to read.
 * @returns The rolled-back changes, or an empty list when the node has undone none.
 *
 * @public
 */
export function rolledBackChanges(engine: ReplicationEngine): Promise<RolledBackChange[]> {
  return engine.history.exclusive(async () => {
    await ensureRolledBackChangesTable(engine.writerConn)
    return selectRolledBackChanges(engine.writerConn)
  })
}

/**
 * Deletes every change that this node keeps from its rollbacks, once an operator has reviewed them through
 * {@link rolledBackChanges}.
 *
 * @param engine - The engine of the node whose rolled-back changes to delete.
 * @returns A promise that settles once the node has deleted them.
 *
 * @public
 */
export function clearRolledBackChanges(engine: ReplicationEngine): Promise<void> {
  return engine.history.exclusive(async () => {
    await ensureRolledBackChangesTable(engine.writerConn)
    await deleteRolledBackChanges(engine.writerConn)
  })
}
