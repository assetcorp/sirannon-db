import type { StagedRecovery } from './sync/staged-pull.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, ReplicationChange } from './sync/types.js'
import type { AppliedMigrationRow } from './system-catalog/index.js'
import type { ChangeEvent } from './types.js'

/**
 * The position up to which a device has pulled from its server, and the epoch
 * of the server change log that the position comes from.
 *
 * @public
 */
export interface DeviceSyncPullState {
  /** The highest server change-log position that this device has applied. */
  seq: bigint
  /** The epoch of the server change log that the position comes from. */
  epoch: string | undefined
}

/**
 * Gives a device's sync loop the calls that it makes on the local database,
 * which stage and apply the changes that the server sends, read the local
 * changes that the device has yet to push, and store the push and pull
 * cursors. Get one from {@link Database.deviceSync}.
 *
 * @public
 */
export interface DeviceSyncPort {
  /** Returns the node identifier that this device stamps on its own changes. */
  identity(): Promise<{ nodeId: string }>
  /** Applies one pulled transaction and advances the pull cursor in the same write. */
  applyPulledTransaction(
    changes: readonly ReplicationChange[],
    pullSeq: bigint,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult>
  /** Stores pulled changes on disk without applying them, so that a device that stops part-way can resume from them, and returns the highest position stored, or null for an empty list. */
  stagePulledChanges(events: readonly ChangeEvent[]): Promise<bigint | null>
  /** Applies each complete staged transaction in order, and returns the position of the last one applied, or null when it applied none. */
  applyStagedPull(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<bigint | null>
  /** Applies the changes that an earlier session left staged, and returns the position to resume from, the position applied, and any error that stopped this apply. */
  recoverStagedPull(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<StagedRecovery>
  /** Returns the next batch of local changes that the device has yet to push, or null when none remain. */
  readOutboxBatch(afterSeq: bigint, limit: number): Promise<ReplicationBatch | null>
  /** Counts the local changes that the device has yet to push. */
  countOutboxPending(afterSeq: bigint): Promise<number>
  /** Returns the position up to which the device has pushed. */
  getPushCursor(): Promise<bigint>
  /** Stores the position up to which the device has pushed. */
  setPushCursor(seq: bigint): Promise<void>
  /** Returns the position up to which the device has pulled, or null before its first pull. */
  getPullState(): Promise<DeviceSyncPullState | null>
  /** Returns true when the device must download a fresh snapshot, as {@link DeviceSyncPort.setResyncRequired} last stored. */
  getResyncRequired(): Promise<boolean>
  /** Stores whether the device must download a fresh snapshot. */
  setResyncRequired(required: boolean): Promise<void>
  /** Stores the position up to which the device has pulled, and the epoch that the position comes from. */
  setPullState(seq: bigint, epoch?: string): Promise<void>
  /** Stops change-log pruning from deleting any local change above the pushed position, so that the device can still push it. */
  protectUnpushedChanges(pushedSeq: bigint): void
  /** Returns true when a snapshot download started and has yet to finish. */
  snapshotLoadPending(): Promise<boolean>
  /** Drops the named tables, discards staged pulled changes, and rejects ordinary reads and writes on this database until {@link DeviceSyncPort.endSnapshotLoad} completes. */
  beginSnapshotLoad(tables: readonly string[]): Promise<void>
  /** Executes the schema statements that a snapshot contains. */
  applySnapshotSchema(schema: readonly string[]): Promise<void>
  /** Writes one page of snapshot rows into a table. */
  loadSnapshotPage(table: string, rows: readonly Record<string, unknown>[]): Promise<void>
  /** Replaces the local migration history with the server's. */
  replaceMigrationHistory(rows: readonly AppliedMigrationRow[]): Promise<void>
  /** Finishes a snapshot download: watches the named tables again, turns foreign keys back on, and lifts the block on ordinary reads and writes. */
  endSnapshotLoad(tables: readonly string[]): Promise<void>
  /** Turns foreign keys back on after a failed snapshot download and leaves the download pending, so ordinary reads and writes stay rejected and {@link DeviceSyncPort.snapshotLoadPending} returns true. */
  abortSnapshotLoad(): Promise<void>
}
