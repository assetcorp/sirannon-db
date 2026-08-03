import type { StagedRecovery } from './sync/staged-pull.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, ReplicationChange } from './sync/types.js'
import type { AppliedMigrationRow } from './system-catalog/index.js'
import type { ChangeEvent } from './types.js'

/**
 * How far a device has pulled from its server, and which sequence space that
 * position belongs to.
 *
 * @public
 */
export interface DeviceSyncPullState {
  /** Highest server change-log position this device has applied. */
  seq: bigint
  /** Sequence space that position came from. */
  epoch: string | undefined
}

/**
 * What a device's local database offers the sync loop: it stages and applies
 * what the server sends, reads what the device still owes the server, and
 * keeps both cursors. Take one from {@link Database.deviceSync}.
 *
 * @public
 */
export interface DeviceSyncPort {
  /** Returns the identifier this device stamps its own changes with. */
  identity(): Promise<{ nodeId: string }>
  /** Applies one pulled transaction and advances the pull cursor in the same write. */
  applyPulledTransaction(
    changes: readonly ReplicationChange[],
    pullSeq: bigint,
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult>
  /** Stores pulled changes durably without applying them, so an interrupted device resumes from disk. */
  stagePulledChanges(events: readonly ChangeEvent[]): Promise<bigint | null>
  /** Applies whatever is staged and returns the position reached. */
  applyStagedPull(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<bigint | null>
  /** Applies what a previous run left staged, and reports the failure that stopped it. */
  recoverStagedPull(
    resolver?: ConflictResolver | ((table: string) => ConflictResolver),
    onChange?: (event: ChangeEvent) => void,
  ): Promise<StagedRecovery>
  /** Reads the next run of local changes the device still owes the server. */
  readOutboxBatch(afterSeq: bigint, limit: number): Promise<ReplicationBatch | null>
  /** Counts the local changes the device still owes the server. */
  countOutboxPending(afterSeq: bigint): Promise<number>
  /** Reads how far the device has pushed. */
  getPushCursor(): Promise<bigint>
  /** Records how far the device has pushed. */
  setPushCursor(seq: bigint): Promise<void>
  /** Reads how far the device has pulled, or null before it has pulled anything. */
  getPullState(): Promise<DeviceSyncPullState | null>
  /** Reports whether the server has told this device to download a fresh snapshot. */
  getResyncRequired(): Promise<boolean>
  /** Records whether the device must download a fresh snapshot. */
  setResyncRequired(required: boolean): Promise<void>
  /** Records how far the device has pulled, and the sequence space it came from. */
  setPullState(seq: bigint, epoch?: string): Promise<void>
  /** Keeps changes above a position out of pruning until the server has accepted them. */
  protectUnpushedChanges(pushedSeq: bigint): void
  /** Reports whether a snapshot download was interrupted and still has to finish. */
  snapshotLoadPending(): Promise<boolean>
  /** Clears the named tables and opens the database for a snapshot download. */
  beginSnapshotLoad(tables: readonly string[]): Promise<void>
  /** Applies the schema statements a snapshot carries. */
  applySnapshotSchema(schema: readonly string[]): Promise<void>
  /** Writes one page of snapshot rows into a table. */
  loadSnapshotPage(table: string, rows: readonly Record<string, unknown>[]): Promise<void>
  /** Replaces the local migration history with the server's. */
  replaceMigrationHistory(rows: readonly AppliedMigrationRow[]): Promise<void>
  /** Closes a completed snapshot download and reopens the database for normal use. */
  endSnapshotLoad(tables: readonly string[]): Promise<void>
  /** Abandons a snapshot download, leaving the database marked as needing another. */
  abortSnapshotLoad(): Promise<void>
}
