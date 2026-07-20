import type { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { CHANGES_TABLE } from '../../core/internal-tables.js'
import type { HLC } from '../hlc.js'
import type { ApplyResult, ConflictResolver, ReplicationBatch, SyncPhase, SyncTableManifest } from '../types.js'
import { BatchApplier } from './batch-applier.js'
import { BatchReader } from './batch-reader.js'
import { DumpOps } from './dump.js'
import { PkResolver } from './pk.js'
import { SchemaOps } from './schema.js'
import { StateOps } from './state.js'

/**
 * Persistent change log that bridges CDC events and the replication protocol.
 *
 * Composes specialised helpers: BatchReader (outbound batches), BatchApplier
 * (inbound batches and conflict resolution), DumpOps (table dumps and
 * manifests), SchemaOps (replication-table bootstrap, schema dump, wipe),
 * StateOps (sequence and sync metadata), and PkResolver (cached primary-key
 * lookups).
 */
export class ReplicationLog {
  private readonly pkResolver: PkResolver
  private readonly state: StateOps
  private readonly schema: SchemaOps
  private readonly batchReader: BatchReader
  private readonly batchApplier: BatchApplier
  private readonly dump: DumpOps

  constructor(
    conn: SQLiteConnection,
    localNodeId: string,
    hlc: HLC,
    private readonly changesTable: string = CHANGES_TABLE,
    tracker?: ChangeTracker,
  ) {
    this.pkResolver = new PkResolver(conn)
    this.state = new StateOps(conn)
    this.schema = new SchemaOps(conn, changesTable)
    this.batchReader = new BatchReader(conn, localNodeId, hlc, changesTable, this.pkResolver)
    this.batchApplier = new BatchApplier(
      conn,
      localNodeId,
      hlc,
      this.pkResolver,
      fromNodeId => this.state.getLastAppliedSeq(fromNodeId),
      tracker,
    )
    this.dump = new DumpOps(conn, localNodeId, hlc, this.pkResolver)
  }

  ensureReplicationTables(): Promise<void> {
    return this.schema.ensureReplicationTables()
  }

  readBatch(afterSeq: bigint, batchSize: number): Promise<ReplicationBatch | null> {
    return this.batchReader.readBatch(afterSeq, batchSize)
  }

  stampChanges(tx: SQLiteConnection, afterSeq: bigint, txId: string): Promise<void> {
    return this.batchReader.stampChanges(tx, afterSeq, txId)
  }

  updateColumnVersions(tx: SQLiteConnection, afterSeq: bigint): Promise<void> {
    return this.batchReader.updateColumnVersions(tx, afterSeq)
  }

  applyBatch(
    batch: ReplicationBatch,
    resolver: ConflictResolver | ((table: string) => ConflictResolver),
  ): Promise<ApplyResult> {
    return this.batchApplier.applyBatch(batch, resolver)
  }

  getLastAppliedSeq(fromNodeId: string): Promise<bigint> {
    return this.state.getLastAppliedSeq(fromNodeId)
  }

  setLastAppliedSeq(fromNodeId: string, seq: bigint): Promise<void> {
    return this.state.setLastAppliedSeq(fromNodeId, seq)
  }

  getPeerAckedSeq(peerNodeId: string): Promise<bigint> {
    return this.state.getPeerAckedSeq(peerNodeId)
  }

  getLocalSeq(): Promise<bigint> {
    return this.state.getLocalSeq(this.changesTable)
  }

  recoverMaxObservedHlc(): Promise<string | null> {
    return this.state.recoverMaxObservedHlc(this.changesTable)
  }

  getMinAckedSeq(): Promise<bigint | null> {
    return this.state.getMinAckedSeq()
  }

  registerActiveSyncSeq(seq: bigint): void {
    this.state.registerActiveSyncSeq(seq)
  }

  unregisterActiveSyncSeq(seq: bigint): void {
    this.state.unregisterActiveSyncSeq(seq)
  }

  dumpTable(table: string, batchSize: number): AsyncGenerator<ReplicationBatch> {
    return this.dump.dumpTable(table, batchSize)
  }

  dumpTableOnConnection(
    conn: SQLiteConnection,
    table: string,
    batchSize: number,
  ): AsyncGenerator<{ rows: Record<string, unknown>[]; checksum: string; isLast: boolean }> {
    return this.dump.dumpTableOnConnection(conn, table, batchSize)
  }

  dumpSchema(conn: SQLiteConnection, excludePrefix?: string): Promise<string[]> {
    return this.schema.dumpSchema(conn, excludePrefix)
  }

  getTablesInFkOrder(conn: SQLiteConnection): Promise<string[]> {
    return this.schema.getTablesInFkOrder(conn)
  }

  wipeTables(conn: SQLiteConnection, tables: string[], tracker: ChangeTracker): Promise<void> {
    return this.schema.wipeTables(conn, tables, tracker)
  }

  setSyncTableStatus(table: string, status: string, rowCount?: number, pkHash?: string): Promise<void> {
    return this.state.setSyncTableStatus(table, status, rowCount, pkHash)
  }

  setSyncMeta(phase: SyncPhase, snapshotSeq?: bigint, sourcePeerId?: string, requestId?: string): Promise<void> {
    return this.state.setSyncMeta(phase, snapshotSeq, sourcePeerId, requestId)
  }

  getSyncState(): Promise<{
    phase: SyncPhase
    completedTables: string[]
    snapshotSeq: bigint | null
    sourcePeerId: string | null
  }> {
    return this.state.getSyncState()
  }

  isSyncCompleted(): Promise<boolean> {
    return this.state.isSyncCompleted()
  }

  generateManifest(conn: SQLiteConnection, table: string): Promise<SyncTableManifest> {
    return this.dump.generateManifest(conn, table)
  }

  verifyManifest(manifest: SyncTableManifest): Promise<boolean> {
    return this.dump.verifyManifest(manifest)
  }
}
