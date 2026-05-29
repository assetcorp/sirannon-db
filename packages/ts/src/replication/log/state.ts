import type { SQLiteConnection } from '../../core/driver/types.js'
import { HLC } from '../hlc.js'
import type { SyncPhase } from '../types.js'

export class StateOps {
  private readonly activeSyncSnapshotSeqs = new Set<bigint>()

  constructor(private readonly conn: SQLiteConnection) {}

  /**
   * Returns the highest valid HLC observed in this database, or `null` when
   * no replication evidence exists yet.
   *
   * Reads two durable sources and takes the lexicographic maximum:
   * - `MAX(hlc) FROM <changes table>` covers HLCs stamped onto local writes.
   * - `MAX(hlc) FROM _sirannon_column_versions` covers HLCs applied from
   *   remote batches and persisted alongside per-column versions.
   *
   * Combining both is necessary: a primary that has never received remote
   * batches has no rows in `_sirannon_column_versions` for inbound HLCs, and
   * a replica that has never written locally has no rows in the changes
   * table. Either source can independently hold the highest value depending
   * on traffic patterns.
   *
   * The changes table is subject to retention pruning by `ChangeTracker`,
   * which means its `MAX(hlc)` can lag behind the highest HLC ever emitted.
   * Cross-checking against `_sirannon_column_versions` (which is upserted
   * per `(table, row, column)` and not subject to time-based retention)
   * keeps the recovered value above any HLC currently still observable by
   * any peer's conflict-resolution decisions.
   *
   * Filters out the empty-string sentinel that the CDC triggers write before
   * the local-stamp pass fills in a real HLC, and rejects any decoded
   * timestamp whose components are not finite, non-negative integers. The
   * SQL filter narrows the result set; the JavaScript-level guard catches a
   * future producer that writes a malformed HLC.
   */
  async recoverMaxObservedHlc(changesTable: string): Promise<string | null> {
    let best: string | null = null

    const changesStmt = await this.conn.prepare(`SELECT MAX(hlc) AS max_hlc FROM "${changesTable}" WHERE hlc != ''`)
    const changesRow = (await changesStmt.get()) as { max_hlc: string | null } | undefined
    best = mergeCandidate(best, changesRow?.max_hlc ?? null)

    const versionsStmt = await this.conn.prepare(
      `SELECT MAX(hlc) AS max_hlc FROM _sirannon_column_versions WHERE hlc != ''`,
    )
    const versionsRow = (await versionsStmt.get()) as { max_hlc: string | null } | undefined
    best = mergeCandidate(best, versionsRow?.max_hlc ?? null)

    return best
  }

  async getLastAppliedSeq(fromNodeId: string): Promise<bigint> {
    const stmt = await this.conn.prepare(
      'SELECT MAX(source_seq) as max_seq FROM _sirannon_applied_changes WHERE source_node_id = ?',
    )
    const row = (await stmt.get(fromNodeId)) as { max_seq: number | null } | undefined
    if (!row || row.max_seq === null) {
      return 0n
    }
    return BigInt(row.max_seq)
  }

  async setLastAppliedSeq(fromNodeId: string, seq: bigint): Promise<void> {
    const stmt = await this.conn.prepare(
      `INSERT INTO _sirannon_peer_state (peer_node_id, last_acked_seq, updated_at)
       VALUES (?, ?, ?)
       ON CONFLICT(peer_node_id)
       DO UPDATE SET
         last_acked_seq = max(_sirannon_peer_state.last_acked_seq, excluded.last_acked_seq),
         updated_at = CASE
           WHEN excluded.last_acked_seq >= _sirannon_peer_state.last_acked_seq THEN excluded.updated_at
           ELSE _sirannon_peer_state.updated_at
         END`,
    )
    await stmt.run(fromNodeId, seq.toString(), Date.now() / 1000)
  }

  async getPeerAckedSeq(peerNodeId: string): Promise<bigint> {
    const stmt = await this.conn.prepare('SELECT last_acked_seq FROM _sirannon_peer_state WHERE peer_node_id = ?')
    const row = (await stmt.get(peerNodeId)) as { last_acked_seq: number | string | null } | undefined
    if (!row || row.last_acked_seq === null) {
      return 0n
    }
    return BigInt(row.last_acked_seq)
  }

  async getLocalSeq(changesTable: string): Promise<bigint> {
    const stmt = await this.conn.prepare(`SELECT MAX(seq) as max_seq FROM "${changesTable}"`)
    const row = (await stmt.get()) as { max_seq: number | null } | undefined
    if (!row || row.max_seq === null) {
      return 0n
    }
    return BigInt(row.max_seq)
  }

  async getMinAckedSeq(): Promise<bigint | null> {
    const stmt = await this.conn.prepare(
      'SELECT MIN(last_acked_seq) as min_seq, COUNT(*) as cnt FROM _sirannon_peer_state',
    )
    const row = (await stmt.get()) as { min_seq: number | null; cnt: number } | undefined

    const hasPeers = row !== undefined && row.cnt > 0
    let result: bigint | null = null

    if (hasPeers && row.min_seq !== null) {
      result = BigInt(row.min_seq)
    } else if (hasPeers) {
      result = 0n
    }

    for (const syncSeq of this.activeSyncSnapshotSeqs) {
      if (result === null || syncSeq < result) {
        result = syncSeq
      }
    }

    return result
  }

  registerActiveSyncSeq(seq: bigint): void {
    this.activeSyncSnapshotSeqs.add(seq)
  }

  unregisterActiveSyncSeq(seq: bigint): void {
    this.activeSyncSnapshotSeqs.delete(seq)
  }

  async setSyncTableStatus(table: string, status: string, rowCount?: number, pkHash?: string): Promise<void> {
    const stmt = await this.conn.prepare(
      `INSERT INTO _sirannon_sync_state (table_name, status, row_count, pk_hash, completed_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(table_name) DO UPDATE SET
         status = excluded.status,
         row_count = COALESCE(excluded.row_count, row_count),
         pk_hash = COALESCE(excluded.pk_hash, pk_hash),
         completed_at = excluded.completed_at`,
    )
    await stmt.run(table, status, rowCount ?? 0, pkHash ?? '', status === 'completed' ? Date.now() / 1000 : null)
  }

  async setSyncMeta(phase: SyncPhase, snapshotSeq?: bigint, sourcePeerId?: string, requestId?: string): Promise<void> {
    const stmt = await this.conn.prepare(
      `INSERT INTO _sirannon_sync_state (table_name, status, snapshot_seq, source_peer_id, started_at, request_id)
       VALUES ('__sync_meta__', ?, ?, ?, ?, ?)
       ON CONFLICT(table_name) DO UPDATE SET
         status = excluded.status,
         snapshot_seq = COALESCE(excluded.snapshot_seq, snapshot_seq),
         source_peer_id = COALESCE(excluded.source_peer_id, source_peer_id),
         started_at = COALESCE(excluded.started_at, started_at),
         request_id = COALESCE(excluded.request_id, request_id)`,
    )
    await stmt.run(
      phase,
      snapshotSeq !== undefined ? snapshotSeq.toString() : null,
      sourcePeerId ?? null,
      phase === 'syncing' ? Date.now() / 1000 : null,
      requestId ?? null,
    )
  }

  async getSyncState(): Promise<{
    phase: SyncPhase
    completedTables: string[]
    snapshotSeq: bigint | null
    sourcePeerId: string | null
  }> {
    const metaStmt = await this.conn.prepare(
      `SELECT status, snapshot_seq, source_peer_id FROM _sirannon_sync_state WHERE table_name = '__sync_meta__'`,
    )
    const meta = (await metaStmt.get()) as
      | { status: string; snapshot_seq: number | null; source_peer_id: string | null }
      | undefined

    if (!meta) {
      return { phase: 'ready', completedTables: [], snapshotSeq: null, sourcePeerId: null }
    }

    const completedStmt = await this.conn.prepare(
      `SELECT table_name FROM _sirannon_sync_state WHERE table_name != '__sync_meta__' AND status = 'completed'`,
    )
    const completedRows = (await completedStmt.all()) as Array<{ table_name: string }>

    return {
      phase: meta.status as SyncPhase,
      completedTables: completedRows.map(r => r.table_name),
      snapshotSeq: meta.snapshot_seq !== null ? BigInt(meta.snapshot_seq) : null,
      sourcePeerId: meta.source_peer_id,
    }
  }

  async isSyncCompleted(): Promise<boolean> {
    const stmt = await this.conn.prepare(`SELECT status FROM _sirannon_sync_state WHERE table_name = '__sync_meta__'`)
    const row = (await stmt.get()) as { status: string } | undefined
    return row?.status === 'ready'
  }
}

function isWellFormedHlc(candidate: string): boolean {
  if (candidate.length === 0) return false
  let decoded: ReturnType<typeof HLC.decode>
  try {
    decoded = HLC.decode(candidate)
  } catch {
    return false
  }
  if (!Number.isInteger(decoded.wallMs) || decoded.wallMs < 0) return false
  if (!Number.isInteger(decoded.logical) || decoded.logical < 0) return false
  if (decoded.nodeId.length === 0) return false
  return true
}

function mergeCandidate(current: string | null, candidate: string | null): string | null {
  if (candidate === null || candidate.length === 0) return current
  if (!isWellFormedHlc(candidate)) return current
  if (current === null) return candidate
  return HLC.compare(candidate, current) > 0 ? candidate : current
}
