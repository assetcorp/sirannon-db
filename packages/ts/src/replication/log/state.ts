import type { SQLiteConnection } from '../../core/driver/types.js'
import { HLC } from '../../core/sync/hlc.js'
import { isWellFormedHlc } from '../../core/sync/hlc-store.js'
import {
  selectCompletedSyncTableNames,
  selectMaxAppliedSourceSeq,
  selectMaxChangeHlc,
  selectMaxChangeSeq,
  selectMaxColumnVersionHlc,
  selectMinPeerAckedSeq,
  selectPeerAckedSeq,
  selectSyncMetaRow,
  upsertPeerAckedSeq,
  upsertSyncMeta,
  upsertSyncTableStatus,
} from '../../core/system-catalog/index.js'
import type { SyncPhase } from '../types.js'

export class StateOps {
  private readonly activeSyncSnapshotSeqs = new Set<bigint>()

  constructor(private readonly conn: SQLiteConnection) {}

  async recoverMaxObservedHlc(changesTable: string): Promise<string | null> {
    let best: string | null = null

    best = mergeCandidate(best, await selectMaxChangeHlc(this.conn, changesTable))
    best = mergeCandidate(best, await selectMaxColumnVersionHlc(this.conn))

    return best
  }

  getLastAppliedSeq(fromNodeId: string): Promise<bigint> {
    return selectMaxAppliedSourceSeq(this.conn, fromNodeId)
  }

  setLastAppliedSeq(fromNodeId: string, seq: bigint): Promise<void> {
    return upsertPeerAckedSeq(this.conn, fromNodeId, seq, Date.now() / 1000)
  }

  getPeerAckedSeq(peerNodeId: string): Promise<bigint> {
    return selectPeerAckedSeq(this.conn, peerNodeId)
  }

  async getLocalSeq(changesTable: string): Promise<bigint> {
    return selectMaxChangeSeq(this.conn, changesTable)
  }

  async getMinAckedSeq(): Promise<bigint | null> {
    let result = await selectMinPeerAckedSeq(this.conn)

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

  setSyncTableStatus(table: string, status: string, rowCount?: number, pkHash?: string): Promise<void> {
    return upsertSyncTableStatus(this.conn, {
      tableName: table,
      status,
      rowCount: rowCount ?? 0,
      pkHash: pkHash ?? '',
      completedAt: status === 'completed' ? Date.now() / 1000 : null,
    })
  }

  setSyncMeta(phase: SyncPhase, snapshotSeq?: bigint, sourcePeerId?: string, requestId?: string): Promise<void> {
    return upsertSyncMeta(this.conn, {
      status: phase,
      snapshotSeq,
      sourcePeerId,
      startedAt: phase === 'syncing' ? Date.now() / 1000 : null,
      requestId,
    })
  }

  async getSyncState(): Promise<{
    phase: SyncPhase
    completedTables: string[]
    snapshotSeq: bigint | null
    sourcePeerId: string | null
  }> {
    const meta = await selectSyncMetaRow(this.conn)

    if (!meta) {
      return { phase: 'ready', completedTables: [], snapshotSeq: null, sourcePeerId: null }
    }

    return {
      phase: meta.status as SyncPhase,
      completedTables: await selectCompletedSyncTableNames(this.conn),
      snapshotSeq: meta.snapshotSeq,
      sourcePeerId: meta.sourcePeerId,
    }
  }

  async isSyncCompleted(): Promise<boolean> {
    const meta = await selectSyncMetaRow(this.conn)
    return meta?.status === 'ready'
  }
}

function mergeCandidate(current: string | null, candidate: string | null): string | null {
  if (candidate === null || candidate.length === 0) return current
  if (!isWellFormedHlc(candidate)) return current
  if (current === null) return candidate
  return HLC.compare(candidate, current) > 0 ? candidate : current
}
