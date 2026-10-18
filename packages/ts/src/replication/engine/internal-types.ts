import type { SQLiteConnection } from '../../core/driver/types.js'
import type { SyncAck, SyncHistoryPoint, SyncPhase, SyncState } from '../types.js'
import type { TableStreamDigest } from './sync-verification.js'

export interface ActiveSyncSession {
  requestId: string
  joinerNodeId: string
  readConn: SQLiteConnection
  snapshotSeq: bigint
  tables: string[]
  totalTables: number
  completedTables: Set<string>
  startedAt: number
  timeoutTimer: ReturnType<typeof setTimeout>
  aborted: boolean
  streamVerification: boolean
  tableDigests: Map<string, TableStreamDigest>
  historyPoint: SyncHistoryPoint | undefined
}

export interface SyncAckWaiter {
  resolve: (ack: SyncAck) => void
  timer: ReturnType<typeof setTimeout>
}

/**
 * Returns true when a node in the given sync phase holds a complete copy of the group's data and serves it.
 *
 * @param phase - The node's sync phase.
 * @returns True for the `ready` and `catching-up` phases.
 *
 * @internal
 */
export function servesItsData(phase: SyncPhase): boolean {
  return phase === 'ready' || phase === 'catching-up'
}

export function initialSyncState(): SyncState {
  return {
    phase: 'ready',
    sourcePeerId: null,
    snapshotSeq: null,
    completedTables: [],
    totalTables: 0,
    startedAt: null,
    error: null,
  }
}
