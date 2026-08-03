import type { SQLiteConnection } from '../../core/driver/types.js'
import type { SyncAck, SyncState } from '../types.js'
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
}

export interface SyncAckWaiter {
  resolve: (ack: SyncAck) => void
  timer: ReturnType<typeof setTimeout>
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
