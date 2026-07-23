import type { SQLiteConnection } from '../../core/driver/types.js'
import type { SyncAck } from '../types.js'
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
