import type { SQLiteConnection } from '../../core/driver/types.js'
import type { SyncAck } from '../types.js'

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
}

export interface SyncAckWaiter {
  resolve: (ack: SyncAck) => void
  timer: ReturnType<typeof setTimeout>
}
