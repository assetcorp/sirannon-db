import type { ChangeEvent } from '../core/types.js'
import type { SnapshotProgress } from './snapshot-loader.js'

export interface SyncControllerOptions {
  url: string
  databaseId: string
  tables: readonly string[]
  headers?: Record<string, string>
  batchSize?: number
  pushIntervalMs?: number
  ackIntervalMs?: number
  maxPushRetryDelayMs?: number
  requestTimeout?: number
  autoResync?: boolean
  snapshotRetryDelayMs?: number
  maxSnapshotRetryDelayMs?: number
  snapshotPageSize?: number
  onChange?: (event: ChangeEvent) => void
  onResyncRequired?: () => void
  onSnapshotProgress?: (progress: SnapshotProgress) => void
}

export type SyncState = 'stopped' | 'starting' | 'running' | 'paused' | 'snapshotting'

export interface SnapshotOptions {
  pageSize?: number
  onProgress?: (progress: SnapshotProgress) => void
}

export interface SyncStatus {
  state: SyncState
  deviceId: string | null
  serverCapabilities: string[] | null
  schemaVersion: number | null
  pendingPushCount: number
  lastPushedSeq: bigint
  lastPulledSeq: bigint | null
  pushCaughtUp: boolean
  resyncRequired: boolean
  lastError: { code: string; message: string } | null
}
