import type { ConflictResolver } from '../core/sync/types.js'
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
  immediateAckAfterChanges?: number
  resolver?: ConflictResolver | ((table: string) => ConflictResolver)
  onChange?: (event: ChangeEvent) => void
  onResyncRequired?: () => void
  onSnapshotProgress?: (progress: SnapshotProgress) => void
  onSnapshotComplete?: (outcome: SnapshotOutcome) => void
}

export type SnapshotOutcome =
  | { ok: true; error: null; databaseUsable: true; retrying: false }
  | { ok: false; error: { code: string; message: string }; databaseUsable: boolean; retrying: boolean }

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
