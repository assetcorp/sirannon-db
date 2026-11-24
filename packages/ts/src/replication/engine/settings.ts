import type { ReplicationConfig } from '../types.js'
import {
  DEFAULT_ACK_TIMEOUT_MS,
  DEFAULT_BATCH_INTERVAL_MS,
  DEFAULT_BATCH_SIZE,
  DEFAULT_CATCH_UP_DEADLINE_MS,
  DEFAULT_MAX_BATCH_CHANGES,
  DEFAULT_MAX_CLOCK_DRIFT_MS,
  DEFAULT_MAX_CONCURRENT_SYNCS,
  DEFAULT_MAX_PENDING_BATCHES,
  DEFAULT_MAX_SYNC_DURATION_MS,
  DEFAULT_MAX_SYNC_LAG_BEFORE_READY,
  DEFAULT_SYNC_ACK_TIMEOUT_MS,
  DEFAULT_SYNC_BATCH_SIZE,
} from './constants.js'

/**
 * The batching, acknowledgement, and first-sync settings of one replication engine, with each default applied.
 *
 * @internal
 */
export interface EngineSettings {
  batchSize: number
  batchIntervalMs: number
  maxClockDriftMs: number
  maxPendingBatches: number
  maxBatchChanges: number
  ackTimeoutMs: number
  initialSync: boolean
  syncBatchSize: number
  maxConcurrentSyncs: number
  maxSyncDurationMs: number
  maxSyncLagBeforeReady: number
  syncAckTimeoutMs: number
  catchUpDeadlineMs: number
}

/**
 * Returns the engine's settings from its configuration, with the default for each setting that the caller left out.
 *
 * @param config - The replication configuration.
 * @returns The settings.
 * @internal
 */
export function engineSettings(config: ReplicationConfig): EngineSettings {
  return {
    batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
    batchIntervalMs: config.batchIntervalMs ?? DEFAULT_BATCH_INTERVAL_MS,
    maxClockDriftMs: config.maxClockDriftMs ?? DEFAULT_MAX_CLOCK_DRIFT_MS,
    maxPendingBatches: config.maxPendingBatches ?? DEFAULT_MAX_PENDING_BATCHES,
    maxBatchChanges: config.maxBatchChanges ?? DEFAULT_MAX_BATCH_CHANGES,
    ackTimeoutMs: config.ackTimeoutMs ?? DEFAULT_ACK_TIMEOUT_MS,
    initialSync: config.initialSync ?? true,
    syncBatchSize: config.syncBatchSize ?? DEFAULT_SYNC_BATCH_SIZE,
    maxConcurrentSyncs: config.maxConcurrentSyncs ?? DEFAULT_MAX_CONCURRENT_SYNCS,
    maxSyncDurationMs: config.maxSyncDurationMs ?? DEFAULT_MAX_SYNC_DURATION_MS,
    maxSyncLagBeforeReady: config.maxSyncLagBeforeReady ?? DEFAULT_MAX_SYNC_LAG_BEFORE_READY,
    syncAckTimeoutMs: config.syncAckTimeoutMs ?? DEFAULT_SYNC_ACK_TIMEOUT_MS,
    catchUpDeadlineMs: config.catchUpDeadlineMs ?? DEFAULT_CATCH_UP_DEADLINE_MS,
  }
}
