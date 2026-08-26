import type { Database } from '@delali/sirannon-db'
import { toSubprotocolCredential } from '@delali/sirannon-db'
import {
  type SnapshotOutcome,
  type SnapshotProgress,
  SyncController,
  type SyncStatus,
} from '@delali/sirannon-db/client'
import { DATABASE_ID, WORK_ORDERS_TABLE } from '../schema'
import { DEFAULT_DEVICE_TOKEN, DEVICE_AUTH_PROTOCOL_PREFIX } from './demo-config'

export interface DeviceSessionHooks {
  onStatusChange: (status: SyncStatus) => void
  onServerChange: () => void
  onResyncRequired: () => void
  onSnapshotProgress: (progress: SnapshotProgress) => void
  onSnapshotComplete: (outcome: SnapshotOutcome) => void
}

export function createSyncController(db: Database, serverUrl: string, hooks: DeviceSessionHooks): SyncController {
  const deviceToken = import.meta.env.VITE_SIRANNON_DEVICE_TOKEN ?? DEFAULT_DEVICE_TOKEN

  return new SyncController(db, {
    url: serverUrl,
    databaseId: DATABASE_ID,
    tables: [WORK_ORDERS_TABLE],
    headers: { Authorization: `Bearer ${deviceToken}` },
    webSocketProtocols: [toSubprotocolCredential(DEVICE_AUTH_PROTOCOL_PREFIX, deviceToken)],
    pushIntervalMs: 500,
    onStatusChange: hooks.onStatusChange,
    onChange: hooks.onServerChange,
    onResyncRequired: hooks.onResyncRequired,
    onSnapshotProgress: hooks.onSnapshotProgress,
    onSnapshotComplete: hooks.onSnapshotComplete,
  })
}
