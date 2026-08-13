import type { SnapshotProgress, SyncStatus } from '@delali/sirannon-db/client'

export interface DeviceSyncView {
  status: SyncStatus | null
  changesReceived: number
  snapshotting: boolean
  snapshotProgress: SnapshotProgress | null
  bannerError: string | null
}

const INITIAL_VIEW: DeviceSyncView = {
  status: null,
  changesReceived: 0,
  snapshotting: false,
  snapshotProgress: null,
  bannerError: null,
}

export interface SyncStore {
  subscribe: (listener: () => void) => () => void
  getSnapshot: () => DeviceSyncView
  patch: (changes: Partial<DeviceSyncView>) => void
  countChange: () => void
}

export function getInitialSyncView(): DeviceSyncView {
  return INITIAL_VIEW
}

export function createSyncStore(): SyncStore {
  let view = INITIAL_VIEW
  const listeners = new Set<() => void>()

  const notify = () => {
    for (const listener of listeners) {
      listener()
    }
  }

  return {
    subscribe: listener => {
      listeners.add(listener)
      return () => {
        listeners.delete(listener)
      }
    },
    getSnapshot: () => view,
    patch: changes => {
      view = { ...view, ...changes }
      notify()
    },
    countChange: () => {
      view = { ...view, changesReceived: view.changesReceived + 1 }
      notify()
    },
  }
}
