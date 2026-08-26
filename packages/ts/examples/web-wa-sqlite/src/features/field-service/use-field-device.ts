import type { SyncController } from '@delali/sirannon-db/client'
import { useCallback, useEffect, useState, useSyncExternalStore } from 'react'
import { browserOnly } from '../../lib/app-mode'
import { acquireDeviceTabLock, rememberDevice } from '../../lib/device-registry'
import { closeFieldDevice, type FieldDevice, openFieldDevice } from '../../lib/field-device'
import { createSyncStore, type DeviceSyncView, getInitialSyncView, type SyncStore } from '../../lib/sync-store'

const SERVER_URL: string | null = browserOnly ? null : (import.meta.env.VITE_SIRANNON_URL ?? 'http://127.0.0.1:9876')
const START_RETRY_INTERVAL_MS = 5_000

export type DevicePhase = 'opening' | 'locked' | 'ready' | 'failed'

interface DeviceSessionState {
  phase: DevicePhase
  device: FieldDevice | null
  store: SyncStore | null
  openError: string | null
}

const OPENING_STATE: DeviceSessionState = { phase: 'opening', device: null, store: null, openError: null }

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function noopSubscribe(): () => void {
  return () => {}
}

function watchForFirstSnapshot(store: SyncStore, sync: SyncController): void {
  let stopWatching: (() => void) | null = null
  let started = false
  stopWatching = store.subscribe(() => {
    if (started) return
    const status = store.getSnapshot().status
    if (status === null || status.state !== 'running' || !status.pushCaughtUp || status.pendingPushCount > 0) {
      return
    }
    started = true
    stopWatching?.()
    store.patch({ snapshotting: true })
    void sync.downloadSnapshot().catch(() => undefined)
  })
}

export function useFieldDevice(name: string) {
  const [state, setState] = useState<DeviceSessionState>(OPENING_STATE)
  const [wantsOnline, setWantsOnline] = useState(true)

  useEffect(() => {
    let cancelled = false
    let device: FieldDevice | null = null
    let releaseLock: (() => void) | null = null
    const store = createSyncStore()

    setState(OPENING_STATE)
    setWantsOnline(true)

    const open = async () => {
      const lock = await acquireDeviceTabLock(name)
      releaseLock = lock.release
      if (cancelled) {
        lock.release()
        return
      }
      if (!lock.acquired) {
        setState({ phase: 'locked', device: null, store: null, openError: null })
        return
      }

      try {
        device = await openFieldDevice(name, SERVER_URL, {
          onStatusChange: status => store.patch({ status }),
          onServerChange: () => store.countChange(),
          onResyncRequired: () => store.patch({ snapshotting: true }),
          onSnapshotProgress: progress => store.patch({ snapshotting: true, snapshotProgress: progress }),
          onSnapshotComplete: outcome =>
            store.patch({
              snapshotProgress: null,
              snapshotting: !outcome.ok && outcome.retrying,
              bannerError: outcome.ok
                ? null
                : `${outcome.error.message}${outcome.retrying ? ' Retrying shortly.' : ''}`,
            }),
        })
      } catch (err) {
        if (!cancelled) {
          setState({ phase: 'failed', device: null, store: null, openError: errorMessage(err) })
        }
        lock.release()
        return
      }

      if (cancelled) {
        void closeFieldDevice(device)
        lock.release()
        return
      }

      rememberDevice(name)
      const sync = device.sync
      if (sync !== null && device.neverSynced) {
        watchForFirstSnapshot(store, sync)
      }
      setState({ phase: 'ready', device, store, openError: null })

      if (sync === null) return

      try {
        await sync.start()
      } catch (err) {
        store.patch({ bannerError: errorMessage(err) })
      }
    }

    void open()

    return () => {
      cancelled = true
      if (device !== null) {
        void closeFieldDevice(device)
      }
      releaseLock?.()
    }
  }, [name])

  const view: DeviceSyncView = useSyncExternalStore(
    state.store?.subscribe ?? noopSubscribe,
    state.store?.getSnapshot ?? getInitialSyncView,
    getInitialSyncView,
  )

  const { device, store } = state

  const setSyncEnabled = useCallback(
    (enabled: boolean) => {
      if (device === null || store === null) return
      const sync = device.sync
      if (sync === null) return
      setWantsOnline(enabled)
      const run = async () => {
        try {
          if (enabled) {
            const syncState = store.getSnapshot().status?.state
            if (syncState === 'paused') {
              await sync.resume()
            } else if (syncState === 'stopped' || syncState === undefined) {
              await sync.start()
            }
            store.patch({ bannerError: null })
          } else {
            sync.pause()
          }
        } catch (err) {
          store.patch({ bannerError: errorMessage(err) })
        }
      }
      void run()
    },
    [device, store],
  )

  useEffect(() => {
    if (!wantsOnline || device === null || store === null) return
    const sync = device.sync
    if (sync === null) return
    const id = window.setInterval(() => {
      if (store.getSnapshot().status?.state !== 'stopped') return
      void sync
        .start()
        .then(() => store.patch({ bannerError: null }))
        .catch(() => undefined)
    }, START_RETRY_INTERVAL_MS)
    return () => {
      window.clearInterval(id)
    }
  }, [wantsOnline, device, store])

  const dismissBanner = useCallback(() => {
    store?.patch({ bannerError: null })
  }, [store])

  const reportError = useCallback(
    (err: unknown) => {
      store?.patch({ bannerError: errorMessage(err) })
    },
    [store],
  )

  return {
    phase: state.phase,
    device,
    view,
    wantsOnline,
    setSyncEnabled,
    dismissBanner,
    reportError,
    openError: state.openError,
  }
}
