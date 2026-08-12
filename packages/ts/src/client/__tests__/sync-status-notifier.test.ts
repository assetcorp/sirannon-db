import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { SyncState, SyncStatus } from '../sync-controller-types.js'
import { SyncStatusNotifier } from '../sync-status-notifier.js'

function statusFor(state: SyncState, pendingPushCount = 0): SyncStatus {
  return {
    state,
    deviceId: 'device-a',
    serverCapabilities: null,
    schemaVersion: 1,
    pendingPushCount,
    lastPushedSeq: 0n,
    lastPulledSeq: null,
    pushCaughtUp: pendingPushCount === 0,
    resyncRequired: false,
    lastError: null,
  }
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('a sync controller with no status listener', () => {
  it('captures nothing and counts nothing', async () => {
    const capture = vi.fn(() => statusFor('running'))
    const refresh = vi.fn(async () => false)
    const notifier = new SyncStatusNotifier(undefined, capture, refresh)

    notifier.notify()
    notifier.notify()
    await vi.advanceTimersByTimeAsync(1_000)

    expect(capture).not.toHaveBeenCalled()
    expect(refresh).not.toHaveBeenCalled()
  })
})

describe('a sync controller reporting its status', () => {
  it('delivers every transition in order, however short-lived', async () => {
    const seen: SyncState[] = []
    let current: SyncState = 'starting'
    const notifier = new SyncStatusNotifier(
      status => seen.push(status.state),
      () => statusFor(current),
      async () => false,
    )

    notifier.notify()
    current = 'snapshotting'
    notifier.notify()
    current = 'running'
    notifier.notify()
    await vi.advanceTimersByTimeAsync(0)

    expect(seen).toEqual(['starting', 'snapshotting', 'running'])
  })

  it('reads the outbox at most once every hundred milliseconds', async () => {
    const refresh = vi.fn(async () => false)
    const notifier = new SyncStatusNotifier(
      () => {},
      () => statusFor('running'),
      refresh,
    )

    notifier.notify()
    await vi.advanceTimersByTimeAsync(0)
    expect(refresh).toHaveBeenCalledTimes(1)

    for (let i = 0; i < 50; i += 1) {
      notifier.notify()
    }
    await vi.advanceTimersByTimeAsync(99)
    expect(refresh).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(1)
    expect(refresh).toHaveBeenCalledTimes(2)
  })

  it('raises a further status when the pending push count changes', async () => {
    const counts: number[] = []
    let pending = 0
    let refreshes = 0
    const notifier = new SyncStatusNotifier(
      status => counts.push(status.pendingPushCount),
      () => statusFor('running', pending),
      async () => {
        refreshes += 1
        if (refreshes > 1) return false
        pending = 3
        return true
      },
    )

    notifier.notify()
    await vi.advanceTimersByTimeAsync(200)

    expect(counts).toEqual([0, 3])
  })

  it('runs no timer once the controller settles', async () => {
    const notifier = new SyncStatusNotifier(
      () => {},
      () => statusFor('running'),
      async () => false,
    )

    notifier.notify()
    await vi.advanceTimersByTimeAsync(1_000)

    expect(vi.getTimerCount()).toBe(0)
  })

  it('keeps delivering after an outbox read fails', async () => {
    const seen: SyncState[] = []
    const notifier = new SyncStatusNotifier(
      status => seen.push(status.state),
      () => statusFor('running'),
      async () => {
        throw new Error('the database is closed')
      },
    )

    notifier.notify()
    await vi.advanceTimersByTimeAsync(0)
    notifier.notify()
    await vi.advanceTimersByTimeAsync(200)

    expect(seen).toEqual(['running', 'running'])
  })

  it('keeps delivering after the listener throws', async () => {
    const seen: SyncState[] = []
    const notifier = new SyncStatusNotifier(
      status => {
        seen.push(status.state)
        throw new Error('the application listener failed')
      },
      () => statusFor('running'),
      async () => false,
    )

    notifier.notify()
    notifier.notify()
    await vi.advanceTimersByTimeAsync(0)

    expect(seen).toEqual(['running', 'running'])
  })
})
