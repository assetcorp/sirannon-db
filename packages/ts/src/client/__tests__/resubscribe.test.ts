import { describe, expect, it } from 'vitest'
import type { ActiveSubscription } from '../transport/ws-subscription-state.js'
import { buildResubscribeMessage } from '../transport/ws-subscription-state.js'

function activeSubscription(overrides: Partial<ActiveSubscription>): ActiveSubscription {
  return {
    table: 'notes',
    filter: undefined,
    callback: () => {},
    onError: undefined,
    onReset: undefined,
    onSubscribed: undefined,
    deviceId: undefined,
    tables: undefined,
    schemaVersion: undefined,
    lastSeq: undefined,
    resumeSeq: undefined,
    epoch: undefined,
    stagedStream: undefined,
    ...overrides,
  }
}

describe('buildResubscribeMessage', () => {
  it('resumes a device from the sequence it has applied, not the one it received', () => {
    const sub = activeSubscription({
      deviceId: 'a'.repeat(32),
      lastSeq: 90n,
      resumeSeq: () => 42n,
    })

    const msg = buildResubscribeMessage('sub-1', sub)

    expect(msg).toMatchObject({ type: 'subscribe', sinceSeq: '42' })
  })

  it('carries the whole table set so a reconnect keeps syncing every table', () => {
    const sub = activeSubscription({
      deviceId: 'a'.repeat(32),
      tables: ['notes', 'tags'],
      resumeSeq: () => undefined,
    })

    const msg = buildResubscribeMessage('sub-1', sub)

    expect(msg).toMatchObject({ tables: ['notes', 'tags'] })
    expect(msg).not.toHaveProperty('sinceSeq')
  })

  it('falls back to the received cursor for a plain subscription', () => {
    const sub = activeSubscription({ lastSeq: 7n })

    expect(buildResubscribeMessage('sub-1', sub)).toMatchObject({ sinceSeq: '7' })
  })

  it('keeps the staged-stream declaration across a reconnect', () => {
    const sub = activeSubscription({
      deviceId: 'a'.repeat(32),
      stagedStream: true,
      resumeSeq: () => 42n,
    })

    expect(buildResubscribeMessage('sub-1', sub)).toMatchObject({ stagedStream: true, sinceSeq: '42' })
    expect(buildResubscribeMessage('sub-2', activeSubscription({}))).not.toHaveProperty('stagedStream')
  })
})
