import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { DeterministicScheduler } from '../scheduler.js'

const T0 = 1_000_000

describe('DeterministicScheduler', () => {
  let scheduler: DeterministicScheduler

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0)
    scheduler = new DeterministicScheduler(T0)
  })

  afterEach(() => {
    scheduler.dispose()
    vi.useRealTimers()
  })

  it('starts with empty queue', () => {
    expect(scheduler.isEmpty).toBe(true)
    expect(scheduler.pendingCount).toBe(0)
    expect(scheduler.now).toBe(T0)
  })

  it('enqueues and delivers a single event', async () => {
    const delivered: string[] = []
    scheduler.setDeliverFn(async event => {
      delivered.push(event.kind)
    })

    scheduler.enqueue({ deliverAt: T0 + 10, from: 'a', to: 'b', kind: 'batch', payload: null })
    expect(scheduler.pendingCount).toBe(1)

    const stepped = await scheduler.step()
    expect(stepped).toBe(true)
    expect(delivered).toEqual(['batch'])
    expect(scheduler.isEmpty).toBe(true)
    expect(scheduler.now).toBe(T0 + 10)
  })

  it('delivers events in time order', async () => {
    const order: number[] = []
    scheduler.setDeliverFn(async event => {
      order.push(event.deliverAt)
    })

    scheduler.enqueue({ deliverAt: T0 + 30, from: 'a', to: 'b', kind: 'c', payload: null })
    scheduler.enqueue({ deliverAt: T0 + 10, from: 'a', to: 'b', kind: 'a', payload: null })
    scheduler.enqueue({ deliverAt: T0 + 20, from: 'a', to: 'b', kind: 'b', payload: null })

    await scheduler.step()
    await scheduler.step()
    await scheduler.step()

    expect(order).toEqual([T0 + 10, T0 + 20, T0 + 30])
  })

  it('breaks ties by insertion order (event id)', async () => {
    const order: string[] = []
    scheduler.setDeliverFn(async event => {
      order.push(event.kind)
    })

    scheduler.enqueue({ deliverAt: T0 + 5, from: 'a', to: 'b', kind: 'first', payload: null })
    scheduler.enqueue({ deliverAt: T0 + 5, from: 'a', to: 'b', kind: 'second', payload: null })
    scheduler.enqueue({ deliverAt: T0 + 5, from: 'a', to: 'b', kind: 'third', payload: null })

    await scheduler.step()
    await scheduler.step()
    await scheduler.step()

    expect(order).toEqual(['first', 'second', 'third'])
  })

  it('advances fake timers to event time', async () => {
    scheduler.setDeliverFn(async () => {})
    scheduler.enqueue({ deliverAt: T0 + 100, from: 'a', to: 'b', kind: 'x', payload: null })

    await scheduler.step()
    expect(Date.now()).toBe(T0 + 100)
    expect(scheduler.now).toBe(T0 + 100)
  })

  it('step returns false on empty queue', async () => {
    const result = await scheduler.step()
    expect(result).toBe(false)
  })

  it('runUntilQuiet drains all events', async () => {
    let count = 0
    scheduler.setDeliverFn(async () => {
      count++
    })

    for (let i = 0; i < 10; i++) {
      scheduler.enqueue({ deliverAt: T0 + i * 5, from: 'a', to: 'b', kind: 'x', payload: null })
    }

    await scheduler.runUntilQuiet(100, 3, 10)
    expect(count).toBe(10)
    expect(scheduler.isEmpty).toBe(true)
  })

  it('runUntilQuiet handles events enqueued during delivery', async () => {
    let delivered = 0
    scheduler.setDeliverFn(async event => {
      delivered++
      if (delivered < 5) {
        scheduler.enqueue({
          deliverAt: event.deliverAt + 10,
          from: 'a',
          to: 'b',
          kind: 'chain',
          payload: null,
        })
      }
    })

    scheduler.enqueue({ deliverAt: T0 + 1, from: 'a', to: 'b', kind: 'start', payload: null })
    await scheduler.runUntilQuiet(100, 3, 10)
    expect(delivered).toBe(5)
  })

  it('runUntilQuiet detects quiescence via idle ticks', async () => {
    let timerCallbackCount = 0
    setTimeout(() => {
      timerCallbackCount++
    }, 30)

    scheduler.setDeliverFn(async () => {})
    await scheduler.runUntilQuiet(100, 2, 30)

    expect(timerCallbackCount).toBe(1)
  })

  it('advanceBy moves time forward', async () => {
    await scheduler.advanceBy(500)
    expect(scheduler.now).toBe(T0 + 500)
    expect(Date.now()).toBe(T0 + 500)
  })

  it('dispose rejects pending forward promises', async () => {
    let rejection: Error | undefined

    const promise = new Promise<unknown>((resolve, reject) => {
      scheduler.enqueue({
        deliverAt: T0 + 100,
        from: 'a',
        to: 'b',
        kind: 'forward_request',
        payload: null,
        resolve,
        reject,
      })
    })

    promise.catch(err => {
      rejection = err as Error
    })

    scheduler.dispose()
    await vi.advanceTimersByTimeAsync(0)

    expect(rejection).toBeDefined()
    expect(rejection?.message).toContain('disposed')
  })

  it('does not enqueue after disposal', () => {
    scheduler.dispose()
    const id = scheduler.enqueue({ deliverAt: T0 + 10, from: 'a', to: 'b', kind: 'x', payload: null })
    expect(id).toBe(-1)
    expect(scheduler.isEmpty).toBe(true)
  })

  it('runWithDrain resolves fn concurrently with event processing', async () => {
    const events: string[] = []
    scheduler.setDeliverFn(async event => {
      events.push(event.kind)
      if (event.kind === 'request' && event.resolve) {
        scheduler.enqueue({
          deliverAt: scheduler.now + 5,
          from: 'b',
          to: 'a',
          kind: 'response',
          payload: { originalResolve: event.resolve, data: 'result-data' },
        })
      }
      if (event.kind === 'response') {
        const p = event.payload as { originalResolve: (v: unknown) => void; data: unknown }
        p.originalResolve(p.data)
      }
    })

    let fnResult: unknown = null

    await scheduler.runWithDrain(async () => {
      fnResult = await new Promise<unknown>((resolve, reject) => {
        scheduler.enqueue({
          deliverAt: scheduler.now + 10,
          from: 'a',
          to: 'b',
          kind: 'request',
          payload: null,
          resolve,
          reject,
        })
      })
    })

    expect(events).toContain('request')
    expect(events).toContain('response')
    expect(fnResult).toBe('result-data')
  })
})
