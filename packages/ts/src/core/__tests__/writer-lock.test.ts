import { describe, expect, it } from 'vitest'
import { nodeWriterContext } from '../../drivers/node-runtime.js'
import { WriterLock } from '../writer-lock.js'

describe('WriterLock', () => {
  it('runs queued operations one at a time in order', async () => {
    const lock = new WriterLock()
    const events: string[] = []

    const makeOp = (name: string, delayTicks: number) => async () => {
      events.push(`start:${name}`)
      for (let i = 0; i < delayTicks; i++) {
        await Promise.resolve()
      }
      events.push(`end:${name}`)
      return name
    }

    const results = await Promise.all([lock.run(makeOp('a', 3)), lock.run(makeOp('b', 1)), lock.run(makeOp('c', 0))])

    expect(results).toEqual(['a', 'b', 'c'])
    expect(events).toEqual(['start:a', 'end:a', 'start:b', 'end:b', 'start:c', 'end:c'])
  })

  it('surfaces a rejection to its caller without blocking later operations', async () => {
    const lock = new WriterLock()
    const order: string[] = []

    const failing = lock.run(async () => {
      order.push('failing')
      throw new Error('boom')
    })
    const following = lock.run(async () => {
      order.push('following')
      return 'ok'
    })

    await expect(failing).rejects.toThrow('boom')
    await expect(following).resolves.toBe('ok')
    expect(order).toEqual(['failing', 'following'])
  })

  it('runs a re-entrant call inline instead of deadlocking when the runtime tracks context', async () => {
    const lock = new WriterLock(nodeWriterContext())

    const result = await lock.run(async () => {
      const inner = await lock.run(async () => 'inner')
      return `outer:${inner}`
    })

    expect(result).toBe('outer:inner')
  })

  it('keeps an outsider out of the held operation when the runtime tracks context', async () => {
    const lock = new WriterLock(nodeWriterContext())
    const order: string[] = []

    const held = lock.run(async () => {
      order.push('held:start')
      await Promise.resolve()
      order.push('held:end')
    })
    const outsider = lock.run(async () => {
      order.push('outsider')
    })

    await Promise.all([held, outsider])
    expect(order).toEqual(['held:start', 'held:end', 'outsider'])
  })

  it('treats every caller as an outsider when the runtime cannot track context', async () => {
    const lock = new WriterLock()
    expect(lock.isHeld()).toBe(false)

    await lock.run(async () => {
      expect(lock.isHeld()).toBe(false)
    })
  })

  it('does not start the next operation until the current one settles', async () => {
    const lock = new WriterLock()
    let firstRunning = false
    let overlapDetected = false

    const first = lock.run(async () => {
      firstRunning = true
      await Promise.resolve()
      await Promise.resolve()
      firstRunning = false
    })
    const second = lock.run(async () => {
      if (firstRunning) overlapDetected = true
    })

    await Promise.all([first, second])
    expect(overlapDetected).toBe(false)
  })
})
