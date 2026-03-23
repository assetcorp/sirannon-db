import { vi } from 'vitest'
import { TransportError } from '../../replication/errors.js'

export interface ScheduledEvent {
  id: number
  deliverAt: number
  from: string
  to: string
  kind: string
  payload: unknown
  resolve?: (value: unknown) => void
  reject?: (err: Error) => void
}

type DeliverFn = (event: ScheduledEvent) => Promise<void>

export class DeterministicScheduler {
  private readonly heap: ScheduledEvent[] = []
  private nextId = 0
  private currentTime: number
  private deliverFn: DeliverFn | null = null
  private disposed = false
  private readonly pendingForwards = new Set<ScheduledEvent>()

  constructor(initialTime: number) {
    this.currentTime = initialTime
  }

  get now(): number {
    return this.currentTime
  }

  get pendingCount(): number {
    return this.heap.length
  }

  get isEmpty(): boolean {
    return this.heap.length === 0
  }

  setDeliverFn(fn: DeliverFn): void {
    this.deliverFn = fn
  }

  enqueue(event: Omit<ScheduledEvent, 'id'>): number {
    if (this.disposed) return -1
    const id = this.nextId++
    const full: ScheduledEvent = { ...event, id }
    if (full.resolve || full.reject) {
      this.pendingForwards.add(full)
    }
    this.heapPush(full)
    return id
  }

  async step(): Promise<boolean> {
    if (this.disposed || this.heap.length === 0) return false

    const event = this.heapPop()
    if (!event) return false

    if (event.deliverAt > this.currentTime) {
      const delta = event.deliverAt - this.currentTime
      this.currentTime = event.deliverAt
      await vi.advanceTimersByTimeAsync(delta)
    }

    this.pendingForwards.delete(event)

    if (this.deliverFn) {
      await this.deliverFn(event)
    }

    await vi.advanceTimersByTimeAsync(0)
    return true
  }

  async runUntilQuiet(maxTicks = 5000, stableCycles = 3, idleTickMs = 30): Promise<void> {
    let totalEvents = 0
    let emptyCycles = 0

    while (emptyCycles < stableCycles && totalEvents < maxTicks && !this.disposed) {
      if (this.heap.length > 0) {
        emptyCycles = 0
        const delivered = await this.step()
        if (delivered) totalEvents++
      } else {
        await vi.advanceTimersByTimeAsync(idleTickMs)
        await vi.advanceTimersByTimeAsync(0)

        if (this.heap.length === 0) {
          emptyCycles++
        } else {
          emptyCycles = 0
        }
      }
    }
  }

  async runWithDrain(fn: () => Promise<unknown>, maxTicks = 5000, stableCycles = 3, idleTickMs = 30): Promise<void> {
    let fnDone = false
    const fnPromise = fn().finally(() => {
      fnDone = true
    })

    await this.runUntilQuiet(maxTicks, stableCycles, idleTickMs)

    let extraTicks = 0
    const maxExtraTicks = 2000
    while (!fnDone && !this.disposed && extraTicks < maxExtraTicks) {
      await vi.advanceTimersByTimeAsync(idleTickMs)
      await vi.advanceTimersByTimeAsync(0)
      this.currentTime += idleTickMs

      while (this.heap.length > 0 && !this.disposed) {
        await this.step()
      }
      extraTicks++
    }

    await fnPromise
  }

  async advanceBy(ms: number): Promise<void> {
    if (this.disposed) return
    this.currentTime += ms
    await vi.advanceTimersByTimeAsync(ms)
    await vi.advanceTimersByTimeAsync(0)
  }

  dispose(): void {
    this.disposed = true

    for (const event of this.pendingForwards) {
      if (event.reject) {
        event.reject(new TransportError('Scheduler disposed'))
      }
    }
    this.pendingForwards.clear()
    this.heap.length = 0
    this.nextId = 0
  }

  private heapPush(event: ScheduledEvent): void {
    this.heap.push(event)
    this.siftUp(this.heap.length - 1)
  }

  private heapPop(): ScheduledEvent | undefined {
    if (this.heap.length === 0) return undefined
    const top = this.heap[0]
    const last = this.heap.pop()
    if (this.heap.length > 0 && last) {
      this.heap[0] = last
      this.siftDown(0)
    }
    return top
  }

  private siftUp(idx: number): void {
    while (idx > 0) {
      const parentIdx = (idx - 1) >> 1
      if (this.compare(idx, parentIdx) < 0) {
        this.swap(idx, parentIdx)
        idx = parentIdx
      } else {
        break
      }
    }
  }

  private siftDown(idx: number): void {
    const length = this.heap.length
    while (true) {
      let smallest = idx
      const left = 2 * idx + 1
      const right = 2 * idx + 2

      if (left < length && this.compare(left, smallest) < 0) {
        smallest = left
      }
      if (right < length && this.compare(right, smallest) < 0) {
        smallest = right
      }
      if (smallest === idx) break
      this.swap(idx, smallest)
      idx = smallest
    }
  }

  private compare(a: number, b: number): number {
    const ea = this.heap[a]
    const eb = this.heap[b]
    if (ea.deliverAt !== eb.deliverAt) {
      return ea.deliverAt - eb.deliverAt
    }
    return ea.id - eb.id
  }

  private swap(a: number, b: number): void {
    const tmp = this.heap[a]
    this.heap[a] = this.heap[b]
    this.heap[b] = tmp
  }
}
