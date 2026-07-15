import { type FailureCategory, FailureTally, OPERATION_REJECTED } from './failures.ts'
import { maxOf, mean, percentile } from './stats.ts'

export type RunOp = () => Promise<FailureCategory | null>

export interface LoadResult {
  targetRate: number
  achievedRate: number
  completed: number
  errors: number
  errorRate: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  p999Ms: number
  maxMs: number
  meanMs: number
  maxInFlight: number
  maxInFlightObserved: number
  capOccupancy: number
  occupancySamples: number
  offeredRate: number
  offeredFraction: number
  dispatchedInWindow: number
  expectedInWindow: number
  sustained: boolean
  reachedCap: boolean
  failures: FailureTally
}

const SUSTAINED_FRACTION = 0.95
const OCCUPANCY_SAMPLE_INTERVAL_MS = 10

class Semaphore {
  private permits: number
  private readonly waiters: Array<() => void> = []

  constructor(permits: number) {
    this.permits = permits
  }

  acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits -= 1
      return Promise.resolve()
    }
    return new Promise<void>(resolve => {
      this.waiters.push(resolve)
    })
  }

  release(): void {
    const waiter = this.waiters.shift()
    if (waiter) {
      waiter()
    } else {
      this.permits += 1
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>(resolve => setTimeout(resolve, ms))
}

export async function runOpenLoop(
  runOp: RunOp,
  targetRate: number,
  warmupSeconds: number,
  measureSeconds: number,
  maxInFlight: number,
): Promise<LoadResult> {
  const semaphore = new Semaphore(maxInFlight)
  const intervalMs = 1000 / targetRate

  const start = performance.now()
  const warmupEnd = start + warmupSeconds * 1000
  const measureEnd = warmupEnd + measureSeconds * 1000

  const latenciesMs: number[] = []
  const failures = new FailureTally()
  let served = 0
  let errors = 0
  let inFlight = 0
  let maxInFlightObserved = 0
  let dispatchedInWindow = 0
  const tasks = new Set<Promise<void>>()

  const inMeasureWindow = (instant: number): boolean => instant >= warmupEnd && instant <= measureEnd

  const fire = async (intendedTime: number, intendedInWindow: boolean): Promise<void> => {
    inFlight += 1
    if (inFlight > maxInFlightObserved) {
      maxInFlightObserved = inFlight
    }
    let failure: FailureCategory | null
    try {
      failure = await runOp()
    } catch {
      failure = OPERATION_REJECTED
    } finally {
      inFlight -= 1
      semaphore.release()
    }
    const arrival = performance.now()
    if (intendedInWindow && failure === null) {
      latenciesMs.push(arrival - intendedTime)
    }
    if (inMeasureWindow(arrival)) {
      if (failure === null) {
        served += 1
      } else {
        errors += 1
        failures.record(failure)
      }
    }
  }

  let occupancySamples = 0
  let capSamples = 0
  const sampler = setInterval(() => {
    const instant = performance.now()
    if (!inMeasureWindow(instant)) {
      return
    }
    occupancySamples += 1
    if (inFlight >= maxInFlight) {
      capSamples += 1
    }
  }, OCCUPANCY_SAMPLE_INTERVAL_MS)
  sampler.unref()

  let dispatchEnd = warmupEnd
  try {
    let index = 0
    for (;;) {
      const intended = start + index * intervalMs
      if (intended > measureEnd) {
        break
      }
      const delay = intended - performance.now()
      if (delay > 0) {
        await sleep(delay)
      }
      await semaphore.acquire()
      const intendedInWindow = intended >= warmupEnd
      if (intendedInWindow) {
        dispatchedInWindow += 1
      }
      const task = fire(intended, intendedInWindow)
      tasks.add(task)
      void task.then(() => {
        tasks.delete(task)
      })
      index += 1
    }
    dispatchEnd = performance.now()

    await Promise.allSettled([...tasks])
  } finally {
    clearInterval(sampler)
  }

  const achievedRate = measureSeconds > 0 ? served / measureSeconds : 0.0
  const total = served + errors
  const errorRate = total > 0 ? errors / total : 0.0
  const sustained = achievedRate >= targetRate * SUSTAINED_FRACTION
  const reachedCap = maxInFlightObserved >= maxInFlight
  const expectedInWindow = Math.round(targetRate * measureSeconds)
  const offeredSpanSeconds = (dispatchEnd - warmupEnd) / 1000
  const offeredRate = offeredSpanSeconds > 0 ? dispatchedInWindow / offeredSpanSeconds : 0.0
  return {
    targetRate,
    achievedRate,
    completed: served,
    errors,
    errorRate,
    p50Ms: percentile(latenciesMs, 0.5),
    p95Ms: percentile(latenciesMs, 0.95),
    p99Ms: percentile(latenciesMs, 0.99),
    p999Ms: percentile(latenciesMs, 0.999),
    maxMs: maxOf(latenciesMs),
    meanMs: mean(latenciesMs),
    maxInFlight,
    maxInFlightObserved,
    capOccupancy: occupancySamples > 0 ? capSamples / occupancySamples : 0.0,
    occupancySamples,
    offeredRate,
    offeredFraction: targetRate > 0 ? offeredRate / targetRate : 0.0,
    dispatchedInWindow,
    expectedInWindow,
    sustained,
    reachedCap,
    failures,
  }
}

export interface ClientCeiling {
  ceilingOps: number
  errors: number
  concurrency: number
  seconds: number
  statement: string
}

export async function measureClientCeiling(
  runOp: RunOp,
  seconds: number,
  concurrency: number,
  statement: string,
): Promise<ClientCeiling> {
  const deadline = performance.now() + seconds * 1000
  let ops = 0
  let errors = 0

  const worker = async (): Promise<void> => {
    while (performance.now() < deadline) {
      let failure: FailureCategory | null
      try {
        failure = await runOp()
      } catch {
        failure = OPERATION_REJECTED
      }
      if (failure === null) {
        ops += 1
      } else {
        errors += 1
      }
    }
  }

  const workers: Array<Promise<void>> = []
  for (let i = 0; i < concurrency; i++) {
    workers.push(worker())
  }
  await Promise.all(workers)

  return { ceilingOps: ops / seconds, errors, concurrency, seconds, statement }
}
