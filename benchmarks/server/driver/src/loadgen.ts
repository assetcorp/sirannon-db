import { type FailureCategory, FailureTally, OPERATION_REJECTED } from './failures.ts'
import { maxOf, mean, percentile } from './stats.ts'

export type RunOp = () => Promise<FailureCategory | null>

export interface LoadBucket {
  startSeconds: number
  served: number
  errors: number
  achievedRate: number
  p50Ms: number
  p99Ms: number
  maxMs: number
}

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
  buckets: LoadBucket[] | null
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
  bucketSeconds = 0,
): Promise<LoadResult> {
  const semaphore = new Semaphore(maxInFlight)
  const intervalMs = 1000 / targetRate

  const start = performance.now()
  const warmupEnd = start + warmupSeconds * 1000
  const measureEnd = warmupEnd + measureSeconds * 1000

  const bucketCount = bucketSeconds > 0 ? Math.ceil(measureSeconds / bucketSeconds) : 0
  const bucketServed = new Array<number>(bucketCount).fill(0)
  const bucketErrors = new Array<number>(bucketCount).fill(0)
  const latencyBucketIndex: number[] = []

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
    const bucket =
      bucketCount > 0 && intendedInWindow
        ? Math.min(bucketCount - 1, Math.floor((intendedTime - warmupEnd) / (bucketSeconds * 1000)))
        : -1
    if (intendedInWindow && failure === null) {
      latenciesMs.push(arrival - intendedTime)
      if (bucket >= 0) {
        latencyBucketIndex.push(bucket)
      }
    }
    if (bucket >= 0) {
      if (failure === null) {
        bucketServed[bucket] = (bucketServed[bucket] ?? 0) + 1
      } else {
        bucketErrors[bucket] = (bucketErrors[bucket] ?? 0) + 1
      }
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

  let buckets: LoadBucket[] | null = null
  if (bucketCount > 0) {
    const perBucket: number[][] = Array.from({ length: bucketCount }, () => [])
    for (const [position, bucketIdx] of latencyBucketIndex.entries()) {
      perBucket[bucketIdx]?.push(latenciesMs[position] ?? 0)
    }
    buckets = perBucket.map((samples, index) => ({
      startSeconds: index * bucketSeconds,
      served: bucketServed[index] ?? 0,
      errors: bucketErrors[index] ?? 0,
      achievedRate: (bucketServed[index] ?? 0) / Math.min(bucketSeconds, measureSeconds - index * bucketSeconds),
      p50Ms: percentile(samples, 0.5),
      p99Ms: percentile(samples, 0.99),
      maxMs: maxOf(samples),
    }))
  }

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
    buckets,
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
