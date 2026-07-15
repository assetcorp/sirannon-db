// Open-loop load generation with coordinated-omission correction.
//
// A closed-loop generator sends the next request only after the previous one returns, so when the
// server stalls the generator stops sending and the slow requests never get measured. This
// generator instead fires requests at a fixed target rate whether or not earlier requests have
// returned, and it records each request's latency from the time it was meant to be sent, not the
// time a backlog let it start. A request delayed by a stall therefore carries the full delay,
// which is the wrk2 correction for coordinated omission.
//
// Concurrency is bounded by a semaphore so a stalled server cannot spawn unbounded work. The result
// records the raw facts a caller needs to classify the outcome: the achieved rate, the rate the
// generator actually offered, and how much of the window it spent pinned against its own in-flight
// cap. Those last two are what separate a slow server from a slow client, and both are reported
// rather than collapsed into a verdict here.
//
// The offered rate is measured, not assumed. Counting dispatches alone cannot show a shortfall: the
// loop walks every scheduled index whatever happens, so it always ends up having dispatched exactly
// what the schedule called for, however far behind the clock it has fallen. The shortfall only
// appears in how long that took, so the offered rate is the in-window dispatch count over the
// wall-clock span from the window's scheduled start to the last dispatch. That needs one clock
// reading per pass rather than one per request, so measuring it costs the request path nothing.
//
// In-flight occupancy is sampled off the request path by a timer, because it answers a question the
// maximum cannot: a single spike touches the cap, but a server that has become the bottleneck holds
// the generator against the cap for the whole window.

import { maxOf, mean, percentile } from './stats.ts'

export type RunOp = () => Promise<boolean>

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

// Drive runOp at targetRate requests per second. runOp performs one operation and resolves true
// on success or false on an application error; it must not reject, so a single failed request
// cannot tear down the run. Latencies from the warmup window are discarded; latencies from the
// measurement window are corrected for coordinated omission and returned as percentiles.
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
  let served = 0
  let errors = 0
  let inFlight = 0
  let maxInFlightObserved = 0
  let dispatchedInWindow = 0
  const tasks = new Set<Promise<void>>()

  const inMeasureWindow = (instant: number): boolean => instant >= warmupEnd && instant <= measureEnd

  // Two accounting rules run here on purpose. Throughput counts responses that arrive within the
  // measurement window, so a saturated server whose backlog drains after the window is not
  // credited for work it did late. Latency is charged from the intended send time for every
  // request meant to be sent in the window, so a stalled request carries its full delay.
  const fire = async (intendedTime: number, intendedInWindow: boolean): Promise<void> => {
    inFlight += 1
    if (inFlight > maxInFlightObserved) {
      maxInFlightObserved = inFlight
    }
    let ok: boolean
    try {
      ok = await runOp()
    } catch {
      ok = false
    } finally {
      inFlight -= 1
      semaphore.release()
    }
    const arrival = performance.now()
    if (intendedInWindow && ok) {
      latenciesMs.push(arrival - intendedTime)
    }
    if (inMeasureWindow(arrival)) {
      if (ok) {
        served += 1
      } else {
        errors += 1
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
    // The loop breaks on the first index past the window and does no work in between, so this is
    // when the last scheduled request left, whether that was on time or long after.
    dispatchEnd = performance.now()

    // Let the backlog drain. Outstanding work is bounded by the in-flight cap, so once the generator
    // stops offering, the queue clears without a pathological tail.
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
  }
}

export interface ClientCeiling {
  ceilingOps: number
  errors: number
  concurrency: number
  seconds: number
  statement: string
}

// Measure how fast the load client can turn round trips over when it is never made to wait, driving
// a trivial statement closed-loop at full concurrency.
//
// Read this for exactly what it is: an upper bound on the client's issue rate, measured on a
// response of a few bytes. A workload whose rows are a kilobyte each costs the client more per
// operation, so its true ceiling is lower than this one, by an amount this probe cannot see. That
// asymmetry decides what the number may be used for. Achieving this rate proves the client is at
// its limit. Falling short of it proves nothing on its own, because the workload's own lower
// ceiling may be what was reached; whether the server is saturated is settled on other evidence.
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
      let ok: boolean
      try {
        ok = await runOp()
      } catch {
        ok = false
      }
      if (ok) {
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
