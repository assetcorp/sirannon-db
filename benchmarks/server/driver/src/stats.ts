// Statistics for reporting throughput and latency credibly. The headline for each rate is the
// median of several independent passes, carrying a percentile bootstrap 95% confidence interval
// and the run-to-run coefficient of variation, so a reader can tell a real difference from noise.
// Latency is summarised by percentiles, never a mean, because latency is right-skewed.
//
// The bootstrap reseeds from a fixed seed, so a given set of samples produces the same interval
// every time and the continuous-integration drift check stays stable.

import { SeededRng } from './rng.ts'

const BOOTSTRAP_ITERATIONS = 10_000

export function mean(samples: number[]): number {
  return samples.length > 0 ? samples.reduce((a, b) => a + b, 0) / samples.length : 0.0
}

// A fold, not Math.max(...samples): spreading a large sample array as call arguments overflows the
// stack, and a measurement window holds hundreds of thousands of latencies.
export function maxOf(samples: number[]): number {
  let max = Number.NEGATIVE_INFINITY
  for (const value of samples) {
    if (value > max) {
      max = value
    }
  }
  return max === Number.NEGATIVE_INFINITY ? 0.0 : max
}

export function median(samples: number[]): number {
  if (samples.length === 0) {
    return 0.0
  }
  const ordered = [...samples].sort((a, b) => a - b)
  const mid = Math.floor(ordered.length / 2)
  if (ordered.length % 2 === 1) {
    return ordered[mid] as number
  }
  return ((ordered[mid - 1] as number) + (ordered[mid] as number)) / 2.0
}

export function sampleStddev(samples: number[]): number {
  const n = samples.length
  if (n < 2) {
    return 0.0
  }
  const avg = mean(samples)
  const variance = samples.reduce((acc, value) => acc + (value - avg) ** 2, 0) / (n - 1)
  return Math.sqrt(variance)
}

// Linear-interpolation percentile (the method NumPy and R call type 7). fraction is in [0, 1];
// 0.99 asks for the 99th percentile.
export function percentile(samples: number[], fraction: number): number {
  if (samples.length === 0) {
    return 0.0
  }
  const ordered = [...samples].sort((a, b) => a - b)
  if (ordered.length === 1) {
    return ordered[0] as number
  }
  const rank = fraction * (ordered.length - 1)
  const low = Math.floor(rank)
  const high = Math.ceil(rank)
  if (low === high) {
    return ordered[low] as number
  }
  const weight = rank - low
  return (ordered[low] as number) * (1.0 - weight) + (ordered[high] as number) * weight
}

export interface MetricSummary {
  median: number
  mean: number
  stddev: number
  cv: number
  ciLow: number
  ciHigh: number
  confidence: number
  runs: number
}

// Summarise one throughput metric collected across independent passes. With a single pass the
// interval collapses to the point value and the spread is zero, which is honest for n=1. With
// more passes the confidence interval is a percentile bootstrap of the median resampled with
// replacement.
export function summarizeMetric(samples: number[], confidence = 0.95, seed = 42): MetricSummary {
  const n = samples.length
  if (n === 0) {
    return { median: 0, mean: 0, stddev: 0, cv: 0, ciLow: 0, ciHigh: 0, confidence, runs: 0 }
  }

  const med = median(samples)
  const avg = mean(samples)
  if (n === 1) {
    return { median: med, mean: avg, stddev: 0, cv: 0, ciLow: med, ciHigh: med, confidence, runs: 1 }
  }

  const stddev = sampleStddev(samples)
  const cv = avg > 0 ? stddev / avg : 0.0

  const rng = new SeededRng(seed)
  const bootstrapMedians: number[] = []
  for (let iteration = 0; iteration < BOOTSTRAP_ITERATIONS; iteration++) {
    const resample: number[] = []
    for (let draw = 0; draw < n; draw++) {
      resample.push(samples[rng.below(n)] as number)
    }
    bootstrapMedians.push(median(resample))
  }
  bootstrapMedians.sort((a, b) => a - b)

  const alpha = 1.0 - confidence
  const lowerIndex = Math.floor(bootstrapMedians.length * (alpha / 2.0))
  const upperIndex = Math.min(bootstrapMedians.length - 1, Math.floor(bootstrapMedians.length * (1.0 - alpha / 2.0)))
  return {
    median: med,
    mean: avg,
    stddev,
    cv,
    ciLow: bootstrapMedians[lowerIndex] as number,
    ciHigh: bootstrapMedians[upperIndex] as number,
    confidence,
    runs: n,
  }
}
