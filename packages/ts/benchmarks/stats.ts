declare module 'simple-statistics' {
  export function gammaln(x: number): number
}

import { errorFunction, gammaln, mean, quantile, sampleStandardDeviation } from 'simple-statistics'
import { SeededRng } from './rng'

export interface WelchResult {
  tStatistic: number
  pValue: number
  degreesOfFreedom: number
  significant005: boolean
  significant001: boolean
  significant0001: boolean
}

export interface OutlierResult {
  count: number
  percentage: number
  lowerFence: number
  upperFence: number
}

export interface SpeedupCI {
  pointEstimate: number
  lowerBound: number
  upperBound: number
  confidence: number
}

function betaContinuedFraction(x: number, a: number, b: number): number {
  const MAX_ITER = 200
  const EPS = 1e-14
  const TINY = 1e-30
  let c = 1
  let d = 1 - ((a + b) * x) / (a + 1)
  if (Math.abs(d) < TINY) d = TINY
  d = 1 / d
  let h = d

  for (let m = 1; m <= MAX_ITER; m++) {
    const m2 = 2 * m
    let num = (m * (b - m) * x) / ((a + m2 - 1) * (a + m2))
    d = 1 + num * d
    if (Math.abs(d) < TINY) d = TINY
    c = 1 + num / c
    if (Math.abs(c) < TINY) c = TINY
    d = 1 / d
    h *= d * c

    num = (-(a + m) * (a + b + m) * x) / ((a + m2) * (a + m2 + 1))
    d = 1 + num * d
    if (Math.abs(d) < TINY) d = TINY
    c = 1 + num / c
    if (Math.abs(c) < TINY) c = TINY
    d = 1 / d
    const delta = d * c
    h *= delta

    if (Math.abs(delta - 1) < EPS) break
  }

  return h
}

function regularizedIncompleteBeta(x: number, a: number, b: number): number {
  if (x <= 0) return 0
  if (x >= 1) return 1

  if (x > (a + 1) / (a + b + 2)) {
    return 1 - regularizedIncompleteBeta(1 - x, b, a)
  }

  const lnBeta = gammaln(a) + gammaln(b) - gammaln(a + b)
  const frontFactor = Math.exp(a * Math.log(x) + b * Math.log(1 - x) - lnBeta) / a
  return frontFactor * betaContinuedFraction(x, a, b)
}

export function tDistCdf(t: number, df: number): number {
  if (!(df > 0)) return NaN
  if (t === 0) return 0.5
  if (!Number.isFinite(df)) return 0.5 * (1 + errorFunction(t / Math.SQRT2))

  const x = df / (df + t * t)
  const ibeta = regularizedIncompleteBeta(x, df / 2, 0.5)

  if (t >= 0) return 1 - 0.5 * ibeta
  return 0.5 * ibeta
}

export function welchTTest(samplesA: number[], samplesB: number[]): WelchResult {
  const nA = samplesA.length
  const nB = samplesB.length
  const meanA = mean(samplesA)
  const meanB = mean(samplesB)
  const sdA = sampleStandardDeviation(samplesA)
  const sdB = sampleStandardDeviation(samplesB)

  const varA = sdA * sdA
  const varB = sdB * sdB
  const seA = varA / nA
  const seB = varB / nB

  if (seA + seB === 0) {
    const identical = meanA === meanB
    return {
      tStatistic: identical ? 0 : meanA > meanB ? Infinity : -Infinity,
      pValue: identical ? 1 : 0,
      degreesOfFreedom: nA + nB - 2,
      significant005: !identical,
      significant001: !identical,
      significant0001: !identical,
    }
  }

  const tStat = (meanA - meanB) / Math.sqrt(seA + seB)

  const numerator = (seA + seB) ** 2
  const denominator = seA ** 2 / (nA - 1) + seB ** 2 / (nB - 1)
  const df = numerator / denominator

  const pValue = 2 * (1 - tDistCdf(Math.abs(tStat), df))

  return {
    tStatistic: tStat,
    pValue,
    degreesOfFreedom: df,
    significant005: pValue < 0.05,
    significant001: pValue < 0.01,
    significant0001: pValue < 0.001,
  }
}

export function detectOutliers(samples: number[]): OutlierResult {
  const q1 = quantile(samples, 0.25)
  const q3 = quantile(samples, 0.75)
  const iqr = q3 - q1
  const lowerFence = q1 - 1.5 * iqr
  const upperFence = q3 + 1.5 * iqr

  let count = 0
  for (const s of samples) {
    if (s < lowerFence || s > upperFence) count++
  }

  return {
    count,
    percentage: (count / samples.length) * 100,
    lowerFence,
    upperFence,
  }
}

export function speedupConfidenceInterval(
  sirannonSamples: number[],
  postgresSamples: number[],
  confidence = 0.95,
  seed?: bigint,
): SpeedupCI {
  const iterations = 10_000
  const rng = new SeededRng(seed ?? 42n)
  const ratios: number[] = []

  for (let i = 0; i < iterations; i++) {
    let sirannonSum = 0
    for (let j = 0; j < sirannonSamples.length; j++) {
      sirannonSum += sirannonSamples[rng.nextInt(sirannonSamples.length)]
    }
    const sirannonMean = sirannonSum / sirannonSamples.length

    let postgresSum = 0
    for (let j = 0; j < postgresSamples.length; j++) {
      postgresSum += postgresSamples[rng.nextInt(postgresSamples.length)]
    }
    const postgresMean = postgresSum / postgresSamples.length

    if (postgresMean > 0) {
      ratios.push(sirannonMean / postgresMean)
    }
  }

  ratios.sort((a, b) => a - b)
  const alpha = 1 - confidence
  const lowerIdx = Math.floor(ratios.length * (alpha / 2))
  const upperIdx = Math.floor(ratios.length * (1 - alpha / 2))

  return {
    pointEstimate: mean(sirannonSamples) / mean(postgresSamples),
    lowerBound: ratios[lowerIdx],
    upperBound: ratios[upperIdx],
    confidence,
  }
}
