import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import type { SystemInfo } from './config'

export interface BenchmarkResult {
  name: string
  opsPerSec: number
  meanNs: number
  p50Ns: number
  p75Ns: number
  p99Ns: number
  p999Ns: number
  minNs: number
  maxNs: number
  sdNs: number
  cv: number
  moe: number
  samples: number
}

export interface SignificanceInfo {
  tStatistic: number
  pValue: number
  degreesOfFreedom: number
  significant005: boolean
  significant001: boolean
  significant0001: boolean
}

export interface SpeedupCIInfo {
  pointEstimate: number
  lowerBound: number
  upperBound: number
  confidence: number
}

export interface OutlierInfo {
  sirannon: { count: number; percentage: number }
  postgres: { count: number; percentage: number }
}

export interface ComparisonResult {
  workload: string
  dataSize: number
  sirannon: BenchmarkResult
  postgres: BenchmarkResult
  speedup: number
  framing: string
  sirannonSamples?: number[]
  postgresSamples?: number[]
  significance?: SignificanceInfo
  speedupCI?: SpeedupCIInfo
  outliers?: OutlierInfo
  runs?: number
}

export interface SirannonOnlyResult {
  workload: string
  result: BenchmarkResult
  framing: string
}

export function formatLatency(ns: number): string {
  if (ns < 1_000) return `${ns.toFixed(2)} ns`
  if (ns < 1_000_000) return `${(ns / 1_000).toFixed(2)} us`
  return `${(ns / 1_000_000).toFixed(2)} ms`
}

function pad(str: string, width: number, align: 'left' | 'right' = 'right'): string {
  if (align === 'left') return str.padEnd(width)
  return str.padStart(width)
}

function fmtOps(ops: number): string {
  if (ops >= 1_000_000) return `${(ops / 1_000_000).toFixed(2)}M`
  if (ops >= 1_000) return `${(ops / 1_000).toFixed(2)}K`
  return ops.toFixed(2)
}

function fmtSpeedup(s: number): string {
  return `${s.toFixed(2)}x`
}

function cvWarning(cv: number): string {
  return cv > 0.1 ? ' [!]' : ''
}

function fmtSig(r: ComparisonResult): string {
  if (!r.significance) return '-'
  if (r.significance.significant0001) return '***'
  if (r.significance.significant001) return '**'
  if (r.significance.significant005) return '*'
  return 'n/s'
}

function fmtCI(r: ComparisonResult): string {
  if (!r.speedupCI) return '-'
  return `[${r.speedupCI.lowerBound.toFixed(1)}, ${r.speedupCI.upperBound.toFixed(1)}]`
}

function fmtRuns(r: ComparisonResult): string {
  return r.runs ? String(r.runs) : '-'
}

export function formatTable(results: ComparisonResult[]): string {
  const hasStats = results.some(r => r.significance)

  const headerCols = [
    pad('Workload', 30, 'left'),
    pad('N Rows', 8),
    pad('Speedup', 10),
    pad('CI', 16),
    pad('Sig', 5),
    pad('Sirannon ops/s', 16),
    pad('Postgres ops/s', 16),
    pad('P50', 14),
    pad('P99', 14),
    pad('CV', 10),
  ]

  if (hasStats) {
    headerCols.push(pad('Runs', 5))
  }

  const header = headerCols.join(' | ')
  const separator = '-'.repeat(header.length)

  const rows = results.map(r => {
    const sirannonCv = `${(r.sirannon.cv * 100).toFixed(1)}%${cvWarning(r.sirannon.cv)}`
    const cols = [
      pad(r.workload, 30, 'left'),
      pad(r.dataSize.toLocaleString(), 8),
      pad(fmtSpeedup(r.speedup), 10),
      pad(fmtCI(r), 16),
      pad(fmtSig(r), 5),
      pad(fmtOps(r.sirannon.opsPerSec), 16),
      pad(fmtOps(r.postgres.opsPerSec), 16),
      pad(formatLatency(r.sirannon.p50Ns), 14),
      pad(formatLatency(r.sirannon.p99Ns), 14),
      pad(sirannonCv, 10),
    ]

    if (hasStats) {
      cols.push(pad(fmtRuns(r), 5))
    }

    return cols.join(' | ')
  })

  const lines = [separator, header, separator, ...rows, separator]

  if (hasStats) {
    lines.push('')
    lines.push('Significance: *** p<0.001, ** p<0.01, * p<0.05, n/s = not significant, - = single run')
    lines.push('CI: 95% bootstrap confidence interval on speedup ratio')
  }

  return lines.join('\n')
}

export function printSystemInfo(info: SystemInfo): void {
  console.log('\n=== Benchmark System Info ===')
  console.log(`OS:              ${info.os}`)
  console.log(`CPU:             ${info.cpu}`)
  console.log(`CPU Cores:       ${info.cpuCores}`)
  console.log(`RAM:             ${info.ramGb} GB`)
  console.log(`Node.js:         ${info.nodeVersion}`)
  console.log(`V8:              ${info.v8Version}`)
  console.log(`SQLite:          ${info.sqliteVersion}`)
  console.log(`Postgres:        ${info.postgresVersion}`)
  console.log(`Durability:      ${info.durability}`)
  console.log(`Seed:            ${info.seed}`)
  console.log('=============================\n')
}

export function escapeCsvField(value: string | number): string {
  const str = String(value)
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return `"${str.replace(/"/g, '""')}"`
  }
  return str
}

function writeCsv(resultsDir: string, category: string, timestamp: string, results: ComparisonResult[]): void {
  const csvHeader = [
    'workload',
    'dataSize',
    'sirannonOpsPerSec',
    'postgresOpsPerSec',
    'speedup',
    'sirannonP50Ns',
    'sirannonP99Ns',
    'postgresP50Ns',
    'postgresP99Ns',
    'sirannonCV',
    'postgresCV',
    'pValue',
    'ciLower',
    'ciUpper',
    'runs',
  ].join(',')

  const csvRows = results.map(r =>
    [
      escapeCsvField(r.workload),
      r.dataSize,
      r.sirannon.opsPerSec.toFixed(2),
      r.postgres.opsPerSec.toFixed(2),
      r.speedup.toFixed(4),
      r.sirannon.p50Ns.toFixed(0),
      r.sirannon.p99Ns.toFixed(0),
      r.postgres.p50Ns.toFixed(0),
      r.postgres.p99Ns.toFixed(0),
      r.sirannon.cv.toFixed(4),
      r.postgres.cv.toFixed(4),
      r.significance ? r.significance.pValue.toFixed(6) : '',
      r.speedupCI ? r.speedupCI.lowerBound.toFixed(2) : '',
      r.speedupCI ? r.speedupCI.upperBound.toFixed(2) : '',
      r.runs ?? '',
    ].join(','),
  )

  const csvPath = join(resultsDir, `${category}-${timestamp}.csv`)
  writeFileSync(csvPath, `${[csvHeader, ...csvRows].join('\n')}\n`)
  console.log(`CSV written to ${csvPath}`)

  const hasMultiRun = results.some(r => r.sirannonSamples && r.postgresSamples)
  if (hasMultiRun) {
    const perRunHeader = 'workload,dataSize,run,sirannonOpsPerSec,postgresOpsPerSec'
    const perRunRows: string[] = []

    for (const r of results) {
      if (!r.sirannonSamples || !r.postgresSamples) continue
      const runCount = Math.min(r.sirannonSamples.length, r.postgresSamples.length)
      for (let i = 0; i < runCount; i++) {
        perRunRows.push(
          [
            escapeCsvField(r.workload),
            r.dataSize,
            i + 1,
            r.sirannonSamples[i].toFixed(2),
            r.postgresSamples[i].toFixed(2),
          ].join(','),
        )
      }
    }

    const perRunPath = join(resultsDir, `${category}-per-run-${timestamp}.csv`)
    writeFileSync(perRunPath, `${[perRunHeader, ...perRunRows].join('\n')}\n`)
    console.log(`Per-run CSV written to ${perRunPath}`)
  }
}

export function writeResults(category: string, systemInfo: SystemInfo, results: ComparisonResult[]): void {
  const resultsDir = join(import.meta.dirname, 'results')
  mkdirSync(resultsDir, { recursive: true })

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const filename = `${category}-${timestamp}.json`
  const filepath = join(resultsDir, filename)

  const output = {
    category,
    benchmarkType: 'comparison' as const,
    timestamp: new Date().toISOString(),
    system: systemInfo,
    results: results.map(r => ({
      workload: r.workload,
      dataSize: r.dataSize,
      framing: r.framing,
      speedup: r.speedup,
      sirannon: r.sirannon,
      postgres: r.postgres,
      ...(r.significance && { significance: r.significance }),
      ...(r.speedupCI && { speedupCI: r.speedupCI }),
      ...(r.outliers && { outliers: r.outliers }),
      ...(r.runs && { runs: r.runs }),
    })),
  }

  writeFileSync(filepath, `${JSON.stringify(output, null, 2)}\n`)
  console.log(`Results written to ${filepath}`)

  writeCsv(resultsDir, category, timestamp, results)

  printSystemInfo(systemInfo)
  console.log(formatTable(results))

  const unreliable = results.filter(r => r.sirannon.cv > 0.1 || r.postgres.cv > 0.1)
  if (unreliable.length > 0) {
    console.log('\n[!] Warning: The following results have CV > 10% and may be unreliable:')
    for (const r of unreliable) {
      const which = []
      if (r.sirannon.cv > 0.1) which.push(`Sirannon CV=${(r.sirannon.cv * 100).toFixed(1)}%`)
      if (r.postgres.cv > 0.1) which.push(`Postgres CV=${(r.postgres.cv * 100).toFixed(1)}%`)
      console.log(`  - ${r.workload} (${r.dataSize} rows): ${which.join(', ')}`)
    }
  }
}

export function writeSirannonOnlyResults(
  category: string,
  systemInfo: SystemInfo,
  results: SirannonOnlyResult[],
): void {
  const resultsDir = join(import.meta.dirname, 'results')
  mkdirSync(resultsDir, { recursive: true })

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const filename = `${category}-${timestamp}.json`
  const filepath = join(resultsDir, filename)

  const output = {
    category,
    benchmarkType: 'feature' as const,
    timestamp: new Date().toISOString(),
    system: systemInfo,
    results: results.map(r => ({
      workload: r.workload,
      framing: r.framing,
      result: r.result,
    })),
  }

  writeFileSync(filepath, `${JSON.stringify(output, null, 2)}\n`)
  console.log(`Results written to ${filepath}`)

  writeSirannonOnlyCsv(resultsDir, category, timestamp, results)

  printSystemInfo(systemInfo)

  console.log('\n--- Feature Benchmark Results ---')
  for (const r of results) {
    console.log(
      `  ${r.workload}: ${fmtOps(r.result.opsPerSec)} ops/s | P50=${formatLatency(r.result.p50Ns)} | P99=${formatLatency(r.result.p99Ns)}`,
    )
  }
}

function writeSirannonOnlyCsv(
  resultsDir: string,
  category: string,
  timestamp: string,
  results: SirannonOnlyResult[],
): void {
  const header = [
    'benchmarkType',
    'workload',
    'opsPerSec',
    'meanNs',
    'p50Ns',
    'p99Ns',
    'p999Ns',
    'minNs',
    'maxNs',
    'sdNs',
    'cv',
    'samples',
  ].join(',')

  const rows = results.map(r =>
    [
      'feature',
      escapeCsvField(r.workload),
      r.result.opsPerSec.toFixed(2),
      r.result.meanNs.toFixed(0),
      r.result.p50Ns.toFixed(0),
      r.result.p99Ns.toFixed(0),
      r.result.p999Ns.toFixed(0),
      r.result.minNs.toFixed(0),
      r.result.maxNs.toFixed(0),
      r.result.sdNs.toFixed(0),
      r.result.cv.toFixed(4),
      r.result.samples,
    ].join(','),
  )

  const csvPath = join(resultsDir, `${category}-${timestamp}.csv`)
  writeFileSync(csvPath, `${[header, ...rows].join('\n')}\n`)
  console.log(`CSV written to ${csvPath}`)
}
