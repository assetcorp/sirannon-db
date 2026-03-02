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

export interface ComparisonResult {
  workload: string
  dataSize: number
  sirannon: BenchmarkResult
  postgres: BenchmarkResult
  speedup: number
  framing: string
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

export function formatTable(results: ComparisonResult[]): string {
  const header = [
    pad('Workload', 30, 'left'),
    pad('N Rows', 8),
    pad('Sirannon ops/s', 16),
    pad('Postgres ops/s', 16),
    pad('Speedup', 10),
    pad('P50', 14),
    pad('P99', 14),
    pad('CV', 10),
  ].join(' | ')

  const separator = '-'.repeat(header.length)

  const rows = results.map(r => {
    const sirannonCv = `${(r.sirannon.cv * 100).toFixed(1)}%${cvWarning(r.sirannon.cv)}`
    return [
      pad(r.workload, 30, 'left'),
      pad(r.dataSize.toLocaleString(), 8),
      pad(fmtOps(r.sirannon.opsPerSec), 16),
      pad(fmtOps(r.postgres.opsPerSec), 16),
      pad(fmtSpeedup(r.speedup), 10),
      pad(formatLatency(r.sirannon.p50Ns), 14),
      pad(formatLatency(r.sirannon.p99Ns), 14),
      pad(sirannonCv, 10),
    ].join(' | ')
  })

  return [separator, header, separator, ...rows, separator].join('\n')
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
  console.log('=============================\n')
}

export function writeResults(category: string, systemInfo: SystemInfo, results: ComparisonResult[]): void {
  const resultsDir = join(import.meta.dirname, 'results')
  mkdirSync(resultsDir, { recursive: true })

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const filename = `${category}-${timestamp}.json`
  const filepath = join(resultsDir, filename)

  const output = {
    category,
    timestamp: new Date().toISOString(),
    system: systemInfo,
    results: results.map(r => ({
      workload: r.workload,
      dataSize: r.dataSize,
      framing: r.framing,
      speedup: r.speedup,
      sirannon: r.sirannon,
      postgres: r.postgres,
    })),
  }

  writeFileSync(filepath, `${JSON.stringify(output, null, 2)}\n`)
  console.log(`Results written to ${filepath}`)

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
  printSystemInfo(systemInfo)
}
