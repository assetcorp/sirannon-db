import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { parse as parseToml } from 'smol-toml'

export interface PostgresConfig {
  host: string
  port: number
  user: string
  password: string
  database: string
  poolSize: number
}

export interface SirannonConfig {
  baseUrl: string
  databaseId: string
}

export interface Config {
  postgres: PostgresConfig
  sirannon: SirannonConfig
  dataSize: number
  warmupSeconds: number
  measureSeconds: number
  runs: number
  seed: number
  sloP99Ms: number
  maxInFlight: number
  workloads: string[]
  targetRates: number[]
  scalingWorkloads: string[]
  driverCpus: number
  engineCpus: number
  requestTimeoutMs: number
  workloadTimeoutMs: number
}

function present(name: string): string | undefined {
  const raw = process.env[name]
  return raw !== undefined && raw.trim() !== '' ? raw : undefined
}

function envInt(name: string, fallback: number): number {
  const raw = present(name)
  if (raw === undefined) {
    return fallback
  }
  const value = Number.parseInt(raw, 10)
  if (!Number.isFinite(value)) {
    throw new Error(`${name} must be an integer, got ${r(raw)}`)
  }
  return value
}

function envFloat(name: string, fallback: number): number {
  const raw = present(name)
  if (raw === undefined) {
    return fallback
  }
  const value = Number.parseFloat(raw)
  if (!Number.isFinite(value)) {
    throw new Error(`${name} must be a number, got ${r(raw)}`)
  }
  return value
}

function envStr(name: string, fallback: string): string {
  return present(name) ?? fallback
}

function envIntList(name: string, fallback: number[]): number[] {
  const raw = present(name)
  if (raw === undefined) {
    return fallback
  }
  return raw
    .split(',')
    .map(part => part.trim())
    .filter(part => part.length > 0)
    .map(part => {
      const value = Number.parseInt(part, 10)
      if (!Number.isFinite(value)) {
        throw new Error(`${name} must be a comma-separated list of integers, got ${r(raw)}`)
      }
      return value
    })
}

function envStrList(name: string, fallback: string[]): string[] {
  const raw = present(name)
  if (raw === undefined) {
    return fallback
  }
  return raw
    .split(',')
    .map(part => part.trim())
    .filter(part => part.length > 0)
}

function r(value: string): string {
  return JSON.stringify(value)
}

function asNumber(value: unknown, fallback: number): number {
  return typeof value === 'number' ? value : typeof value === 'bigint' ? Number(value) : fallback
}

function asString(value: unknown, fallback: string): string {
  return typeof value === 'string' ? value : fallback
}

function asStringList(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []
}

function asNumberList(value: unknown): number[] {
  return Array.isArray(value) ? value.map(item => asNumber(item, Number.NaN)).filter(item => Number.isFinite(item)) : []
}

function section(raw: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = raw[key]
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : {}
}

export function loadConfig(path: string): Config {
  const raw = parseToml(readFileSync(path, 'utf-8')) as Record<string, unknown>
  const run = section(raw, 'run')
  const load = section(raw, 'load')
  const scaling = section(raw, 'scaling')
  const pg = section(raw, 'postgres')
  const sir = section(raw, 'sirannon')
  const resources = section(raw, 'resources')

  const config: Config = {
    postgres: {
      host: envStr('BENCH_PG_HOST', asString(pg.host, '127.0.0.1')),
      port: envInt('BENCH_PG_PORT', asNumber(pg.port, 5432)),
      user: envStr('BENCH_PG_USER', asString(pg.user, 'benchmark')),
      password: envStr('BENCH_PG_PASSWORD', asString(pg.password, 'benchmark')),
      database: envStr('BENCH_PG_DATABASE', asString(pg.database, 'benchmark')),
      poolSize: envInt('BENCH_PG_POOL_SIZE', asNumber(pg.pool_size, 16)),
    },
    sirannon: {
      baseUrl: envStr('BENCH_SIRANNON_URL', asString(sir.base_url, 'http://127.0.0.1:9876')),
      databaseId: envStr('BENCH_SIRANNON_DB', asString(sir.database_id, 'bench')),
    },
    dataSize: envInt('BENCH_DATA_SIZE', asNumber(run.data_size, 10_000)),
    warmupSeconds: envFloat('BENCH_WARMUP_SECONDS', asNumber(run.warmup_seconds, 3.0)),
    measureSeconds: envFloat('BENCH_MEASURE_SECONDS', asNumber(run.measure_seconds, 10.0)),
    runs: envInt('BENCH_RUNS', asNumber(run.runs, 5)),
    seed: envInt('BENCH_SEED', asNumber(run.seed, 42)),
    sloP99Ms: envFloat('BENCH_SLO_P99_MS', asNumber(load.slo_p99_ms, 25.0)),
    maxInFlight: envInt('BENCH_MAX_IN_FLIGHT', asNumber(load.max_in_flight, 256)),
    workloads: envStrList('BENCH_WORKLOADS', asStringList(run.workloads)),
    targetRates: envIntList('BENCH_TARGET_RATES', asNumberList(load.target_rates)),
    scalingWorkloads: envStrList('BENCH_SCALING_WORKLOADS', asStringList(scaling.workloads)),
    driverCpus: envFloat('BENCH_DRIVER_CPUS', asNumber(resources.driver_cpus, 2.0)),
    engineCpus: envFloat('BENCH_ENGINE_CPUS', asNumber(resources.engine_cpus, 2.0)),
    requestTimeoutMs: envInt('BENCH_REQUEST_TIMEOUT_MS', asNumber(run.request_timeout_ms, 60_000)),
    workloadTimeoutMs: envInt('BENCH_WORKLOAD_TIMEOUT_MS', asNumber(run.workload_timeout_ms, 0)),
  }
  validate(config)
  return config
}

function validate(config: Config): void {
  if (config.warmupSeconds < 0) {
    throw new Error(`warmup_seconds must be >= 0, got ${config.warmupSeconds}`)
  }
  if (config.measureSeconds <= 0) {
    throw new Error(`measure_seconds must be > 0, got ${config.measureSeconds}`)
  }
  if (config.runs < 1) {
    throw new Error(`runs must be >= 1, got ${config.runs}`)
  }
  if (config.dataSize < 1) {
    throw new Error(`data_size must be >= 1, got ${config.dataSize}`)
  }
  if (config.maxInFlight < 1) {
    throw new Error(`max_in_flight must be >= 1, got ${config.maxInFlight}`)
  }
  if (config.workloads.length === 0) {
    throw new Error('at least one workload must be configured')
  }
  if (config.targetRates.length === 0) {
    throw new Error('at least one target rate must be configured')
  }
  for (const rate of config.targetRates) {
    if (rate < 1) {
      throw new Error(`each target rate must be positive, got ${rate}`)
    }
  }
  if (config.postgres.port < 1 || config.postgres.port > 65535) {
    throw new Error(`postgres port must be 1-65535, got ${config.postgres.port}`)
  }
  if (config.requestTimeoutMs <= 0) {
    throw new Error(
      `request_timeout_ms must be > 0; the benchmark never issues an unbounded request, got ${config.requestTimeoutMs}`,
    )
  }
  if (config.workloadTimeoutMs < 0) {
    throw new Error(
      `workload_timeout_ms must be >= 0, where 0 derives the deadline from the workload size, got ${config.workloadTimeoutMs}`,
    )
  }
}

export function defaultConfigPath(): string {
  const override = present('BENCH_CONFIG')
  if (override) {
    return override
  }
  const candidates = [
    resolve(process.cwd(), 'config', 'benchmark.toml'),
    resolve(import.meta.dirname, '..', '..', 'config', 'benchmark.toml'),
  ]
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate
    }
  }
  return candidates[0] as string
}
