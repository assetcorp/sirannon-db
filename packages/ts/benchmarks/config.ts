import { cpus, release, totalmem, type } from 'node:os'
import type { SQLiteDriver } from '../src/core/driver/types'

export interface BenchConfig {
  postgres: {
    host: string
    port: number
    user: string
    password: string
    database: string
    max: number
  }
  /**
   * Controls WAL flush strategy for both engines.
   * - 'matched': SQLite uses PRAGMA synchronous=NORMAL (WAL-mode default, survives process crashes
   *   but not power loss); Postgres uses synchronous_commit=off (WAL writes are deferred, survives
   *   process crashes but may lose recent transactions on OS crash). Both approaches trade crash
   *   safety for write throughput, using different WAL flush strategies.
   * - 'full': Both engines use their strongest durability guarantees.
   */
  durability: 'matched' | 'full'
  dataSizes: number[]
  warmupTime: number
  measureTime: number
  seed: string
}

export interface SystemInfo {
  os: string
  cpu: string
  cpuCores: number
  ramGb: number
  nodeVersion: string
  v8Version: string
  sqliteVersion: string
  postgresVersion: string
  durability: string
  seed: string
  driverName: string
}

let cachedDriver: SQLiteDriver | null = null

export function getBenchDriverName(): string {
  return process.env.BENCH_DRIVER ?? 'better-sqlite3'
}

export async function loadBenchDriver(): Promise<SQLiteDriver> {
  if (cachedDriver) return cachedDriver

  const name = getBenchDriverName()
  switch (name) {
    case 'better-sqlite3': {
      const { betterSqlite3 } = await import('../src/drivers/better-sqlite3/index.js')
      cachedDriver = betterSqlite3()
      return cachedDriver
    }
    case 'node': {
      const { nodeSqlite } = await import('../src/drivers/node/index.js')
      cachedDriver = nodeSqlite()
      return cachedDriver
    }
    default:
      throw new Error(`Unknown BENCH_DRIVER: ${name}. Supported: better-sqlite3, node`)
  }
}

export function loadConfig(): BenchConfig {
  const config: BenchConfig = {
    postgres: {
      host: process.env.BENCH_PG_HOST ?? '127.0.0.1',
      port: Number(process.env.BENCH_PG_PORT ?? 5433),
      user: process.env.BENCH_PG_USER ?? 'benchmark',
      password: process.env.BENCH_PG_PASSWORD ?? 'benchmark',
      database: process.env.BENCH_PG_DATABASE ?? 'benchmark',
      max: Number(process.env.BENCH_PG_MAX_CONNECTIONS ?? 10),
    },
    durability: (process.env.BENCH_DURABILITY as 'matched' | 'full') ?? 'matched',
    dataSizes: process.env.BENCH_DATA_SIZES
      ? process.env.BENCH_DATA_SIZES.split(',').map(Number)
      : [1_000, 10_000, 100_000],
    warmupTime: Number(process.env.BENCH_WARMUP_MS ?? 5_000),
    measureTime: Number(process.env.BENCH_MEASURE_MS ?? 10_000),
    seed: process.env.BENCH_SEED ?? '42',
  }

  validateConfig(config)
  return config
}

function validateConfig(config: BenchConfig): void {
  if (!Number.isFinite(config.warmupTime) || config.warmupTime < 0) {
    throw new Error(`BENCH_WARMUP_MS must be >= 0 and finite, got: ${config.warmupTime}`)
  }
  if (!Number.isFinite(config.measureTime) || config.measureTime < 1000) {
    throw new Error(`BENCH_MEASURE_MS must be >= 1000, got: ${config.measureTime}`)
  }
  if (config.dataSizes.length === 0) {
    throw new Error('BENCH_DATA_SIZES must contain at least one value')
  }
  for (const size of config.dataSizes) {
    if (!Number.isFinite(size) || size < 1 || !Number.isInteger(size)) {
      throw new Error(`Each BENCH_DATA_SIZES entry must be a positive integer, got: ${size}`)
    }
  }
  if (config.postgres.port < 1 || config.postgres.port > 65535) {
    throw new Error(`BENCH_PG_PORT must be 1-65535, got: ${config.postgres.port}`)
  }
  if (config.postgres.max < 1 || config.postgres.max > 1000) {
    throw new Error(`BENCH_PG_MAX_CONNECTIONS must be 1-1000, got: ${config.postgres.max}`)
  }
  if (config.durability !== 'matched' && config.durability !== 'full') {
    throw new Error(`BENCH_DURABILITY must be 'matched' or 'full', got: ${config.durability}`)
  }
  try {
    BigInt(config.seed)
  } catch {
    throw new Error(`BENCH_SEED must be a valid integer, got: ${config.seed}`)
  }
}

export function collectSystemInfo(): SystemInfo {
  const config = loadConfig()
  const cpuInfo = cpus()
  return {
    os: `${type()} ${release()}`,
    cpu: cpuInfo[0]?.model ?? 'unknown',
    cpuCores: cpuInfo.length,
    ramGb: Math.round((totalmem() / (1024 * 1024 * 1024)) * 10) / 10,
    nodeVersion: process.version,
    v8Version: process.versions.v8,
    durability: config.durability,
    seed: config.seed,
    sqliteVersion: '',
    postgresVersion: '',
    driverName: getBenchDriverName(),
  }
}
