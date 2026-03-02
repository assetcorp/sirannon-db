import { cpus, release, totalmem, type } from 'node:os'

export interface BenchConfig {
  postgres: {
    host: string
    port: number
    user: string
    password: string
    database: string
    max: number
  }
  durability: 'matched' | 'full'
  dataSizes: number[]
  warmupTime: number
  measureTime: number
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
}

export function loadConfig(): BenchConfig {
  return {
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
  }
}

export function collectSystemInfo(): SystemInfo {
  const cpuInfo = cpus()
  return {
    os: `${type()} ${release()}`,
    cpu: cpuInfo[0]?.model ?? 'unknown',
    cpuCores: cpuInfo.length,
    ramGb: Math.round((totalmem() / (1024 * 1024 * 1024)) * 10) / 10,
    nodeVersion: process.version,
    v8Version: process.versions.v8,
    durability: loadConfig().durability,
    sqliteVersion: '',
    postgresVersion: '',
  }
}
