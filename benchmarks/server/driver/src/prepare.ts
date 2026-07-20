import type { Config } from './config.ts'
import type { Driver } from './drivers/driver.ts'
import { engineCpuBlock, withEngineCores } from './engine-cpu.ts'
import { progress } from './progress.ts'
import { SeededRng } from './rng.ts'
import type { Workload } from './workloads/workload.ts'

const RESET_RETRY_DELAY_MS = 5_000

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function resetSchema(driver: Driver, workload: Workload, config: Config): Promise<void> {
  const schema = driver.dialect === 'sqlite' ? workload.sqliteSchema : workload.postgresSchema
  const statements = schema
    .split(';')
    .map(part => part.trim())
    .filter(part => part.length > 0)
  const budgetMs = config.prepareRetrySeconds * 1000
  const start = performance.now()
  for (;;) {
    try {
      await driver.dropTables([...workload.tables])
      await driver.executeDdl(statements)
      return
    } catch (err) {
      const code = driver.failureClassifier.codeOf(err)
      const kind = driver.failureClassifier.kindOf(code, err)
      const elapsedMs = performance.now() - start
      if (kind === 'client_error' || elapsedMs + RESET_RETRY_DELAY_MS > budgetMs) {
        throw err
      }
      progress(
        `prepare ${workload.name}: engine not ready (${code}); retrying in ${RESET_RETRY_DELAY_MS / 1000}s, ` +
          `${Math.round(elapsedMs / 1000)}s of the ${Math.round(budgetMs / 1000)}s recovery budget used`,
      )
      await sleep(RESET_RETRY_DELAY_MS)
    }
  }
}

export async function prepare(driver: Driver, workload: Workload, config: Config): Promise<Record<string, unknown>> {
  await resetSchema(driver, workload, config)
  const seedRng = new SeededRng(config.seed)
  const seedTables = workload.seed(seedRng, config.dataSize)
  const counter = { rows: 0 }
  const counted = seedTables.map(table => ({
    ...table,
    rows: (function* (source: Iterable<unknown[]>) {
      for (const row of source) {
        counter.rows += 1
        yield row
      }
    })(table.rows),
  }))
  const seedStart = performance.now()
  const ticker = setInterval(() => {
    const elapsed = (performance.now() - seedStart) / 1000
    progress(`seed ${workload.name}: ${counter.rows.toLocaleString('en-US')} rows in ${Math.round(elapsed)}s`)
  }, 10_000)
  let seconds: number
  let seedCores: number | null = null
  try {
    seedCores = (await withEngineCores(config.engineCgroup, () => driver.seed(counted))).coresUsed
  } finally {
    clearInterval(ticker)
    seconds = (performance.now() - seedStart) / 1000
  }
  progress(
    `seed ${workload.name}: done, ${counter.rows.toLocaleString('en-US')} rows in ${seconds.toFixed(1)}s` +
      (seconds > 0 ? ` (${Math.round(counter.rows / seconds).toLocaleString('en-US')} rows/s)` : '') +
      (seedCores !== null ? ` (engine ${seedCores.toFixed(1)} cores)` : ''),
  )
  return {
    rows: counter.rows,
    seconds,
    rows_per_second: seconds > 0 ? counter.rows / seconds : 0,
    engine_cpu: engineCpuBlock([seedCores], config.engineCpus),
    note:
      'Seeding is disclosed, never compared: each engine loads through its own fastest documented ' +
      'path, and Sirannon seeds with durability off while PostgreSQL seeds inside one transaction.',
  }
}
