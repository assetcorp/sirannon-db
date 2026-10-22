import type { Config } from './config.ts'
import { WorkloadStallError, withDeadline, workloadDeadlineMs } from './deadline.ts'
import type { Driver } from './drivers/driver.ts'
import { type EngineCpuSource, engineCpuSource } from './engine-cpu.ts'
import { FailureInterner } from './failures.ts'
import { type LevelRecord, type LevelSweep, levelRecord, peakLevelIndex, repeatLevel, sweepLevel } from './levels.ts'
import { type ClientCeiling, measureClientCeiling, type RunOp } from './loadgen.ts'
import { makeRunOp, operationCost } from './operations.ts'
import { prepare } from './prepare.ts'
import { progress } from './progress.ts'
import { SeededRng, ZipfianGenerator } from './rng.ts'
import { runSoak } from './soak.ts'
import { buildWorkloads } from './workloads/catalogue.ts'
import type { Workload } from './workloads/workload.ts'

const CEILING_MIN_SECONDS = 2
const CEILING_MAX_SECONDS = 4
const CEILING_STATEMENT = 'SELECT 1'

export interface EngineResult {
  workloads: WorkloadResult[]
  failedWorkloads: Record<string, unknown>[]
  clientCeilings: ClientCeiling[]
  clientBoundAny: boolean
  indeterminateAny: boolean
}

export interface WorkloadResult {
  workload: string
  category: string
  runs: number
  operation_cost: Record<string, unknown>
  seed: Record<string, unknown>
  peak_concurrency: number
  concurrency_levels: LevelRecord[]
  operating_point: Record<string, unknown>
  sweep: Record<string, unknown>[]
  sweep_stopped_early: boolean
  soak: Record<string, unknown> | null
}

function ceilingAt(ceilings: ClientCeiling[], level: number): number {
  return ceilings.find(ceiling => ceiling.concurrency === level)?.ceilingOps ?? 0
}

async function runWorkload(
  driver: Driver,
  workload: Workload,
  config: Config,
  ceilings: ClientCeiling[],
  cpuSource: EngineCpuSource,
): Promise<WorkloadResult> {
  const seed = await prepare(driver, workload, config)
  const zipf = new ZipfianGenerator(config.dataSize)
  const makeOp = (): RunOp => makeRunOp(driver, workload, new SeededRng(config.seed), zipf, config.dataSize)

  const sweeps: LevelSweep[] = []
  for (const level of config.concurrencyLevels) {
    sweeps.push(await sweepLevel(driver, makeOp, workload.name, level, ceilingAt(ceilings, level), config, cpuSource))
  }
  const firstPass = sweeps.map(sweep => levelRecord(sweep, driver, config))
  const peakIndex = peakLevelIndex(firstPass)
  const peakSweep = sweeps[peakIndex]
  if (peakSweep === undefined) {
    throw new Error(`workload ${workload.name} measured no concurrency level`)
  }
  progress(`workload ${workload.name}: peak at ${peakSweep.level} in flight; repeating it to ${config.runs} runs`)
  const repeated = await repeatLevel(driver, makeOp, workload.name, peakSweep, config, cpuSource)
  const peak = levelRecord(repeated, driver, config)
  const levels = firstPass.map((record, index) => (index === peakIndex ? peak : record))

  const soak =
    config.soakSeconds > 0 && config.soakWorkloads.includes(workload.name)
      ? await runSoak(makeOp, peak.operating_point, peak.concurrency, config, cpuSource, workload.name)
      : null
  return {
    workload: workload.name,
    category: workload.category,
    runs: config.runs,
    operation_cost: operationCost(driver, workload),
    seed,
    peak_concurrency: peak.concurrency,
    concurrency_levels: levels,
    operating_point: peak.operating_point,
    sweep: peak.sweep,
    sweep_stopped_early: peak.sweep_stopped_early,
    soak,
  }
}

async function measureCeilings(driver: Driver, config: Config, deadlineMs: number): Promise<ClientCeiling[]> {
  const ceilingSeconds = Math.min(Math.max(config.measureSeconds, CEILING_MIN_SECONDS), CEILING_MAX_SECONDS)
  const ceilingFailures = new FailureInterner(driver.failureClassifier)
  const trivialOp: RunOp = async () => {
    try {
      await driver.read(CEILING_STATEMENT, [])
      return null
    } catch (err) {
      return ceilingFailures.categorize(err)
    }
  }
  const ceilings: ClientCeiling[] = []
  for (const level of config.concurrencyLevels) {
    await driver.setConcurrency(level)
    ceilings.push(
      await withDeadline(
        measureClientCeiling(trivialOp, ceilingSeconds, level, CEILING_STATEMENT),
        deadlineMs,
        `client-ceiling measurement at ${level} in flight`,
      ),
    )
  }
  return ceilings
}

function anyRate(result: WorkloadResult, test: (rate: Record<string, unknown>) => boolean): boolean {
  return result.concurrency_levels.some(level => level.sweep.some(test))
}

export async function runEngine(
  driver: Driver,
  config: Config,
  onWorkload?: (snapshot: EngineResult) => void,
): Promise<EngineResult> {
  const catalogue = buildWorkloads()
  for (const name of config.soakWorkloads) {
    if (!catalogue.has(name)) {
      throw new Error(
        `unknown soak workload ${JSON.stringify(name)}; known workloads are ${[...catalogue.keys()].sort().join(', ')}`,
      )
    }
  }
  const cpuSource = engineCpuSource(config.engineCgroup, config.engineCpuUrl)
  const deadlineMs = workloadDeadlineMs(config)
  const clientCeilings = await measureCeilings(driver, config, deadlineMs)

  const results: WorkloadResult[] = []
  const failedWorkloads: Record<string, unknown>[] = []
  let clientBoundAny = false
  let indeterminateAny = false
  const snapshot = (): EngineResult => ({
    workloads: results,
    failedWorkloads,
    clientCeilings,
    clientBoundAny,
    indeterminateAny,
  })
  for (const name of config.workloads) {
    const workload = catalogue.get(name)
    if (workload === undefined) {
      throw new Error(
        `unknown workload ${JSON.stringify(name)}; known workloads are ${[...catalogue.keys()].sort().join(', ')}`,
      )
    }
    try {
      const result = await withDeadline(
        runWorkload(driver, workload, config, clientCeilings, cpuSource),
        deadlineMs,
        `workload ${name} (${driver.name}, ${config.dataSize} rows)`,
      )
      if (anyRate(result, rate => rate.client_bound === true)) {
        clientBoundAny = true
      }
      if (anyRate(result, rate => rate.limit_verdict === 'indeterminate')) {
        indeterminateAny = true
      }
      results.push(result)
    } catch (err) {
      const code = err instanceof WorkloadStallError ? err.name : driver.failureClassifier.codeOf(err)
      const message = err instanceof Error ? err.message : String(err)
      failedWorkloads.push({ workload: name, error_code: code, error: message })
      if (err instanceof WorkloadStallError) {
        progress(
          `workload ${name} stalled past its deadline; the abandoned attempt may still be issuing requests, ` +
            'so the pass ends here instead of measuring the next workload against that interference',
        )
        onWorkload?.(snapshot())
        throw err
      }
      progress(`workload ${name} failed (${code}); recording the failure and moving to the next workload`)
    }
    onWorkload?.(snapshot())
  }
  return snapshot()
}
