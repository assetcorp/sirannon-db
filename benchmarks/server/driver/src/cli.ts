import { existsSync, readFileSync, unlinkSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { parseArgs } from 'node:util'
import { type Config, defaultConfigPath, loadConfig } from './config.ts'
import { buildDriver } from './drivers/registry.ts'
import { captureEnvironment } from './environment.ts'
import { measureCdcLatency } from './features.ts'
import { type EngineResult, runEngine } from './harness.ts'
import { buildEngineReport, clientSaturationBlock, configBlock } from './reporter.ts'
import {
  defaultResultsDir,
  resolveRunIdForWrite,
  runDirectory,
  validateArtifactName,
  writeJson,
  writeRunManifest,
} from './run-store.ts'

function harnessVersion(): string {
  try {
    const pkg = JSON.parse(readFileSync(resolve(import.meta.dirname, '..', 'package.json'), 'utf-8')) as {
      version?: string
    }
    return pkg.version ?? '0.0.0'
  } catch {
    return '0.0.0'
  }
}

function describeError(err: unknown): string {
  if (!(err instanceof Error)) {
    return String(err)
  }
  const base = err.stack ?? err.message
  const cause = (err as { cause?: unknown }).cause
  if (cause === undefined) {
    return base
  }
  const causeText = cause instanceof Error ? (cause.stack ?? cause.message) : String(cause)
  return `${base}\ncaused by: ${causeText}`
}

function envInt(name: string, fallback: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === '') {
    return fallback
  }
  const value = Number.parseInt(raw, 10)
  return Number.isFinite(value) ? value : fallback
}

async function run(
  engine: string,
  durability: string,
  config: Config,
  wantFeatures: boolean,
  environment: Record<string, unknown>,
  savePartial: (report: Record<string, unknown>) => void,
): Promise<Record<string, unknown>> {
  const driver = buildDriver(engine, config, durability)
  await driver.connect()
  try {
    const engineInfo = await driver.info()
    const reportOf = (result: EngineResult, features: Record<string, unknown>[]): Record<string, unknown> =>
      buildEngineReport({
        environment,
        engine,
        delivery: driver.delivery,
        durability,
        engineInfo,
        config,
        workloads: result.workloads as unknown as Record<string, unknown>[],
        failedWorkloads: result.failedWorkloads,
        features,
        clientSaturation: clientSaturationBlock(
          engine,
          config,
          result.clientCeiling,
          result.clientBoundAny,
          result.indeterminateAny,
        ),
      })
    const engineResult = await runEngine(driver, config, snapshot =>
      savePartial({ ...reportOf(snapshot, []), partial: true }),
    )

    const features: Record<string, unknown>[] = []
    if (wantFeatures && engine === 'sirannon') {
      try {
        features.push(
          await measureCdcLatency(
            config.sirannon.baseUrl,
            config.sirannon.databaseId,
            envInt('BENCH_CDC_SAMPLES', 200),
            envInt('BENCH_CDC_WARMUP', 20),
          ),
        )
      } catch (err) {
        process.stderr.write(
          `change-feed characterisation failed; recording the run without it: ${describeError(err)}\n`,
        )
      }
    }

    return reportOf(engineResult, features)
  } finally {
    await driver.close()
  }
}

async function main(argv: string[]): Promise<number> {
  const { values } = parseArgs({
    args: argv,
    options: {
      engine: { type: 'string' },
      durability: { type: 'string', default: 'matched' },
      config: { type: 'string' },
      'results-dir': { type: 'string' },
      'run-id': { type: 'string' },
      features: { type: 'boolean', default: false },
    },
  })

  const engine = values.engine
  if (engine !== 'sirannon' && engine !== 'postgres') {
    process.stderr.write("--engine is required and must be 'sirannon' or 'postgres'\n")
    return 2
  }
  const durability = values.durability === 'full' ? 'full' : 'matched'
  const configPath = values.config ?? defaultConfigPath()
  const resultsDir = values['results-dir'] ?? defaultResultsDir()

  const config = loadConfig(configPath)
  const environment = captureEnvironment(harnessVersion())
  const runId = resolveRunIdForWrite(values['run-id'] ?? null)
  writeRunManifest(resultsDir, runId, environment, configBlock(config))
  const directory = runDirectory(resultsDir, runId)
  const artifact = validateArtifactName(`engine-${engine}-${durability}.json`)
  const partialPath = join(directory, validateArtifactName(`engine-${engine}-${durability}.partial.json`))

  const report = await run(engine, durability, config, values.features === true, environment, partial => {
    writeJson(partialPath, partial)
  })

  const path = writeJson(join(directory, artifact), report)
  if (existsSync(partialPath)) {
    unlinkSync(partialPath)
  }

  const failed = report.failed_workloads as Record<string, unknown>[]
  if (failed.length > 0) {
    const names = failed.map(entry => String(entry.workload)).join(', ')
    process.stderr.write(
      `Recorded ${engine} (${durability} durability) to ${path} with ${failed.length} failed workload(s): ${names}\n`,
    )
    return 3
  }
  process.stdout.write(`Recorded ${engine} (${durability} durability) to ${path}\n`)
  return 0
}

main(process.argv.slice(2))
  .then(code => {
    process.exitCode = code
  })
  .catch((err: unknown) => {
    process.stderr.write(`${describeError(err)}\n`)
    process.exitCode = 1
  })
