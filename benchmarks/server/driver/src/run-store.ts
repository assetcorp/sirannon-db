import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'

const RUN_ID_ENV = 'BENCH_RUN_ID'
const RUNS_DIRNAME = 'runs'
export const MANIFEST_NAME = 'run.json'

// The leading-alphanumeric class rejects '.' and '..'; widening it reopens path traversal.
const SEGMENT = /^[0-9A-Za-z][0-9A-Za-z._-]{0,63}$/

function pad(value: number, width: number): string {
  return String(value).padStart(width, '0')
}

export function mintRunId(): string {
  const now = new Date()
  return (
    `${pad(now.getUTCFullYear(), 4)}${pad(now.getUTCMonth() + 1, 2)}${pad(now.getUTCDate(), 2)}` +
    `T${pad(now.getUTCHours(), 2)}${pad(now.getUTCMinutes(), 2)}${pad(now.getUTCSeconds(), 2)}Z`
  )
}

export function defaultResultsDir(): string {
  const override = process.env.BENCH_RESULTS_DIR
  return override && override.trim() ? override : join(process.cwd(), 'results')
}

function validateSegment(value: string, kind: string): string {
  if (!SEGMENT.test(value)) {
    throw new Error(
      `invalid ${kind} ${JSON.stringify(value)}: expected 1-64 characters of letters, digits, dot, ` +
        'dash, or underscore, not starting with a dot or dash',
    )
  }
  return value
}

export function validateRunId(runId: string): string {
  return validateSegment(runId, 'run id')
}

export function validateArtifactName(name: string): string {
  return validateSegment(name, 'artifact name')
}

export function runDirectory(resultsDir: string, runId: string): string {
  validateRunId(runId)
  const root = resolve(resultsDir, RUNS_DIRNAME)
  const candidate = resolve(root, runId)
  if (candidate !== root && !candidate.startsWith(root + '/')) {
    throw new Error(`run id ${JSON.stringify(runId)} resolves outside the results directory`)
  }
  return join(resultsDir, RUNS_DIRNAME, runId)
}

export function resolveRunIdForWrite(explicit: string | null): string {
  const raw = (explicit ?? process.env[RUN_ID_ENV] ?? '').trim()
  return raw ? validateRunId(raw) : mintRunId()
}

export function writeJson(path: string, payload: unknown): string {
  mkdirSync(dirname(path), { recursive: true })
  const temporary = join(dirname(path), `.${basename(path)}.tmp`)
  writeFileSync(temporary, `${JSON.stringify(payload, null, 2)}\n`, 'utf-8')
  renameSync(temporary, path)
  return path
}

function basename(path: string): string {
  const index = path.lastIndexOf('/')
  return index >= 0 ? path.slice(index + 1) : path
}

export function writeRunManifest(
  resultsDir: string,
  runId: string,
  environment: Record<string, unknown>,
  config: Record<string, unknown>,
): string {
  const directory = runDirectory(resultsDir, runId)
  mkdirSync(directory, { recursive: true })
  const path = join(directory, MANIFEST_NAME)
  let createdAt = environment.captured_at
  if (existsSync(path)) {
    try {
      const prior = JSON.parse(readFileSync(path, 'utf-8')) as Record<string, unknown>
      createdAt = prior.created_at ?? createdAt
    } catch {
      createdAt = environment.captured_at
    }
  }
  const manifest = {
    run_id: runId,
    created_at: createdAt,
    harness_version: environment.harness_version,
    environment,
    config,
  }
  writeJson(path, manifest)
  return path
}
