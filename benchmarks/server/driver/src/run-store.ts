// Own the on-disk layout of a benchmark run. A published run is a self-contained directory keyed
// by a compact UTC run id, so the newest run is the lexicographic maximum. The manifest carries
// the machine provenance; the sibling result files carry the numbers. All engines and durability
// passes of one run share a single run id threaded through the environment, so they land together.
// The Python aggregate step reads this same layout to build the cross-engine comparison.

import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'

const RUN_ID_ENV = 'BENCH_RUN_ID'
const RUNS_DIRNAME = 'runs'
export const MANIFEST_NAME = 'run.json'

// A run id and an artifact name both become one directory or file segment under the results tree,
// so they must not start a hidden entry, climb out with a dot entry, carry a path separator, or
// smuggle a null byte. The leading-alphanumeric rule rejects '.', '..', and option-like leading
// dashes; the body allows only the characters a timestamp id and an artifact name actually use.
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

// Write pretty-printed JSON with a trailing newline through a temporary file, then rename it into
// place, so a reader never sees a half-written file and a crash cannot leave a truncated result.
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
