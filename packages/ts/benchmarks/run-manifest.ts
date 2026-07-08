import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import type { SystemInfo } from './config'

export const HARNESS_VERSION = '0.1.0'

/**
 * A published run is a self-contained directory keyed by a compact UTC run id, so the
 * newest run is the lexicographic maximum. The manifest carries the machine provenance
 * for the run; the sibling result files carry the numbers. The writeup generator reads
 * the latest committed run directory, so publishing numbers means committing one of these
 * directories alongside the regenerated page.
 */
export interface RunManifest {
  runId: string
  createdAt: string
  harnessVersion: string
  category: string
  machineLabel: string
  gitCommit: string
  gitDirty: boolean
  environment: {
    os: string
    cpu: string
    cpuCores: number
    ramGb: number
    nodeVersion: string
    v8Version: string
  }
  config: {
    durability: string
    seed: string
    driverName: string
  }
  engines: {
    sqliteVersion: string
    postgresVersion: string
  }
}

const RUN_ID_PATTERN = /^[0-9]{8}T[0-9]{6}Z$/

/**
 * Compact UTC timestamp with no separators inside the date or time, matching the run-id
 * convention the writeup generator sorts on. Derived from a caller-supplied ISO instant so
 * every artifact of one run shares a single timestamp.
 */
export function runIdFromIso(iso: string): string {
  const runId = iso.replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z')
  if (!RUN_ID_PATTERN.test(runId)) {
    throw new Error(`run id derived from '${iso}' is not a compact UTC timestamp: ${runId}`)
  }
  return runId
}

export function buildManifest(params: {
  runId: string
  createdAt: string
  category: string
  system: SystemInfo
}): RunManifest {
  const { runId, createdAt, category, system } = params
  return {
    runId,
    createdAt,
    harnessVersion: HARNESS_VERSION,
    category,
    machineLabel: system.machineLabel,
    gitCommit: system.gitCommit,
    gitDirty: system.gitDirty,
    environment: {
      os: system.os,
      cpu: system.cpu,
      cpuCores: system.cpuCores,
      ramGb: system.ramGb,
      nodeVersion: system.nodeVersion,
      v8Version: system.v8Version,
    },
    config: {
      durability: system.durability,
      seed: system.seed,
      driverName: system.driverName,
    },
    engines: {
      sqliteVersion: system.sqliteVersion,
      postgresVersion: system.postgresVersion,
    },
  }
}

/**
 * Create the run directory under results/runs/<run-id> and write the manifest. Returns the
 * absolute directory so callers can drop their result JSON files beside the manifest.
 */
export function createRunDirectory(resultsDir: string, manifest: RunManifest): string {
  const runDir = join(resultsDir, 'runs', manifest.runId)
  mkdirSync(runDir, { recursive: true })
  writeFileSync(join(runDir, 'run.json'), `${JSON.stringify(manifest, null, 2)}\n`)
  return runDir
}

export function writeRunArtifact(runDir: string, filename: string, data: unknown): string {
  const path = join(runDir, filename)
  writeFileSync(path, `${JSON.stringify(data, null, 2)}\n`)
  return path
}
