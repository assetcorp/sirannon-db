// Capture the machine and build provenance recorded with every run. Published numbers name the
// exact host they came from and the exact commit that produced them, so a reader can reproduce the
// run and a stale page can be told apart from a fresh one.

import { execFileSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { arch, cpus, release, totalmem, type as osType } from 'node:os'

function git(args: string[]): string | null {
  try {
    return execFileSync('git', args, { encoding: 'utf-8' }).trim()
  } catch {
    return null
  }
}

function gitProvenance(): { commit: string; dirty: boolean } {
  const commit = git(['rev-parse', 'HEAD'])
  if (commit === null) {
    return { commit: 'unknown', dirty: false }
  }
  const status = git(['status', '--porcelain'])
  return { commit, dirty: status !== null && status.length > 0 }
}

function cpuModel(): string | null {
  const first = cpus()[0]
  return first ? first.model : null
}

export function captureEnvironment(harnessVersion: string): Record<string, unknown> {
  const provenance = gitProvenance()
  const machineLabel = process.env.BENCH_MACHINE_LABEL
  return {
    captured_at: new Date().toISOString(),
    harness_version: harnessVersion,
    containerized: existsSync('/.dockerenv'),
    machine_label: machineLabel && machineLabel.trim() ? machineLabel : null,
    os: `${osType()} ${release()}`,
    arch: arch(),
    cpu_model: cpuModel(),
    logical_cpus: cpus().length,
    total_memory_bytes: totalmem(),
    node_version: process.version,
    git_commit: provenance.commit,
    git_dirty: provenance.dirty,
  }
}
