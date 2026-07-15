import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { arch, cpus, type as osType, release, totalmem } from 'node:os'

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

function ownCgroupPath(): string | null {
  try {
    for (const line of readFileSync('/proc/self/cgroup', 'utf-8').split('\n')) {
      if (line.startsWith('0::')) {
        return line.slice(3).trim()
      }
    }
    return null
  } catch {
    return null
  }
}

function cgroupValue(cgroupPath: string, file: string): string | null {
  try {
    return readFileSync(`/sys/fs/cgroup${cgroupPath}/${file}`, 'utf-8').trim()
  } catch {
    return null
  }
}

function envValue(name: string): string | null {
  const raw = process.env[name]
  return raw !== undefined && raw.trim() !== '' ? raw : null
}

function cpusetSize(spec: string): number {
  const selected = new Set<number>()
  for (const part of spec.split(',')) {
    const token = part.trim()
    if (token === '') {
      continue
    }
    if (token.includes('-')) {
      const bounds = token.split('-', 2).map(Number)
      const low = bounds[0]
      const high = bounds[1]
      if (low === undefined || high === undefined || !Number.isInteger(low) || !Number.isInteger(high) || high < low) {
        return 0
      }
      for (let cpu = low; cpu <= high; cpu += 1) {
        selected.add(cpu)
      }
    } else {
      const cpu = Number(token)
      if (!Number.isInteger(cpu)) {
        return 0
      }
      selected.add(cpu)
    }
  }
  return selected.size
}

// cgroup-v2 exposes these files even for an unrestricted process, so only a real cap counts.
function resourceControl(): Record<string, unknown> {
  const cgroupPath = ownCgroupPath()
  const driverCpus = cgroupPath === null ? null : cgroupValue(cgroupPath, 'cpuset.cpus.effective')
  const driverMemoryMax = cgroupPath === null ? null : cgroupValue(cgroupPath, 'memory.max')
  const pinnedCpuCount = driverCpus === null ? null : cpusetSize(driverCpus)
  const cpusPinned = pinnedCpuCount !== null && pinnedCpuCount > 0 && pinnedCpuCount < cpus().length
  const memoryCapped = driverMemoryMax !== null && driverMemoryMax !== 'max'
  return {
    mechanism: cpusPinned || memoryCapped ? 'cgroup-v2' : 'none',
    driver_allowed_cpus: driverCpus,
    driver_memory_max: driverMemoryMax,
    engine_allowed_cpus: envValue('BENCH_ENGINE_CPUSET'),
    engine_memory_max: envValue('BENCH_ENGINE_MEMORY'),
  }
}

export function captureEnvironment(harnessVersion: string): Record<string, unknown> {
  const provenance = gitProvenance()
  const machineLabel = process.env.BENCH_MACHINE_LABEL
  return {
    captured_at: new Date().toISOString(),
    harness_version: harnessVersion,
    resource_control: resourceControl(),
    machine_label: machineLabel?.trim() ? machineLabel : null,
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
