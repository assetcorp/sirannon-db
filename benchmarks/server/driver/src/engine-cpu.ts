import { readFileSync } from 'node:fs'
import { request } from 'undici'
import { median } from './stats.ts'

const CPU_STAT_TIMEOUT_MS = 5_000

export type EngineCpuSource = { kind: 'cgroup'; directory: string } | { kind: 'http'; url: string } | null

export function engineCpuSource(cgroupDir: string | null, cpuUrl: string | null): EngineCpuSource {
  if (cgroupDir !== null) {
    return { kind: 'cgroup', directory: cgroupDir }
  }
  if (cpuUrl !== null) {
    return { kind: 'http', url: cpuUrl }
  }
  return null
}

function usageFromCpuStat(text: string): number | null {
  const match = text.match(/^usage_usec (\d+)$/m)
  return match ? Number(match[1]) : null
}

async function usageUsec(source: Exclude<EngineCpuSource, null>): Promise<number | null> {
  try {
    if (source.kind === 'cgroup') {
      return usageFromCpuStat(readFileSync(`${source.directory}/cpu.stat`, 'utf8'))
    }
    const response = await request(source.url, {
      headersTimeout: CPU_STAT_TIMEOUT_MS,
      bodyTimeout: CPU_STAT_TIMEOUT_MS,
    })
    const body = await response.body.text()
    return response.statusCode === 200 ? usageFromCpuStat(body) : null
  } catch {
    return null
  }
}

export async function withEngineCores<T>(
  source: EngineCpuSource,
  task: () => Promise<T>,
): Promise<{ result: T; coresUsed: number | null }> {
  if (source === null) {
    return { result: await task(), coresUsed: null }
  }
  const before = await usageUsec(source)
  const started = performance.now()
  const result = await task()
  const elapsedMs = performance.now() - started
  const after = await usageUsec(source)
  if (before === null || after === null || elapsedMs <= 0) {
    return { result, coresUsed: null }
  }
  return { result, coresUsed: (after - before) / (elapsedMs * 1000) }
}

export function engineCpuBlock(samples: Array<number | null>, coresAllowed: number): Record<string, unknown> | null {
  const usable = samples.filter((value): value is number => value !== null)
  if (usable.length === 0) {
    return null
  }
  return {
    cores_used: median(usable),
    cores_allowed: coresAllowed,
    samples: usable,
    note:
      "cores_used is the engine cgroup's cpu.stat usage divided by wall time over the measured span, " +
      'so it is the average number of cores the engine kept busy, not a peak.',
  }
}
