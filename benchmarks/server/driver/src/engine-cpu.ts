import { readFileSync } from 'node:fs'
import { median } from './stats.ts'

function usageUsec(cgroupDir: string): number | null {
  try {
    const match = readFileSync(`${cgroupDir}/cpu.stat`, 'utf8').match(/^usage_usec (\d+)$/m)
    return match ? Number(match[1]) : null
  } catch {
    return null
  }
}

export async function withEngineCores<T>(
  cgroupDir: string | null,
  task: () => Promise<T>,
): Promise<{ result: T; coresUsed: number | null }> {
  if (cgroupDir === null) {
    return { result: await task(), coresUsed: null }
  }
  const before = usageUsec(cgroupDir)
  const started = performance.now()
  const result = await task()
  const elapsedMs = performance.now() - started
  const after = usageUsec(cgroupDir)
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
