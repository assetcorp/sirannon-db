import { existsSync, statSync } from 'node:fs'

export function sameMembers(actual: readonly string[], expected: readonly string[]): boolean {
  if (actual.length !== expected.length) return false
  const actualSet = new Set(actual)
  return expected.every(value => actualSet.has(value))
}

export function zip<T, U>(left: readonly T[], right: readonly U[]): Array<[T, U]> {
  return left.map((value, index) => {
    const paired = right[index]
    if (paired === undefined) {
      throw new Error('Cannot zip arrays of different lengths')
    }
    return [value, paired]
  })
}

export function fileSize(path: string): number {
  return existsSync(path) ? statSync(path).size : 0
}

export function jsonReplacer(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? value.toString() : value
}

export async function waitForCondition(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastError: Error | null = null

  while (Date.now() < deadline) {
    try {
      if (await predicate()) return
    } catch (err: unknown) {
      lastError = err instanceof Error ? err : new Error(String(err))
    }
    await sleep(50)
  }

  if (lastError) throw lastError
  throw new Error(`Condition was not met within ${timeoutMs}ms`)
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms) as ReturnType<typeof setTimeout> & {
      unref?: () => void
    }
    timer.unref?.()
  })
}
