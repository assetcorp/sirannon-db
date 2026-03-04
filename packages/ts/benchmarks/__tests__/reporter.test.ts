import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { escapeCsvField, formatTable } from '../reporter'

describe('escapeCsvField', () => {
  it('returns plain strings unchanged', () => {
    expect(escapeCsvField('hello')).toBe('hello')
    expect(escapeCsvField('point-select')).toBe('point-select')
  })

  it('returns numbers as strings', () => {
    expect(escapeCsvField(42)).toBe('42')
    expect(escapeCsvField(3.14)).toBe('3.14')
  })

  it('wraps fields containing commas in quotes', () => {
    expect(escapeCsvField('mixed-50/50,rw')).toBe('"mixed-50/50,rw"')
  })

  it('wraps fields containing double quotes and escapes them', () => {
    expect(escapeCsvField('say "hello"')).toBe('"say ""hello"""')
  })

  it('wraps fields containing newlines in quotes', () => {
    expect(escapeCsvField('line1\nline2')).toBe('"line1\nline2"')
  })

  it('handles fields with multiple special characters', () => {
    expect(escapeCsvField('a,"b"\nc')).toBe('"a,""b""\nc"')
  })
})

describe('formatTable', () => {
  it('shows Speedup and CI before absolute ops/s columns', () => {
    const results = [
      {
        workload: 'point-select',
        dataSize: 1000,
        sirannon: {
          name: 'sirannon',
          opsPerSec: 100_000,
          meanNs: 10_000,
          p50Ns: 9_000,
          p75Ns: 11_000,
          p99Ns: 20_000,
          p999Ns: 50_000,
          minNs: 5_000,
          maxNs: 100_000,
          sdNs: 3_000,
          cv: 0.03,
          moe: 500,
          samples: 1000,
        },
        postgres: {
          name: 'postgres',
          opsPerSec: 50_000,
          meanNs: 20_000,
          p50Ns: 18_000,
          p75Ns: 22_000,
          p99Ns: 40_000,
          p999Ns: 80_000,
          minNs: 10_000,
          maxNs: 200_000,
          sdNs: 6_000,
          cv: 0.03,
          moe: 1000,
          samples: 1000,
        },
        speedup: 2.0,
        framing: '2.0x faster',
      },
    ]

    const table = formatTable(results)
    const headerLine = table.split('\n').find(line => line.includes('Workload'))
    expect(headerLine).toBeDefined()
    if (!headerLine) return

    const speedupIdx = headerLine.indexOf('Speedup')
    const sirannonIdx = headerLine.indexOf('Sirannon ops/s')
    const postgresIdx = headerLine.indexOf('Postgres ops/s')

    expect(speedupIdx).toBeLessThan(sirannonIdx)
    expect(speedupIdx).toBeLessThan(postgresIdx)
  })
})

describe('control-server worker config includes durability', () => {
  it('passes durability to worker config in runConcurrentBenchmark', () => {
    const src = readFileSync(join(import.meta.dirname, '..', 'engine', 'control-server.ts'), 'utf-8')

    const workerConfigPattern = /const wConfig.*?=.*?\{[\s\S]*?durability:\s*DURABILITY[\s\S]*?\}/
    expect(src).toMatch(workerConfigPattern)
  })
})

describe('run-statistical runtime estimate uses DATA_SIZES count', () => {
  it('computes dataSizeCount from DATA_SIZES.split', () => {
    const src = readFileSync(join(import.meta.dirname, '..', 'run-statistical.ts'), 'utf-8')

    expect(src).toContain("DATA_SIZES.split(',').length")
    expect(src).toContain('dataSizeCount')
    expect(src).toMatch(/BENCHMARKS\.length\s*\*\s*dataSizeCount/)
    expect(src).not.toMatch(/BENCHMARKS\.length\s*\*\s*2\s*\*/)
  })
})

describe('run-engine CSV uses escapeCsvField for string fields', () => {
  it('imports escapeCsvField from reporter', () => {
    const src = readFileSync(join(import.meta.dirname, '..', 'run-engine.ts'), 'utf-8')

    expect(src).toMatch(/import\s*\{[^}]*escapeCsvField[^}]*\}\s*from\s*['"]\.\/reporter['"]/)
  })

  it('escapes workload and operation name fields in engine CSV', () => {
    const src = readFileSync(join(import.meta.dirname, '..', 'run-engine.ts'), 'utf-8')

    expect(src).toContain('escapeCsvField(sResult.workload)')
    expect(src).toContain('escapeCsvField(sr.name)')
  })

  it('escapes model and workload fields in scaling CSV', () => {
    const src = readFileSync(join(import.meta.dirname, '..', 'run-engine.ts'), 'utf-8')

    expect(src).toContain('escapeCsvField(sp.model)')
    expect(src).toContain('escapeCsvField(workloadLabel(sp.readRatio))')
  })
})
