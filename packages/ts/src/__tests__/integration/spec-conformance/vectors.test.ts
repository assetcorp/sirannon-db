import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { decodeTaggedValues } from '../../../core/cdc/encoding.js'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { canonicaliseForChecksum } from '../../../core/sync/canonicalise.js'
import { computeChecksum } from '../../../core/sync/checksum.js'
import { FieldMergeResolver } from '../../../core/sync/conflict/field-merge.js'
import { LWWResolver } from '../../../core/sync/conflict/lww.js'
import { PrimaryWinsResolver } from '../../../core/sync/conflict/primary-wins.js'
import { HLC } from '../../../core/sync/hlc.js'
import { sha256Hex } from '../../../core/sync/sha256.js'
import type { ConflictContext, ConflictResolution, HLCTimestamp, ReplicationChange } from '../../../core/sync/types.js'
import type { QueryWithColumnsOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'

const vectorsDir = join(dirname(fileURLToPath(import.meta.url)), '..', '..', '..', '..', '..', 'spec', 'test-vectors')

function loadVectors<T>(name: string): T {
  return JSON.parse(readFileSync(join(vectorsDir, name), 'utf8')) as T
}

interface ChecksumVectors {
  batch_checksum: { name: string; changes: ReplicationChange[]; canonical: string; expected: string }[]
  sync_batch_checksum: { name: string; rows: Record<string, unknown>[]; canonical: string; expected: string }[]
}

describe('spec checksum vectors', () => {
  const vectors = loadVectors<ChecksumVectors>('checksum.json')

  for (const v of vectors.batch_checksum) {
    it(`batch checksum: ${v.name}`, () => {
      expect(canonicaliseForChecksum(v.changes)).toBe(v.canonical)
      expect(computeChecksum(v.changes)).toBe(v.expected)
    })
  }

  for (const v of vectors.sync_batch_checksum) {
    it(`sync batch checksum: ${v.name}`, () => {
      expect(canonicaliseForChecksum(v.rows)).toBe(v.canonical)
      expect(sha256Hex(canonicaliseForChecksum(v.rows))).toBe(v.expected)
    })
  }
})

type HlcEncodeVector = { input: HLCTimestamp; expected: string; note?: string }
type HlcDecodeVector =
  | { input: string; expected: HLCTimestamp; note?: string }
  | { input: string; expected_error: true; note?: string }
type HlcCompareVector = { a: string; b: string; expected: number; reason?: string }

interface HlcVectors {
  encoding: HlcEncodeVector[]
  decoding: HlcDecodeVector[]
  comparison: HlcCompareVector[]
}

describe('spec HLC vectors', () => {
  const vectors = loadVectors<HlcVectors>('hlc.json')

  for (const v of vectors.encoding) {
    it(`HLC encoding: ${v.expected}`, () => {
      expect(HLC.encode(v.input.wallMs, v.input.logical, v.input.nodeId)).toBe(v.expected)
      expect(HLC.decode(v.expected)).toEqual(v.input)
    })
  }

  for (const v of vectors.decoding) {
    if ('expected_error' in v) {
      it(`HLC decoding rejects: ${v.input}`, () => {
        expect(() => HLC.decode(v.input)).toThrow()
      })
      continue
    }
    it(`HLC decoding: ${v.input}`, () => {
      expect(HLC.decode(v.input)).toEqual(v.expected)
    })
  }

  for (const v of vectors.comparison) {
    it(`HLC comparison: ${v.a} vs ${v.b}`, () => {
      expect(HLC.compare(v.a, v.b)).toBe(v.expected)
    })
  }
})

interface VectorChange {
  nodeId: string
  oldData?: Record<string, unknown>
  newData?: Record<string, unknown>
}

interface ConflictInput {
  table: string
  rowId: string
  localHlc: string | null
  remoteHlc: string
  localChange: VectorChange | null
  remoteChange: VectorChange
}

interface ConflictVectors {
  lww: { name: string; input: ConflictInput; expected: ConflictResolution; reason?: string }[]
  primary_wins: {
    name: string
    input: ConflictInput & { primaryNodeId: string }
    expected: ConflictResolution
    reason?: string
  }[]
  field_merge: {
    name: string
    input: ConflictInput & { columnVersions: Record<string, { hlc: string; nodeId: string }> }
    expected: ConflictResolution
    reason?: string
  }[]
}

function buildChange(v: VectorChange): ReplicationChange {
  return {
    table: '',
    operation: 'update',
    rowId: '',
    primaryKey: {},
    hlc: '',
    txId: '',
    nodeId: v.nodeId,
    newData: v.newData ?? null,
    oldData: v.oldData ?? null,
  }
}

function buildContext(input: ConflictInput): ConflictContext {
  return {
    table: input.table,
    rowId: input.rowId,
    localChange: input.localChange === null ? null : buildChange(input.localChange),
    remoteChange: buildChange(input.remoteChange),
    localHlc: input.localHlc,
    remoteHlc: input.remoteHlc,
  }
}

describe('spec conflict resolution vectors', () => {
  const vectors = loadVectors<ConflictVectors>('conflict-resolution.json')

  for (const v of vectors.lww) {
    it(`LWW: ${v.name}`, () => {
      expect(new LWWResolver().resolve(buildContext(v.input))).toEqual(v.expected)
    })
  }

  for (const v of vectors.primary_wins) {
    it(`PrimaryWins: ${v.name}`, () => {
      const resolver = new PrimaryWinsResolver(v.input.primaryNodeId)
      expect(resolver.resolve(buildContext(v.input))).toEqual(v.expected)
    })
  }

  for (const v of vectors.field_merge) {
    it(`FieldMerge: ${v.name}`, async () => {
      const columnVersions = new Map(Object.entries(v.input.columnVersions))
      const resolver = new FieldMergeResolver(() => Promise.resolve(columnVersions))
      expect(await resolver.resolve(buildContext(v.input))).toEqual(v.expected)
    })
  }
})

interface ColumnReadCase {
  limits: { maxRows?: number; maxBytes?: number }
  returned: number
  truncated: 'rows' | 'bytes' | null
}

interface ColumnReadVectors {
  valueBytes: { value: unknown; bytes: number }[]
  truncation: { rows: unknown[][]; cases: ColumnReadCase[] }
  emptyResult: ColumnReadCase[]
  validLimits: unknown[]
  invalidLimits: unknown[]
}

describe('spec column read vectors', () => {
  const vectors = loadVectors<ColumnReadVectors>('column-reads.json')
  let dir: string
  let sirannon: Sirannon
  let db: Database

  beforeAll(async () => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-column-vectors-'))
    sirannon = new Sirannon({ driver: betterSqlite3() })
    db = await sirannon.open('vectors', join(dir, 'vectors.db'))
    const width = vectors.truncation.rows[0]?.length ?? 1
    const names = Array.from({ length: width }, (_, index) => `c${index}`)
    await db.execute(`CREATE TABLE stored (${names.join(', ')})`)
    await db.execute(`CREATE TABLE empty (${names.join(', ')})`)
    for (const row of vectors.truncation.rows) {
      await db.execute(`INSERT INTO stored VALUES (${names.map(() => '?').join(', ')})`, row.map(decodeTaggedValues))
    }
  })

  afterAll(async () => {
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  for (const v of vectors.valueBytes) {
    it(`counts ${v.bytes} bytes for ${JSON.stringify(v.value)}`, async () => {
      const value = decodeTaggedValues(v.value)
      const fits = await db.queryWithColumns('SELECT ? AS v', [value], { maxBytes: v.bytes })
      const short = await db.queryWithColumns('SELECT ? AS v', [value], { maxBytes: v.bytes - 1 })
      expect([fits.rows.length, fits.truncated]).toEqual([1, null])
      expect([short.rows.length, short.truncated]).toEqual([0, 'bytes'])
    })
  }

  for (const v of vectors.truncation.cases) {
    it(`returns ${v.returned} stored rows under ${JSON.stringify(v.limits)}`, async () => {
      const read = await db.queryWithColumns('SELECT * FROM stored ORDER BY rowid', [], v.limits)
      expect([read.rows.length, read.truncated]).toEqual([v.returned, v.truncated])
    })
  }

  for (const v of vectors.emptyResult) {
    it(`returns ${v.returned} rows of an empty result under ${JSON.stringify(v.limits)}`, async () => {
      const read = await db.queryWithColumns('SELECT * FROM empty', [], v.limits)
      expect([read.rows.length, read.truncated]).toEqual([v.returned, v.truncated])
    })
  }

  for (const limit of vectors.validLimits) {
    it(`accepts ${JSON.stringify(limit)} as maxRows and maxBytes`, async () => {
      const options = { maxRows: limit, maxBytes: limit } as QueryWithColumnsOptions
      await expect(db.queryWithColumns('SELECT * FROM empty', [], options)).resolves.toMatchObject({ rows: [] })
    })
  }

  for (const limit of vectors.invalidLimits) {
    it(`refuses ${JSON.stringify(limit)} as maxRows and as maxBytes`, async () => {
      for (const key of ['maxRows', 'maxBytes']) {
        const options = { [key]: limit } as QueryWithColumnsOptions
        await expect(db.queryWithColumns('SELECT 1', [], options), key).rejects.toMatchObject({
          code: 'INVALID_READ_LIMIT',
        })
      }
    })
  }
})
