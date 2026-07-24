import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { canonicaliseForChecksum } from '../../sync/canonicalise.js'
import { computeChecksum } from '../../sync/checksum.js'
import { FieldMergeResolver } from '../../sync/conflict/field-merge.js'
import { LWWResolver } from '../../sync/conflict/lww.js'
import { PrimaryWinsResolver } from '../../sync/conflict/primary-wins.js'
import { HLC } from '../../sync/hlc.js'
import { sha256Hex } from '../../sync/sha256.js'
import type { ConflictContext, ConflictResolution, HLCTimestamp, ReplicationChange } from '../../sync/types.js'

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
    it(`HLC encoding round-trips: ${v.expected}`, () => {
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
