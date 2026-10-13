import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const SPEC_VECTORS_DIR = join(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  '..',
  '..',
  '..',
  'spec',
  'test-vectors',
)
const SAFE_INTEGER_LIMIT = BigInt(Number.MAX_SAFE_INTEGER)
const HLC_WALL_DIGITS = 12
const HLC_LOGICAL_DIGITS = 4

export interface DecodedHlc {
  wallMs: number
  logical: number
  nodeId: string
}

export function canonicalForm(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (typeof value === 'string') return JSON.stringify(value)
  if (typeof value === 'bigint') return `{"__sint":"${value.toString(10)}"}`
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return 'null'
    if (Number.isInteger(value)) return `{"__sint":"${BigInt(value).toString(10)}"}`
    return JSON.stringify(value)
  }
  if (value instanceof Uint8Array) return `{"__blob":"${Buffer.from(value).toString('base64')}"}`
  if (Array.isArray(value)) return `[${value.map(canonicalForm).join(',')}]`
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, field]) => field !== undefined)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
  return `{${entries.map(([key, field]) => `${JSON.stringify(key)}:${canonicalForm(field)}`).join(',')}}`
}

export function specChecksum(value: unknown): string {
  return createHash('sha256').update(canonicalForm(value), 'utf8').digest('hex')
}

export function wireValue(value: unknown): unknown {
  if (typeof value === 'bigint') {
    if (value > SAFE_INTEGER_LIMIT || value < -SAFE_INTEGER_LIMIT) return { __sirannon_int: value.toString(10) }
    return Number(value)
  }
  if (value instanceof Uint8Array) return { __sirannon_blob: Buffer.from(value).toString('hex').toUpperCase() }
  return value
}

export function wireRow(row: Record<string, unknown> | null): Record<string, unknown> | null {
  if (row === null) return null
  return Object.fromEntries(Object.entries(row).map(([column, value]) => [column, wireValue(value)]))
}

export function decodeWireValue(value: unknown): unknown {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return value
  const keys = Object.keys(value)
  const record = value as Record<string, unknown>
  if (keys.length === 1 && typeof record.__sirannon_int === 'string') return BigInt(record.__sirannon_int)
  if (keys.length === 1 && typeof record.__sirannon_blob === 'string') {
    return new Uint8Array(Buffer.from(record.__sirannon_blob, 'hex'))
  }
  return value
}

export function decodeWireRow(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(row).map(([column, value]) => [column, decodeWireValue(value)]))
}

export function encodeHlc(wallMs: number, logical: number, nodeId: string): string {
  const wall = wallMs.toString(16).padStart(HLC_WALL_DIGITS, '0')
  const counter = logical.toString(16).padStart(HLC_LOGICAL_DIGITS, '0')
  return `${wall}-${counter}-${nodeId}`
}

export function decodeHlc(encoded: string): DecodedHlc | null {
  const parts = encoded.split('-')
  if (parts.length < 3) return null
  const [wall, counter, ...node] = parts
  if (!/^[0-9a-f]+$/.test(wall) || !/^[0-9a-f]+$/.test(counter)) return null
  return { wallMs: Number.parseInt(wall, 16), logical: Number.parseInt(counter, 16), nodeId: node.join('-') }
}

interface ChecksumVectors {
  batch_checksum: Array<{ changes: unknown; canonical: string; expected: string }>
  sync_batch_checksum: Array<{ rows: unknown; canonical: string; expected: string }>
}

interface HlcVectors {
  encoding: Array<{ input: DecodedHlc; expected: string }>
  decoding: Array<{ input: string; expected?: DecodedHlc; expected_error?: boolean }>
}

function readVectors<T>(name: string): T {
  return JSON.parse(readFileSync(join(SPEC_VECTORS_DIR, name), 'utf8')) as T
}

export function oracleDisagreementsWithSpecVectors(): string[] {
  const disagreements: string[] = []
  const checksums = readVectors<ChecksumVectors>('checksum.json')
  const payloads = [
    ...checksums.batch_checksum.map(vector => ({ payload: vector.changes, ...vector })),
    ...checksums.sync_batch_checksum.map(vector => ({ payload: vector.rows, ...vector })),
  ]
  for (const vector of payloads) {
    if (canonicalForm(vector.payload) !== vector.canonical) disagreements.push(`canonical ${vector.canonical}`)
    if (specChecksum(vector.payload) !== vector.expected) disagreements.push(`checksum ${vector.expected}`)
  }
  const clocks = readVectors<HlcVectors>('hlc.json')
  for (const vector of clocks.encoding) {
    const { wallMs, logical, nodeId } = vector.input
    if (encodeHlc(wallMs, logical, nodeId) !== vector.expected) disagreements.push(`encode ${vector.expected}`)
  }
  for (const vector of clocks.decoding) {
    const decoded = decodeHlc(vector.input)
    const expected = vector.expected_error === true ? null : (vector.expected ?? null)
    if (JSON.stringify(decoded) !== JSON.stringify(expected)) disagreements.push(`decode ${vector.input}`)
  }
  return disagreements
}
