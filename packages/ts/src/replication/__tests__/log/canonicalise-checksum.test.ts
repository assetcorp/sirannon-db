import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { LWWResolver } from '../../../core/sync/conflict/lww.js'
import { HLC } from '../../../core/sync/hlc.js'
import { canonicaliseForChecksum, ReplicationLog } from '../../log.js'
import type { ReplicationBatch, ReplicationChange } from '../../types.js'
import { createTestDb, NODE_A, NODE_B, setupTrackerAndTable } from './helpers.js'

describe('ReplicationLog', () => {
  let conn: SQLiteConnection
  let hlcA: HLC
  let log: ReplicationLog

  beforeEach(async () => {
    conn = await createTestDb()
    hlcA = new HLC(NODE_A)
    await setupTrackerAndTable(conn)
    log = new ReplicationLog(conn, NODE_A, hlcA)
    await log.ensureReplicationTables()
  })

  afterEach(async () => {
    await conn.close()
  })

  describe('canonicaliseForChecksum', () => {
    it('does not throw on BigInt values (unlike raw JSON.stringify)', () => {
      const value = { id: 1n, label: 'x' }
      expect(() => JSON.stringify(value)).toThrow()
      expect(() => canonicaliseForChecksum(value)).not.toThrow()
    })

    it('produces the same string for an integer number and the equivalent BigInt', () => {
      const numberInput = { id: 42, count: 7 }
      const bigintInput = { id: 42n, count: 7n }
      expect(canonicaliseForChecksum(numberInput)).toBe(canonicaliseForChecksum(bigintInput))
    })

    it('distinguishes adjacent BigInt values that a JS number cannot separate', () => {
      const even = canonicaliseForChecksum({ balance: 9007199254740992n })
      const odd = canonicaliseForChecksum({ balance: 9007199254740993n })
      expect(even).not.toBe(odd)
      expect(Number(9007199254740992n)).toBe(Number(9007199254740993n))
    })

    it('produces the same string for a mixed number-and-BigInt object as for the all-BigInt equivalent', () => {
      const mixed = { id: 1, count: 99n, label: 'x' }
      const allBigint = { id: 1n, count: 99n, label: 'x' }
      expect(canonicaliseForChecksum(mixed)).toBe(canonicaliseForChecksum(allBigint))
    })

    it('produces the same string for Uint8Array and Buffer with identical bytes', () => {
      const bytes = [0xde, 0xad, 0xbe, 0xef]
      const arr = new Uint8Array(bytes)
      const buf = Buffer.from(bytes)
      expect(canonicaliseForChecksum({ blob: arr })).toBe(canonicaliseForChecksum({ blob: buf }))
    })

    it('preserves float values as-is (non-integer numbers)', () => {
      const value = { ratio: 3.14 }
      expect(canonicaliseForChecksum(value)).toBe('{"ratio":3.14}')
    })

    it('preserves strings, booleans, and null without alteration', () => {
      const value = { name: 'alice', active: true, ghost: null }
      expect(canonicaliseForChecksum(value)).toBe('{"active":true,"ghost":null,"name":"alice"}')
    })

    it('omits properties with undefined values to match JSON.stringify semantics', () => {
      const withUndefined = { a: 1n, b: undefined }
      const without = { a: 1n }
      expect(canonicaliseForChecksum(withUndefined)).toBe(canonicaliseForChecksum(without))
    })

    it('produces stable output regardless of property insertion order', () => {
      const a = { x: 'first', y: 'second', z: 'third' }
      const b = { z: 'third', y: 'second', x: 'first' }
      expect(canonicaliseForChecksum(a)).toBe(canonicaliseForChecksum(b))
    })

    it('represents undefined elements inside arrays as null (matches JSON.stringify)', () => {
      const value = ['a', undefined, 'c']
      expect(canonicaliseForChecksum(value)).toBe('["a",null,"c"]')
    })
  })

  describe('checksum stability across wire-format round-trip', () => {
    it('produces matching checksums for primary-side and replica-side representations', async () => {
      const hlcVal = hlcA.now()

      const primarySideChange: ReplicationChange = {
        table: 'users',
        operation: 'insert',
        rowId: '1',
        primaryKey: { id: 1 },
        hlc: hlcVal,
        txId: 'tx-1',
        nodeId: NODE_A,
        newData: { id: 1, name: 'Alice', email: null },
        oldData: null,
      }
      const replicaSideChange: ReplicationChange = {
        table: 'users',
        operation: 'insert',
        rowId: '1',
        primaryKey: { id: 1n },
        hlc: hlcVal,
        txId: 'tx-1',
        nodeId: NODE_A,
        newData: { id: 1n, name: 'Alice', email: null },
        oldData: null,
      }

      const primaryChecksum = createHash('sha256')
        .update(canonicaliseForChecksum([primarySideChange]))
        .digest('hex')
      const replicaChecksum = createHash('sha256')
        .update(canonicaliseForChecksum([replicaSideChange]))
        .digest('hex')

      expect(primaryChecksum).toBe(replicaChecksum)
    })

    it('applyBatch accepts a batch whose values have been coerced to BigInt by the wire layer', async () => {
      const remoteHlc = new HLC(NODE_B).now()
      const changes: ReplicationChange[] = [
        {
          table: 'users',
          operation: 'insert',
          rowId: '7',
          primaryKey: { id: 7 },
          hlc: remoteHlc,
          txId: 'tx-remote',
          nodeId: NODE_B,
          newData: { id: 7, name: 'Remote', email: null },
          oldData: null,
        },
      ]
      const primaryChecksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const wireRoundtripped: ReplicationChange[] = changes.map(c => ({
        ...c,
        primaryKey: { id: 7n },
        newData: { id: 7n, name: 'Remote', email: null },
      }))

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-1-1`,
        fromSeq: 1n,
        toSeq: 1n,
        hlcRange: { min: remoteHlc, max: remoteHlc },
        changes: wireRoundtripped,
        checksum: primaryChecksum,
      }

      const result = await log.applyBatch(batch, new LWWResolver())
      expect(result.applied).toBe(1)
      expect(result.conflicts).toBe(0)
    })
  })
})
