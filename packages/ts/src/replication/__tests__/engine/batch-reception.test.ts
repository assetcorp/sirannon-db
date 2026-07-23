import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { HLC } from '../../../core/sync/hlc.js'
import { ReplicationEngine } from '../../engine.js'
import { BatchValidationError } from '../../errors.js'
import { canonicaliseForChecksum } from '../../log.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationErrorEvent } from '../../types.js'
import {
  createDbAndConn,
  createHarness,
  type EngineTestHarness,
  makeConfig,
  NODE_A,
  NODE_B,
  teardownHarness,
} from './helpers.js'

const NODE_C = 'cccc0000cccc0000cccc0000cccc0000'

describe('ReplicationEngine', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  describe('batch receiving', () => {
    it('applies incoming batches and sends ACK', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig(harness.transport, { topology: new PrimaryReplicaTopology('replica') }),
      )
      await engine.start()
      harness.transport.addPeer(NODE_B, 'primary')

      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()
      const changes = [
        {
          table: 'users',
          operation: 'insert' as const,
          rowId: '100',
          primaryKey: { id: 100 },
          hlc: hlcVal,
          txId: 'remote-tx',
          nodeId: NODE_B,
          newData: { id: 100, name: 'Remote' },
          oldData: null,
        },
      ]
      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      await harness.transport.triggerBatchReceived(
        {
          sourceNodeId: NODE_B,
          batchId: `${NODE_B}-1-1`,
          fromSeq: 1n,
          toSeq: 1n,
          hlcRange: { min: hlcVal, max: hlcVal },
          changes,
          checksum,
        },
        NODE_B,
      )

      const stmt = await conn.prepare('SELECT name FROM users WHERE id = 100')
      const row = (await stmt.get()) as { name: string } | undefined
      expect(row?.name).toBe('Remote')

      expect(harness.transport.sentAcks.length).toBeGreaterThan(0)

      await engine.stop()
    })

    it('rejects batches with excessive clock drift', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig(harness.transport, {
          topology: new PrimaryReplicaTopology('replica'),
          maxClockDriftMs: 1000,
        }),
      )
      await engine.start()
      harness.transport.addPeer(NODE_B, 'primary')

      const farFutureMs = Date.now() + 100_000
      const wallHex = farFutureMs.toString(16).padStart(12, '0')
      const hlcVal = `${wallHex}-0000-${NODE_B}`

      const changes = [
        {
          table: 'users',
          operation: 'insert' as const,
          rowId: '200',
          primaryKey: { id: 200 },
          hlc: hlcVal,
          txId: 'drift-tx',
          nodeId: NODE_B,
          newData: { id: 200, name: 'Drifted' },
          oldData: null,
        },
      ]
      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const errorEvents: ReplicationErrorEvent[] = []
      engine.on('replication-error', (event: ReplicationErrorEvent) => {
        errorEvents.push(event)
      })

      await harness.transport.triggerBatchReceived(
        {
          sourceNodeId: NODE_B,
          batchId: `${NODE_B}-50-50`,
          fromSeq: 50n,
          toSeq: 50n,
          hlcRange: { min: hlcVal, max: hlcVal },
          changes,
          checksum,
        },
        NODE_B,
      )

      expect(errorEvents.length).toBe(1)
      expect(errorEvents[0].error).toBeInstanceOf(BatchValidationError)
      expect(errorEvents[0].operation).toBe('batch-received')
      expect(errorEvents[0].peerId).toBe(NODE_B)
      expect(errorEvents[0].recoverable).toBe(true)

      await engine.stop()
    })
  })

  describe('ACK tracking', () => {
    it('updates peer tracker on ACK received', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      harness.transport.addPeer(NODE_B)

      harness.transport.triggerAckReceived({ batchId: 'test-batch', ackedSeq: 5n, nodeId: NODE_B }, NODE_B)

      const status = engine.status()
      const peer = status.peers.find(p => p.nodeId === NODE_B)
      expect(peer?.lastAckedSeq).toBe(5n)

      await engine.stop()
    })

    it('rejects static-mode ACKs whose payload identity does not match the sender', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      const errorEvents: ReplicationErrorEvent[] = []
      engine.on('replication-error', (event: ReplicationErrorEvent) => {
        errorEvents.push(event)
      })
      await engine.start()

      harness.transport.addPeer(NODE_B)
      harness.transport.addPeer(NODE_C)
      harness.transport.triggerAckReceived({ batchId: 'spoofed-batch', ackedSeq: 5n, nodeId: NODE_C }, NODE_B)

      const status = engine.status()
      const claimedPeer = status.peers.find(p => p.nodeId === NODE_C)
      expect(claimedPeer?.lastAckedSeq).toBe(0n)
      expect(errorEvents).toHaveLength(1)
      expect(errorEvents[0].operation).toBe('ack-identity-mismatch')
      expect(errorEvents[0].peerId).toBe(NODE_B)
      expect((errorEvents[0].error as { code?: string }).code).toBe('ACK_NODE_ID_MISMATCH')

      await engine.stop()
    })

    it('rejects static-mode sync ACKs whose payload identity does not match the sender', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      const errorEvents: ReplicationErrorEvent[] = []
      engine.on('replication-error', (event: ReplicationErrorEvent) => {
        errorEvents.push(event)
      })
      await engine.start()

      harness.transport.triggerSyncAckReceived(
        {
          requestId: 'sync-1',
          joinerNodeId: NODE_A,
          table: 'users',
          batchIndex: 0,
          success: true,
        },
        NODE_B,
      )

      expect(errorEvents).toHaveLength(1)
      expect(errorEvents[0].operation).toBe('sync-ack-identity-mismatch')
      expect(errorEvents[0].peerId).toBe(NODE_B)
      expect((errorEvents[0].error as { code?: string }).code).toBe('SYNC_ACK_NODE_ID_MISMATCH')

      await engine.stop()
    })
  })
})
