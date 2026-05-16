import { afterEach, describe, expect, it } from 'vitest'
import type { ForwardedTransaction } from '../../../../replication/types.js'
import type { GrpcReplicationTransport } from '../../index.js'
import { setupPrimaryReplica, teardown } from './_helpers.js'

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  describe('forward request/response', () => {
    it('forwards a write request from replica to primary with deadline', async () => {
      const { primary, replica } = await setupPrimaryReplica(transports)

      primary.onForwardReceived(async (request, _from) => {
        return {
          requestId: request.requestId,
          results: request.statements.map(() => ({
            changes: 1,
            lastInsertRowId: 42,
          })),
        }
      })

      const request: ForwardedTransaction = {
        requestId: 'fwd-1',
        statements: [{ sql: 'INSERT INTO users (name) VALUES (:name)', params: { ':name': 'Bob' } }],
      }

      const result = await replica.forward('primary-node', request)
      expect(result.requestId).toBe('fwd-1')
      expect(result.results.length).toBe(1)
      expect(result.results[0]?.changes).toBe(1)
      expect(result.results[0]?.lastInsertRowId).toBe(42)
    })

    it('propagates forward errors via the error field', async () => {
      const { primary, replica } = await setupPrimaryReplica(transports)

      primary.onForwardReceived(async () => {
        throw new Error('constraint violation: UNIQUE')
      })

      const request: ForwardedTransaction = {
        requestId: 'fwd-err',
        statements: [{ sql: 'INSERT INTO users (id) VALUES (1)' }],
      }

      await expect(replica.forward('primary-node', request)).rejects.toThrow(
        'Forward RPC error: constraint violation: UNIQUE',
      )
    })
  })
})
