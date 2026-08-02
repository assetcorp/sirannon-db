import { Metadata, type ServiceError } from '@grpc/grpc-js'
import { TransportError } from '../../replication/errors.js'
import type { ForwardedTransaction, ForwardedTransactionResult } from '../../replication/types.js'
import { toForwardRequest } from './codec.js'
import type { ForwardResponse as ProtoForwardResponse } from './generated/replication.js'
import type { ClientPeerEntry } from './peer-streams.js'

export function forwardOverRpc(
  entry: ClientPeerEntry,
  request: ForwardedTransaction,
  deadlineMs: number,
): Promise<ForwardedTransactionResult> {
  const protoRequest = toForwardRequest(request)
  const deadline = new Date(Date.now() + deadlineMs)

  return new Promise<ForwardedTransactionResult>((resolve, reject) => {
    entry.client.forward(
      protoRequest,
      new Metadata(),
      { deadline },
      (err: ServiceError | null, response: ProtoForwardResponse | undefined) => {
        if (err) {
          reject(new TransportError(`Forward RPC failed: ${err.message}`))
          return
        }
        if (!response) {
          reject(new TransportError('Forward RPC returned empty response'))
          return
        }
        if (response.error) {
          reject(new TransportError(`Forward RPC error: ${response.error}`))
          return
        }
        resolve({
          requestId: response.requestId,
          results: response.results.map(r => ({
            changes: r.changes,
            lastInsertRowId: Number(r.lastInsertRowId),
          })),
          groupId: response.groupId || undefined,
          primaryTerm: response.primaryTerm === 0n ? undefined : response.primaryTerm,
        })
      },
    )
  })
}
