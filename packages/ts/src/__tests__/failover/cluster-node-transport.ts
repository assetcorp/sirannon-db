import type { ReplicationAck, SyncBatch, SyncRequest } from '../../replication/types.js'
import type { GrpcReplicationTransport } from '../../transport/grpc/index.js'
import {
  parseAck,
  parseBatch,
  parseForwardRequest,
  parseSyncBatch,
  parseSyncRequest,
  stringPayload,
} from './cluster-node-payloads.js'
import { CopyTrafficHold } from './copy-traffic-hold.js'

export const copyTraffic = new CopyTrafficHold()
const receivedCopyRequests = new Map<string, number>()

export function proxiedTransportClass(
  Base: typeof GrpcReplicationTransport,
  endpoints: Record<string, string>,
): typeof GrpcReplicationTransport {
  return class extends Base {
    override onSyncRequested(handler: Parameters<GrpcReplicationTransport['onSyncRequested']>[0]): void {
      super.onSyncRequested((request, peerId) => {
        const handling = handler(request, peerId)
        if (!request.historyTail) receivedCopyRequests.set(peerId, (receivedCopyRequests.get(peerId) ?? 0) + 1)
        return handling
      })
    }

    override async requestSync(peerId: string, request: SyncRequest): Promise<void> {
      if (!request.historyTail) await copyTraffic.pass('request', peerId)
      return super.requestSync(peerId, request)
    }

    override async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
      await copyTraffic.pass('batch', peerId)
      return super.sendSyncBatch(peerId, batch)
    }

    override async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
      await copyTraffic.pass('ack', peerId)
      return super.sendAck(peerId, ack)
    }

    override setPeerEndpoints(known: ReadonlyMap<string, string>): void {
      const proxied = new Map<string, string>()
      for (const nodeId of known.keys()) {
        const proxy = endpoints[nodeId]
        if (proxy !== undefined) proxied.set(nodeId, proxy)
      }
      super.setPeerEndpoints(proxied)
    }
  }
}

export async function handleTransportCommand(
  transport: GrpcReplicationTransport,
  command: string,
  payload: Record<string, unknown>,
): Promise<number | null | undefined> {
  if (command === 'copyTraffic') {
    if (payload.action === 'received') return receivedCopyRequests.get(stringPayload(payload, 'peerId')) ?? 0
    return copyTraffic.configure(
      stringPayload(payload, 'action'),
      typeof payload.kind === 'string' ? payload.kind : undefined,
      typeof payload.peerId === 'string' ? payload.peerId : undefined,
    )
  }
  if (command === 'sendRawBatch') {
    await transport.send(stringPayload(payload, 'peerId'), parseBatch(payload.batch))
  } else if (command === 'sendRawAck') {
    await transport.sendAck(stringPayload(payload, 'peerId'), parseAck(payload.ack))
  } else if (command === 'requestRawSync') {
    await transport.requestSync(stringPayload(payload, 'peerId'), parseSyncRequest(payload.request))
  } else if (command === 'sendRawSyncBatch') {
    await transport.sendSyncBatch(stringPayload(payload, 'peerId'), parseSyncBatch(payload.batch))
  } else if (command === 'sendRawForward') {
    await transport.forward(stringPayload(payload, 'peerId'), parseForwardRequest(payload.request))
  } else {
    return undefined
  }
  return null
}
