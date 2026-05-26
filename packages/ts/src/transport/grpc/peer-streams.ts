import type { ClientDuplexStream, ServerDuplexStream } from '@grpc/grpc-js'
import type { NodeInfo, TopologyRole } from '../../replication/types.js'
import type { ReplicationClient, ReplicationMessage, SyncMessage } from './generated/replication.js'

export interface PeerStreamEntry {
  replicateStream: ServerDuplexStream<ReplicationMessage, ReplicationMessage> | null
  syncStream: ServerDuplexStream<SyncMessage, SyncMessage> | null
}

export interface ClientPeerEntry {
  client: InstanceType<typeof ReplicationClient>
  replicateStream: ClientDuplexStream<ReplicationMessage, ReplicationMessage> | null
  syncStream: ClientDuplexStream<SyncMessage, SyncMessage> | null
}

export type BatchHandler = (
  batch: import('../../replication/types.js').ReplicationBatch,
  fromPeerId: string,
) => Promise<void>
export type AckHandler = (ack: import('../../replication/types.js').ReplicationAck, fromPeerId: string) => void
export type ForwardHandler = (
  request: import('../../replication/types.js').ForwardedTransaction,
  fromPeerId: string,
) => Promise<import('../../replication/types.js').ForwardedTransactionResult>
export type PeerConnectedHandler = (peer: NodeInfo) => void
export type PeerDisconnectedHandler = (peerId: string) => void
export type SyncRequestHandler = (
  request: import('../../replication/types.js').SyncRequest,
  fromPeerId: string,
) => Promise<void>
export type SyncBatchHandler = (
  batch: import('../../replication/types.js').SyncBatch,
  fromPeerId: string,
) => Promise<void>
export type SyncCompleteHandler = (
  complete: import('../../replication/types.js').SyncComplete,
  fromPeerId: string,
) => Promise<void>
export type SyncAckHandler = (ack: import('../../replication/types.js').SyncAck, fromPeerId: string) => void

export function registerPeer(
  connectedPeers: Map<string, NodeInfo>,
  peerConnectedHandler: PeerConnectedHandler | null,
  nodeId: string,
  role: TopologyRole,
): void {
  if (connectedPeers.has(nodeId)) return
  const peerInfo: NodeInfo = {
    id: nodeId,
    role,
    joinedAt: Date.now(),
    lastSeenAt: Date.now(),
    lastAckedSeq: 0n,
  }
  connectedPeers.set(nodeId, peerInfo)
  peerConnectedHandler?.(peerInfo)
}

export function removePeer(
  connectedPeers: Map<string, NodeInfo>,
  serverPeerStreams: Map<string, PeerStreamEntry>,
  peerDisconnectedHandler: PeerDisconnectedHandler | null,
  nodeId: string,
): void {
  if (!connectedPeers.has(nodeId)) return
  connectedPeers.delete(nodeId)
  serverPeerStreams.delete(nodeId)
  peerDisconnectedHandler?.(nodeId)
}
