import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  ReplicationAck,
  ReplicationBatch,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
} from '../../replication/types.js'

export type BatchHandler = (batch: ReplicationBatch, fromPeerId: string) => Promise<void>
export type AckHandler = (ack: ReplicationAck, fromPeerId: string) => void
export type ForwardHandler = (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>
export type PeerConnectedHandler = (peer: NodeInfo) => void
export type PeerDisconnectedHandler = (peerId: string) => void
export type SyncRequestHandler = (request: SyncRequest, fromPeerId: string) => Promise<void>
export type SyncBatchHandler = (batch: SyncBatch, fromPeerId: string) => Promise<void>
export type SyncCompleteHandler = (complete: SyncComplete, fromPeerId: string) => Promise<void>
export type SyncAckHandler = (ack: SyncAck, fromPeerId: string) => void
