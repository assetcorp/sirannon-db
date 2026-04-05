import { readFileSync } from 'node:fs'
import {
  type ChannelCredentials,
  type ClientDuplexStream,
  type ClientOptions,
  credentials,
  Metadata,
  Server,
  ServerCredentials,
  type ServerDuplexStream,
  type ServiceError,
  status,
} from '@grpc/grpc-js'
import { HealthImplementation } from 'grpc-health-check'
import { TransportError } from '../../replication/errors.js'
import type {
  ReplicationChange as AppReplicationChange,
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  ReplicationAck,
  ReplicationBatch,
  ReplicationTransport,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
  SyncTableManifest,
  TopologyRole,
  TransportConfig,
} from '../../replication/types.js'
import {
  type AckPayload,
  type BatchPayload,
  type ColumnValue,
  type ForwardRequest as ProtoForwardRequest,
  type ForwardResponse as ProtoForwardResponse,
  type ReplicationChange as ProtoReplicationChange,
  type SyncTableManifest as ProtoSyncTableManifest,
  ReplicationClient,
  type ReplicationMessage,
  ReplicationService,
  type RowData,
  type Statement,
  type SyncAckPayload,
  type SyncBatchPayload,
  type SyncCompletePayload,
  type SyncMessage,
  type SyncRequestPayload,
} from './generated/replication.js'

export interface GrpcReplicationOptions {
  host?: string
  port?: number
  tlsCert?: string
  tlsKey?: string
  tlsCaCert?: string
  insecure?: boolean
  forwardDeadlineMs?: number
}

type BatchHandler = (batch: ReplicationBatch, fromPeerId: string) => Promise<void>
type AckHandler = (ack: ReplicationAck, fromPeerId: string) => void
type ForwardHandler = (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>
type PeerConnectedHandler = (peer: NodeInfo) => void
type PeerDisconnectedHandler = (peerId: string) => void
type SyncRequestHandler = (request: SyncRequest, fromPeerId: string) => Promise<void>
type SyncBatchHandler = (batch: SyncBatch, fromPeerId: string) => Promise<void>
type SyncCompleteHandler = (complete: SyncComplete, fromPeerId: string) => Promise<void>
type SyncAckHandler = (ack: SyncAck, fromPeerId: string) => void

interface PeerStreamEntry {
  replicateStream: ServerDuplexStream<ReplicationMessage, ReplicationMessage> | null
  syncStream: ServerDuplexStream<SyncMessage, SyncMessage> | null
}

interface ClientPeerEntry {
  client: InstanceType<typeof ReplicationClient>
  replicateStream: ClientDuplexStream<ReplicationMessage, ReplicationMessage> | null
  syncStream: ClientDuplexStream<SyncMessage, SyncMessage> | null
}

export function toColumnValue(value: unknown): ColumnValue {
  if (value === null || value === undefined) {
    return { nullValue: true }
  }
  if (typeof value === 'string') {
    return { stringValue: value }
  }
  if (typeof value === 'bigint') {
    return { intValue: value }
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return { intValue: BigInt(value) }
    }
    return { floatValue: value }
  }
  if (typeof value === 'boolean') {
    return { boolValue: value }
  }
  if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
    return { blobValue: Buffer.from(value) }
  }
  throw new TypeError(
    `Unsupported value type for ColumnValue conversion: ${typeof value} (${Object.prototype.toString.call(value)})`,
  )
}

export function fromColumnValue(cv: ColumnValue): unknown {
  if (cv.nullValue !== undefined) return null
  if (cv.stringValue !== undefined) return cv.stringValue
  if (cv.intValue !== undefined) return cv.intValue
  if (cv.floatValue !== undefined) return cv.floatValue
  if (cv.blobValue !== undefined) return cv.blobValue
  if (cv.boolValue !== undefined) return cv.boolValue
  return null
}

function toRowData(record: Record<string, unknown> | null): RowData | undefined {
  if (record === null || record === undefined) return undefined
  const fields: Record<string, ColumnValue> = {}
  for (const [key, value] of Object.entries(record)) {
    fields[key] = toColumnValue(value)
  }
  return { fields }
}

function fromRowData(rowData: RowData | undefined): Record<string, unknown> | null {
  if (rowData === undefined) return null
  const result: Record<string, unknown> = {}
  for (const [key, cv] of Object.entries(rowData.fields)) {
    result[key] = fromColumnValue(cv)
  }
  return result
}

function toProtoChange(change: AppReplicationChange): ProtoReplicationChange {
  return {
    table: change.table,
    operation: change.operation,
    rowId: change.rowId,
    primaryKey: toRowData(change.primaryKey as Record<string, unknown>),
    hlc: change.hlc,
    txId: change.txId,
    nodeId: change.nodeId,
    newData: toRowData(change.newData),
    oldData: toRowData(change.oldData),
    ddlStatement: change.ddlStatement ?? '',
  }
}

function fromProtoChange(proto: ProtoReplicationChange): AppReplicationChange {
  return {
    table: proto.table,
    operation: proto.operation as AppReplicationChange['operation'],
    rowId: proto.rowId,
    primaryKey: (fromRowData(proto.primaryKey) ?? {}) as Record<string, unknown>,
    hlc: proto.hlc,
    txId: proto.txId,
    nodeId: proto.nodeId,
    newData: fromRowData(proto.newData),
    oldData: fromRowData(proto.oldData),
    ddlStatement: proto.ddlStatement || undefined,
  }
}

function toBatchPayload(batch: ReplicationBatch): BatchPayload {
  return {
    sourceNodeId: batch.sourceNodeId,
    batchId: batch.batchId,
    fromSeq: batch.fromSeq,
    toSeq: batch.toSeq,
    hlcRange: { min: batch.hlcRange.min, max: batch.hlcRange.max },
    changes: batch.changes.map(toProtoChange),
    checksum: batch.checksum,
  }
}

function fromBatchPayload(payload: BatchPayload): ReplicationBatch {
  return {
    sourceNodeId: payload.sourceNodeId,
    batchId: payload.batchId,
    fromSeq: payload.fromSeq,
    toSeq: payload.toSeq,
    hlcRange: {
      min: payload.hlcRange?.min ?? '',
      max: payload.hlcRange?.max ?? '',
    },
    changes: payload.changes.map(fromProtoChange),
    checksum: payload.checksum,
  }
}

function toAckPayload(ack: ReplicationAck): AckPayload {
  return {
    batchId: ack.batchId,
    ackedSeq: ack.ackedSeq,
    nodeId: ack.nodeId,
  }
}

function fromAckPayload(payload: AckPayload): ReplicationAck {
  return {
    batchId: payload.batchId,
    ackedSeq: payload.ackedSeq,
    nodeId: payload.nodeId,
  }
}

function toSyncRequestPayload(req: SyncRequest): SyncRequestPayload {
  return {
    requestId: req.requestId,
    joinerNodeId: req.joinerNodeId,
    completedTables: req.completedTables,
  }
}

function fromSyncRequestPayload(p: SyncRequestPayload): SyncRequest {
  return {
    requestId: p.requestId,
    joinerNodeId: p.joinerNodeId,
    completedTables: p.completedTables,
  }
}

function toSyncBatchPayload(batch: SyncBatch): SyncBatchPayload {
  return {
    requestId: batch.requestId,
    table: batch.table,
    batchIndex: batch.batchIndex,
    rows: batch.rows.map(row => toRowData(row) ?? { fields: {} }),
    schema: batch.schema ?? [],
    checksum: batch.checksum,
    isLastBatchForTable: batch.isLastBatchForTable,
  }
}

function fromSyncBatchPayload(p: SyncBatchPayload): SyncBatch {
  return {
    requestId: p.requestId,
    table: p.table,
    batchIndex: p.batchIndex,
    rows: p.rows.map(r => (fromRowData(r) ?? {}) as Record<string, unknown>),
    schema: p.schema.length > 0 ? p.schema : undefined,
    checksum: p.checksum,
    isLastBatchForTable: p.isLastBatchForTable,
  }
}

function toSyncCompletePayload(complete: SyncComplete): SyncCompletePayload {
  return {
    requestId: complete.requestId,
    snapshotSeq: complete.snapshotSeq,
    manifests: complete.manifests.map(m => ({
      table: m.table,
      rowCount: m.rowCount,
      pkHash: m.pkHash,
    })),
  }
}

function fromSyncCompletePayload(p: SyncCompletePayload): SyncComplete {
  return {
    requestId: p.requestId,
    snapshotSeq: p.snapshotSeq,
    manifests: p.manifests.map(
      (m: ProtoSyncTableManifest): SyncTableManifest => ({
        table: m.table,
        rowCount: m.rowCount,
        pkHash: m.pkHash,
      }),
    ),
  }
}

function toSyncAckPayload(ack: SyncAck): SyncAckPayload {
  return {
    requestId: ack.requestId,
    joinerNodeId: ack.joinerNodeId,
    table: ack.table,
    batchIndex: ack.batchIndex,
    success: ack.success,
    error: ack.error ?? '',
  }
}

function fromSyncAckPayload(p: SyncAckPayload): SyncAck {
  return {
    requestId: p.requestId,
    joinerNodeId: p.joinerNodeId,
    table: p.table,
    batchIndex: p.batchIndex,
    success: p.success,
    error: p.error || undefined,
  }
}

function toForwardRequest(req: ForwardedTransaction): ProtoForwardRequest {
  const statements: Statement[] = req.statements.map(s => {
    const namedParams: Record<string, ColumnValue> = {}
    const positionalParams: ColumnValue[] = []
    if (s.params !== undefined && s.params !== null) {
      if (Array.isArray(s.params)) {
        for (const v of s.params) {
          positionalParams.push(toColumnValue(v))
        }
      } else {
        for (const [k, v] of Object.entries(s.params as Record<string, unknown>)) {
          namedParams[k] = toColumnValue(v)
        }
      }
    }
    return { sql: s.sql, namedParams, positionalParams }
  })
  return { requestId: req.requestId, statements }
}

function fromForwardRequest(proto: ProtoForwardRequest): ForwardedTransaction {
  const statements = proto.statements.map(s => {
    const hasNamed = Object.keys(s.namedParams).length > 0
    const hasPositional = s.positionalParams.length > 0
    let params: Record<string, unknown> | unknown[] | undefined
    if (hasNamed) {
      const named: Record<string, unknown> = {}
      for (const [k, cv] of Object.entries(s.namedParams)) {
        named[k] = fromColumnValue(cv)
      }
      params = named
    } else if (hasPositional) {
      params = s.positionalParams.map(fromColumnValue)
    }
    return { sql: s.sql, params }
  })
  return { requestId: proto.requestId, statements }
}

function writeWithBackpressure<T>(
  stream: {
    write(msg: T): boolean
    once(event: string, listener: (...args: unknown[]) => void): unknown
    removeListener(event: string, listener: (...args: unknown[]) => void): unknown
  },
  message: T,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const ok = stream.write(message)
    if (ok) {
      resolve()
      return
    }
    const onDrain = () => {
      stream.removeListener('error', onError)
      resolve()
    }
    const onError = (...args: unknown[]) => {
      stream.removeListener('drain', onDrain)
      const err = args[0]
      reject(err instanceof Error ? err : new Error(String(err)))
    }
    stream.once('drain', onDrain)
    stream.once('error', onError)
  })
}

function buildServerCreds(options: GrpcReplicationOptions): ServerCredentials {
  if (options.insecure) {
    return ServerCredentials.createInsecure()
  }
  if (!options.tlsCert || !options.tlsKey) {
    throw new TransportError('TLS certificate and key paths are required when insecure mode is disabled')
  }
  const cert = readFileSync(options.tlsCert)
  const key = readFileSync(options.tlsKey)
  const ca = options.tlsCaCert ? readFileSync(options.tlsCaCert) : null
  return ServerCredentials.createSsl(ca, [{ private_key: key, cert_chain: cert }], ca !== null)
}

function buildChannelCreds(options: GrpcReplicationOptions): ChannelCredentials {
  if (options.insecure) {
    return credentials.createInsecure()
  }
  if (!options.tlsCert || !options.tlsKey) {
    throw new TransportError('TLS certificate and key paths are required when insecure mode is disabled')
  }
  const cert = readFileSync(options.tlsCert)
  const key = readFileSync(options.tlsKey)
  const ca = options.tlsCaCert ? readFileSync(options.tlsCaCert) : null
  return credentials.createSsl(ca, key, cert)
}

const DEFAULT_FORWARD_DEADLINE_MS = 30_000
const SERVICE_NAME = 'sirannon.replication.v1.Replication'

export class GrpcReplicationTransport implements ReplicationTransport {
  private readonly options: GrpcReplicationOptions
  private localNodeId = ''
  private localRole: TopologyRole = 'replica'
  private connected = false
  private server: Server | null = null
  private boundPort = 0

  private readonly connectedPeers = new Map<string, NodeInfo>()
  private readonly serverPeerStreams = new Map<string, PeerStreamEntry>()
  private readonly clientPeerStreams = new Map<string, ClientPeerEntry>()

  private batchHandler: BatchHandler | null = null
  private ackHandler: AckHandler | null = null
  private forwardHandler: ForwardHandler | null = null
  private peerConnectedHandler: PeerConnectedHandler | null = null
  private peerDisconnectedHandler: PeerDisconnectedHandler | null = null
  private syncRequestHandler: SyncRequestHandler | null = null
  private syncBatchHandler: SyncBatchHandler | null = null
  private syncCompleteHandler: SyncCompleteHandler | null = null
  private syncAckHandler: SyncAckHandler | null = null
  private healthImpl: HealthImplementation | null = null

  constructor(options: GrpcReplicationOptions = {}) {
    this.options = options
  }

  getPort(): number {
    return this.boundPort
  }

  async connect(localNodeId: string, config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }
    if (!localNodeId || typeof localNodeId !== 'string') {
      throw new TransportError('localNodeId must be a non-empty string')
    }

    this.localNodeId = localNodeId
    this.localRole = config.localRole ?? 'replica'
    this.connected = true

    if (this.localRole === 'primary') {
      await this.startServer()
    }

    if (config.endpoints && config.endpoints.length > 0) {
      for (const endpoint of config.endpoints) {
        await this.connectToEndpoint(endpoint)
      }
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return
    this.connected = false

    for (const [peerId, entry] of this.clientPeerStreams) {
      entry.replicateStream?.cancel()
      entry.syncStream?.cancel()
      entry.client.close()
      this.clientPeerStreams.delete(peerId)
    }

    for (const [peerId, ps] of this.serverPeerStreams) {
      ps.replicateStream?.end()
      ps.syncStream?.end()
      this.serverPeerStreams.delete(peerId)
    }

    const peerIds = [...this.connectedPeers.keys()]
    this.connectedPeers.clear()
    for (const peerId of peerIds) {
      this.peerDisconnectedHandler?.(peerId)
    }

    if (this.healthImpl) {
      this.healthImpl.setStatus(SERVICE_NAME, 'NOT_SERVING')
    }

    if (this.server) {
      await new Promise<void>(resolve => {
        const srv = this.server
        if (!srv) {
          resolve()
          return
        }
        srv.tryShutdown(err => {
          if (err) {
            srv.forceShutdown()
          }
          resolve()
        })
      })
      this.server = null
    }
  }

  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    const stream = this.getReplicateWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: ReplicationMessage = { batch: toBatchPayload(batch) }
    await writeWithBackpressure(stream, msg)
  }

  async broadcast(batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    const msg: ReplicationMessage = { batch: toBatchPayload(batch) }
    const promises: Promise<void>[] = []
    for (const [peerId] of this.connectedPeers) {
      const stream = this.getReplicateWriteStream(peerId)
      if (stream) {
        promises.push(writeWithBackpressure(stream, msg))
      }
    }
    await Promise.all(promises)
  }

  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.ensureConnected()
    const stream = this.getReplicateWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: ReplicationMessage = { ack: toAckPayload(ack) }
    await writeWithBackpressure(stream, msg)
  }

  async forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    this.ensureConnected()
    const clientEntry = this.clientPeerStreams.get(peerId)
    if (!clientEntry) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }

    const protoReq = toForwardRequest(request)
    const deadlineMs = this.options.forwardDeadlineMs ?? DEFAULT_FORWARD_DEADLINE_MS
    const deadline = new Date(Date.now() + deadlineMs)

    return new Promise<ForwardedTransactionResult>((resolve, reject) => {
      clientEntry.client.forward(
        protoReq,
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
          })
        },
      )
    })
  }

  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncRequest: toSyncRequestPayload(request) }
    await writeWithBackpressure(stream, msg)
  }

  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncBatch: toSyncBatchPayload(batch) }
    await writeWithBackpressure(stream, msg)
  }

  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncComplete: toSyncCompletePayload(complete) }
    await writeWithBackpressure(stream, msg)
  }

  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncAck: toSyncAckPayload(ack) }
    await writeWithBackpressure(stream, msg)
  }

  onBatchReceived(handler: BatchHandler): void {
    this.batchHandler = handler
  }

  onAckReceived(handler: AckHandler): void {
    this.ackHandler = handler
  }

  onForwardReceived(handler: ForwardHandler): void {
    this.forwardHandler = handler
  }

  onSyncRequested(handler: SyncRequestHandler): void {
    this.syncRequestHandler = handler
  }

  onSyncBatchReceived(handler: SyncBatchHandler): void {
    this.syncBatchHandler = handler
  }

  onSyncCompleteReceived(handler: SyncCompleteHandler): void {
    this.syncCompleteHandler = handler
  }

  onSyncAckReceived(handler: SyncAckHandler): void {
    this.syncAckHandler = handler
  }

  onPeerConnected(handler: PeerConnectedHandler): void {
    this.peerConnectedHandler = handler
  }

  onPeerDisconnected(handler: PeerDisconnectedHandler): void {
    this.peerDisconnectedHandler = handler
  }

  peers(): ReadonlyMap<string, NodeInfo> {
    return this.connectedPeers
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new TransportError('Transport is not connected')
    }
  }

  private getReplicateWriteStream(
    peerId: string,
  ):
    | ServerDuplexStream<ReplicationMessage, ReplicationMessage>
    | ClientDuplexStream<ReplicationMessage, ReplicationMessage>
    | null {
    const serverStream = this.serverPeerStreams.get(peerId)
    if (serverStream?.replicateStream) return serverStream.replicateStream

    const clientEntry = this.clientPeerStreams.get(peerId)
    if (clientEntry?.replicateStream) return clientEntry.replicateStream

    return null
  }

  private getSyncWriteStream(
    peerId: string,
  ): ServerDuplexStream<SyncMessage, SyncMessage> | ClientDuplexStream<SyncMessage, SyncMessage> | null {
    const serverStream = this.serverPeerStreams.get(peerId)
    if (serverStream?.syncStream) return serverStream.syncStream

    const clientEntry = this.clientPeerStreams.get(peerId)
    if (clientEntry?.syncStream) return clientEntry.syncStream

    return null
  }

  private registerPeer(nodeId: string, role: TopologyRole): void {
    if (this.connectedPeers.has(nodeId)) return
    const peerInfo: NodeInfo = {
      id: nodeId,
      role,
      joinedAt: Date.now(),
      lastSeenAt: Date.now(),
      lastAckedSeq: 0n,
    }
    this.connectedPeers.set(nodeId, peerInfo)
    this.peerConnectedHandler?.(peerInfo)
  }

  private removePeer(nodeId: string): void {
    if (!this.connectedPeers.has(nodeId)) return
    this.connectedPeers.delete(nodeId)
    this.serverPeerStreams.delete(nodeId)
    this.peerDisconnectedHandler?.(nodeId)
  }

  private async startServer(): Promise<void> {
    const server = new Server()
    this.server = server

    this.healthImpl = new HealthImplementation({
      [SERVICE_NAME]: 'SERVING',
      '': 'SERVING',
    })
    this.healthImpl.addToServer(server)

    server.addService(ReplicationService, {
      replicate: (call: ServerDuplexStream<ReplicationMessage, ReplicationMessage>) => {
        this.handleServerReplicateStream(call)
      },
      sync: (call: ServerDuplexStream<SyncMessage, SyncMessage>) => {
        this.handleServerSyncStream(call)
      },
      forward: (
        call: { request: ProtoForwardRequest },
        callback: (err: { code: number; message: string } | null, response?: ProtoForwardResponse) => void,
      ) => {
        this.handleForwardCall(call.request, callback)
      },
    })

    const host = this.options.host ?? '0.0.0.0'
    const port = this.options.port ?? 0
    const bindAddress = `${host}:${port}`

    const serverCreds = buildServerCreds(this.options)

    await new Promise<void>((resolve, reject) => {
      server.bindAsync(bindAddress, serverCreds, (err, assignedPort) => {
        if (err) {
          reject(new TransportError(`Failed to bind gRPC server: ${err.message}`))
          return
        }
        this.boundPort = assignedPort
        resolve()
      })
    })
  }

  private handleServerReplicateStream(call: ServerDuplexStream<ReplicationMessage, ReplicationMessage>): void {
    let peerId: string | null = null
    let helloSent = false

    call.on('data', (msg: ReplicationMessage) => {
      if (peerId === null) {
        if (!msg.hello) {
          call.destroy(new Error('First message must be Hello'))
          return
        }
        peerId = msg.hello.nodeId
        const peerRole = msg.hello.role as TopologyRole

        const existing = this.serverPeerStreams.get(peerId)
        if (existing?.replicateStream) {
          existing.replicateStream.end()
        }

        const ps = this.serverPeerStreams.get(peerId) ?? {
          replicateStream: null,
          syncStream: null,
        }
        ps.replicateStream = call
        this.serverPeerStreams.set(peerId, ps)

        if (!helloSent) {
          call.write({
            hello: { nodeId: this.localNodeId, role: this.localRole },
          })
          helloSent = true
        }

        this.registerPeer(peerId, peerRole)
        return
      }

      if (msg.batch) {
        const batch = fromBatchPayload(msg.batch)
        this.batchHandler?.(batch, peerId).catch(() => {})
      } else if (msg.ack) {
        const ack = fromAckPayload(msg.ack)
        this.ackHandler?.(ack, peerId)
      }
    })

    call.on('end', () => {
      call.end()
      if (peerId) {
        const ps = this.serverPeerStreams.get(peerId)
        if (ps?.replicateStream === call) {
          ps.replicateStream = null
        }
        if (!ps?.syncStream) {
          this.removePeer(peerId)
        }
      }
    })

    call.on('error', () => {
      if (peerId) {
        const ps = this.serverPeerStreams.get(peerId)
        if (ps?.replicateStream === call) {
          ps.replicateStream = null
        }
        if (!ps?.syncStream) {
          this.removePeer(peerId)
        }
      }
    })
  }

  private handleServerSyncStream(call: ServerDuplexStream<SyncMessage, SyncMessage>): void {
    let peerId: string | null = null
    let helloSent = false

    call.on('data', (msg: SyncMessage) => {
      if (peerId === null) {
        if (!msg.hello) {
          call.destroy(new Error('First message must be Hello'))
          return
        }
        peerId = msg.hello.nodeId
        const peerRole = msg.hello.role as TopologyRole

        const existing = this.serverPeerStreams.get(peerId)
        if (existing?.syncStream) {
          existing.syncStream.end()
        }

        const ps = this.serverPeerStreams.get(peerId) ?? {
          replicateStream: null,
          syncStream: null,
        }
        ps.syncStream = call
        this.serverPeerStreams.set(peerId, ps)

        if (!helloSent) {
          call.write({
            hello: { nodeId: this.localNodeId, role: this.localRole },
          })
          helloSent = true
        }

        this.registerPeer(peerId, peerRole)
        return
      }

      if (msg.syncRequest) {
        this.syncRequestHandler?.(fromSyncRequestPayload(msg.syncRequest), peerId).catch(() => {})
      } else if (msg.syncBatch) {
        this.syncBatchHandler?.(fromSyncBatchPayload(msg.syncBatch), peerId).catch(() => {})
      } else if (msg.syncComplete) {
        this.syncCompleteHandler?.(fromSyncCompletePayload(msg.syncComplete), peerId).catch(() => {})
      } else if (msg.syncAck) {
        this.syncAckHandler?.(fromSyncAckPayload(msg.syncAck), peerId)
      }
    })

    call.on('end', () => {
      call.end()
      if (peerId) {
        const ps = this.serverPeerStreams.get(peerId)
        if (ps?.syncStream === call) {
          ps.syncStream = null
        }
        if (!ps?.replicateStream) {
          this.removePeer(peerId)
        }
      }
    })

    call.on('error', () => {
      if (peerId) {
        const ps = this.serverPeerStreams.get(peerId)
        if (ps?.syncStream === call) {
          ps.syncStream = null
        }
        if (!ps?.replicateStream) {
          this.removePeer(peerId)
        }
      }
    })
  }

  private handleForwardCall(
    request: ProtoForwardRequest,
    callback: (err: { code: number; message: string } | null, response?: ProtoForwardResponse) => void,
  ): void {
    if (!this.forwardHandler) {
      callback({
        code: status.UNIMPLEMENTED,
        message: 'Forward handler not registered',
      })
      return
    }

    const appReq = fromForwardRequest(request)
    this.forwardHandler(appReq, 'unknown')
      .then(result => {
        callback(null, {
          requestId: result.requestId,
          results: result.results.map(r => ({
            changes: r.changes,
            lastInsertRowId: BigInt(typeof r.lastInsertRowId === 'string' ? r.lastInsertRowId : r.lastInsertRowId),
          })),
          error: '',
        })
      })
      .catch((err: Error) => {
        callback(null, {
          requestId: request.requestId,
          results: [],
          error: err.message,
        })
      })
  }

  private async connectToEndpoint(endpoint: string): Promise<void> {
    const channelCreds = buildChannelCreds(this.options)
    const clientOpts: Partial<ClientOptions> = {}
    const client = new ReplicationClient(endpoint, channelCreds, clientOpts)

    const replicateStream = client.replicate()
    const syncStream = client.sync()

    const entry: ClientPeerEntry = {
      client,
      replicateStream,
      syncStream,
    }

    replicateStream.write({
      hello: { nodeId: this.localNodeId, role: this.localRole },
    })
    syncStream.write({
      hello: { nodeId: this.localNodeId, role: this.localRole },
    })

    let replicatePeerId: string | null = null

    replicateStream.on('data', (msg: ReplicationMessage) => {
      if (replicatePeerId === null) {
        if (!msg.hello) {
          replicateStream.cancel()
          return
        }
        replicatePeerId = msg.hello.nodeId
        const peerRole = msg.hello.role as TopologyRole
        this.clientPeerStreams.set(replicatePeerId, entry)
        this.registerPeer(replicatePeerId, peerRole)
        return
      }

      if (msg.batch) {
        this.batchHandler?.(fromBatchPayload(msg.batch), replicatePeerId).catch(() => {})
      } else if (msg.ack) {
        this.ackHandler?.(fromAckPayload(msg.ack), replicatePeerId)
      }
    })

    replicateStream.on('end', () => {
      if (replicatePeerId) {
        const e = this.clientPeerStreams.get(replicatePeerId)
        if (e) {
          e.replicateStream = null
          if (!e.syncStream) {
            this.clientPeerStreams.delete(replicatePeerId)
            this.removePeer(replicatePeerId)
          }
        }
      }
    })

    replicateStream.on('error', () => {
      if (replicatePeerId) {
        const e = this.clientPeerStreams.get(replicatePeerId)
        if (e) {
          e.replicateStream = null
          if (!e.syncStream) {
            this.clientPeerStreams.delete(replicatePeerId)
            this.removePeer(replicatePeerId)
          }
        }
      }
    })

    let syncPeerId: string | null = null

    syncStream.on('data', (msg: SyncMessage) => {
      if (syncPeerId === null) {
        if (!msg.hello) {
          syncStream.cancel()
          return
        }
        syncPeerId = msg.hello.nodeId
        return
      }

      if (msg.syncRequest) {
        this.syncRequestHandler?.(fromSyncRequestPayload(msg.syncRequest), syncPeerId).catch(() => {})
      } else if (msg.syncBatch) {
        this.syncBatchHandler?.(fromSyncBatchPayload(msg.syncBatch), syncPeerId).catch(() => {})
      } else if (msg.syncComplete) {
        this.syncCompleteHandler?.(fromSyncCompletePayload(msg.syncComplete), syncPeerId).catch(() => {})
      } else if (msg.syncAck) {
        this.syncAckHandler?.(fromSyncAckPayload(msg.syncAck), syncPeerId)
      }
    })

    syncStream.on('end', () => {
      if (syncPeerId) {
        const e = this.clientPeerStreams.get(syncPeerId)
        if (e) {
          e.syncStream = null
          if (!e.replicateStream) {
            this.clientPeerStreams.delete(syncPeerId)
            this.removePeer(syncPeerId)
          }
        }
      }
    })

    syncStream.on('error', () => {
      if (syncPeerId) {
        const e = this.clientPeerStreams.get(syncPeerId)
        if (e) {
          e.syncStream = null
          if (!e.replicateStream) {
            this.clientPeerStreams.delete(syncPeerId)
            this.removePeer(syncPeerId)
          }
        }
      }
    })
  }
}
