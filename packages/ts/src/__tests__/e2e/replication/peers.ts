import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { credentials, type ServiceError } from '@grpc/grpc-js'
import type {
  ReplicationAck,
  ReplicationBatch,
  ReplicationChange,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
  TopologyRole,
} from '../../../replication/index.js'
import { type ForwardResponse, ReplicationClient } from '../../../transport/grpc/generated/replication.js'
import { GrpcReplicationTransport } from '../../../transport/grpc/index.js'
import type { NodeCert } from '../lib/certs.js'
import { encodeHlc, specChecksum } from '../lib/device-sync-oracle.js'
import { eventually, type ReplicationScene, type Row } from '../lib/replication-scene.js'

export interface Received<T> {
  message: T
  from: string
}

export interface PeerIdentity {
  nodeId: string
  cert: NodeCert
  caCertPath: string
}

export class RawPeer {
  readonly batches: Received<ReplicationBatch>[] = []
  readonly acks: Received<ReplicationAck>[] = []
  readonly syncRequests: Received<SyncRequest>[] = []
  readonly syncBatches: Received<SyncBatch>[] = []
  readonly syncCompletes: Received<SyncComplete>[] = []
  readonly syncAcks: Received<SyncAck>[] = []
  readonly peerEvents: string[] = []
  readonly transport: GrpcReplicationTransport
  private readonly syncRequestListeners: Array<(request: SyncRequest, from: string) => void> = []

  constructor(
    readonly nodeId: string,
    identity: PeerIdentity,
    listen: boolean,
  ) {
    this.transport = new GrpcReplicationTransport({
      host: 'localhost',
      ...(listen ? { port: 0 } : {}),
      tlsCert: identity.cert.certPath,
      tlsKey: identity.cert.keyPath,
      tlsCaCert: identity.caCertPath,
    })
    this.transport.onBatchReceived(async (message, from) => {
      this.batches.push({ message, from })
    })
    this.transport.onAckReceived((message, from) => {
      this.acks.push({ message, from })
    })
    this.transport.onSyncRequested(async (message, from) => {
      this.syncRequests.push({ message, from })
      for (const listener of this.syncRequestListeners) listener(message, from)
    })
    this.transport.onSyncBatchReceived(async (message, from) => {
      this.syncBatches.push({ message, from })
    })
    this.transport.onSyncCompleteReceived(async (message, from) => {
      this.syncCompletes.push({ message, from })
    })
    this.transport.onSyncAckReceived((message, from) => {
      this.syncAcks.push({ message, from })
    })
    this.transport.onPeerConnected(peer => {
      this.peerEvents.push(`joined ${peer.id}`)
    })
    this.transport.onPeerDisconnected(peerId => {
      this.peerEvents.push(`left ${peerId}`)
    })
  }

  departures(peerId: string): number {
    return this.peerEvents.filter(event => event === `left ${peerId}`).length
  }

  get port(): number {
    return this.transport.getPort()
  }

  onSyncRequest(listener: (request: SyncRequest, from: string) => void): void {
    this.syncRequestListeners.push(listener)
  }

  async connected(peerId: string): Promise<void> {
    await eventually(() => this.transport.peers().has(peerId), `${this.nodeId} to see ${peerId} connected`)
  }

  async syncAck(requestId: string, table: string, batchIndex: number): Promise<SyncAck> {
    let found: SyncAck | undefined
    await eventually(() => {
      found = this.syncAcks.find(
        ({ message }) =>
          message.requestId === requestId && message.table === table && message.batchIndex === batchIndex,
      )?.message
      return found !== undefined
    }, `the sync acknowledgement of ${table} batch ${batchIndex}`)
    if (found === undefined) throw new Error(`No sync acknowledgement for ${table} batch ${batchIndex}`)
    return found
  }
}

export async function listenAsPeer(
  scene: ReplicationScene,
  identity: PeerIdentity,
  role: TopologyRole,
  claimedNodeId = identity.nodeId,
): Promise<RawPeer> {
  const peer = new RawPeer(claimedNodeId, identity, true)
  scene.track(peer.transport)
  await peer.transport.connect(claimedNodeId, { localRole: role, groupId: role === 'replica' ? 'static' : undefined })
  return peer
}

export async function dialAsPeer(
  scene: ReplicationScene,
  identity: PeerIdentity,
  port: number,
  role: TopologyRole = 'replica',
  claimedNodeId = identity.nodeId,
): Promise<RawPeer> {
  const peer = new RawPeer(claimedNodeId, identity, false)
  scene.track(peer.transport)
  await peer.transport.connect(claimedNodeId, { localRole: role, endpoints: [`localhost:${port}`] })
  return peer
}

export function stampAt(offsetMs: number, nodeId: string, logical = 0): string {
  return encodeHlc(Date.now() + offsetMs, logical, nodeId)
}

export function insertChange(
  table: string,
  row: Row,
  nodeId: string,
  hlc: string,
  txId = 'intruder-tx',
): ReplicationChange {
  return {
    table,
    operation: 'insert',
    rowId: String(row.id),
    primaryKey: { id: row.id },
    hlc,
    txId,
    nodeId,
    newData: row,
    oldData: null,
  }
}

export function ddlChange(statement: string, nodeId: string, hlc: string): ReplicationChange {
  return {
    table: '__ddl__',
    operation: 'ddl',
    rowId: '',
    primaryKey: {},
    hlc,
    txId: `ddl-${hlc}`,
    nodeId,
    newData: null,
    oldData: null,
    ddlStatement: statement,
  }
}

export function signedBatch(
  sourceNodeId: string,
  fromSeq: bigint,
  changes: ReplicationChange[],
  overrides: Partial<ReplicationBatch> = {},
): ReplicationBatch {
  const stamps = changes.map(change => change.hlc).sort()
  const toSeq = fromSeq + BigInt(Math.max(changes.length, 1) - 1)
  return {
    sourceNodeId,
    batchId: `${sourceNodeId}-${fromSeq}-${toSeq}`,
    fromSeq,
    toSeq,
    hlcRange: { min: stamps[0] ?? '', max: stamps[stamps.length - 1] ?? '' },
    changes,
    checksum: specChecksum(changes),
    ...overrides,
  }
}

export function chainedDigest(checksums: readonly string[]): string {
  let digest = ''
  for (const checksum of checksums)
    digest = createHash('sha256')
      .update(digest + checksum, 'utf8')
      .digest('hex')
  return digest
}

export interface ServedTable {
  table: string
  batches: Row[][]
}

export interface SyncPlan {
  schema: string[]
  tables: ServedTable[]
  totalTables?: number
  snapshotSeq?: bigint
  tamper?: {
    batch?: (batch: SyncBatch) => SyncBatch
    complete?: (complete: SyncComplete) => SyncComplete
  }
}

export async function serveSync(source: RawPeer, request: SyncRequest, plan: SyncPlan): Promise<SyncAck[]> {
  const joiner = request.joinerNodeId
  const acks: SyncAck[] = []
  const send = async (batch: SyncBatch): Promise<boolean> => {
    const outgoing = plan.tamper?.batch ? plan.tamper.batch(batch) : batch
    await source.transport.sendSyncBatch(joiner, outgoing)
    const ack = await source.syncAck(request.requestId, outgoing.table, outgoing.batchIndex)
    acks.push(ack)
    return ack.success
  }

  const schemaAccepted = await send({
    requestId: request.requestId,
    table: '__schema__',
    batchIndex: 0,
    rows: [],
    schema: plan.schema,
    checksum: specChecksum(plan.schema),
    isLastBatchForTable: true,
    totalTables: plan.totalTables ?? plan.tables.length,
  })
  if (!schemaAccepted) return acks

  const manifests = []
  for (const { table, batches } of plan.tables) {
    const pages = batches.length === 0 ? [[]] : batches
    const checksums: string[] = []
    let rowCount = 0
    for (const [batchIndex, rows] of pages.entries()) {
      const checksum = specChecksum(rows)
      checksums.push(checksum)
      rowCount += rows.length
      const accepted = await send({
        requestId: request.requestId,
        table,
        batchIndex,
        rows,
        checksum,
        isLastBatchForTable: batchIndex === pages.length - 1,
      })
      if (!accepted) return acks
    }
    manifests.push({ table, rowCount, batchDigest: chainedDigest(checksums) })
  }

  const complete: SyncComplete = { requestId: request.requestId, snapshotSeq: plan.snapshotSeq ?? 0n, manifests }
  await source.transport.sendSyncComplete(joiner, plan.tamper?.complete ? plan.tamper.complete(complete) : complete)
  return acks
}

export interface HelloOutcome {
  answeredBy: string | null
  code: number | null
}

export function helloOutcome(
  port: number,
  identity: PeerIdentity | 'plaintext',
  claimedNodeId: string,
  opening: 'hello' | 'ack' = 'hello',
) {
  const channelCredentials =
    identity === 'plaintext'
      ? credentials.createInsecure()
      : credentials.createSsl(
          readFileSync(identity.caCertPath),
          readFileSync(identity.cert.keyPath),
          readFileSync(identity.cert.certPath),
        )
  const client = new ReplicationClient(`localhost:${port}`, channelCredentials)
  const call = client.replicate()
  return new Promise<HelloOutcome>(resolve => {
    let settled = false
    const finish = (outcome: HelloOutcome): void => {
      if (settled) return
      settled = true
      call.cancel()
      client.close()
      resolve(outcome)
    }
    call.on('data', (message: { hello?: { nodeId: string } }) => {
      if (message.hello) finish({ answeredBy: message.hello.nodeId, code: null })
    })
    call.on('error', (error: ServiceError) => finish({ answeredBy: null, code: error.code }))
    call.on('end', () => finish({ answeredBy: null, code: null }))
    if (opening === 'ack') {
      call.write({ ack: { batchId: '', ackedSeq: 1n, nodeId: claimedNodeId, groupId: '', primaryTerm: 0n } })
      return
    }
    call.write({ hello: { nodeId: claimedNodeId, role: 'replica', groupId: '', primaryTerm: 0n, protocolVersion: '' } })
  })
}

export interface ForwardOutcome {
  error: ServiceError | null
  response: ForwardResponse | undefined
}

export function forwardWithoutJoining(port: number, identity: PeerIdentity, sql: string): Promise<ForwardOutcome> {
  const client = new ReplicationClient(
    `localhost:${port}`,
    credentials.createSsl(
      readFileSync(identity.caCertPath),
      readFileSync(identity.cert.keyPath),
      readFileSync(identity.cert.certPath),
    ),
  )
  return new Promise(resolve => {
    client.forward(
      {
        requestId: 'forward-without-joining',
        statements: [{ sql, namedParams: {}, positionalParams: [] }],
        groupId: '',
        primaryTerm: 0n,
        writeConcernLevel: '',
        writeConcernTimeoutMs: 0,
      },
      (error, response) => {
        client.close()
        resolve({ error, response })
      },
    )
  })
}
