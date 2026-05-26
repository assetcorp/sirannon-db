import { TransportError } from '../../replication/errors.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  ReplicationAck,
  ReplicationBatch,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
} from '../../replication/types.js'
import type { EventKind, FaultPolicy } from './fault-policy.js'
import type { DeterministicScheduler, ScheduledEvent } from './scheduler.js'
import { SimulatedTransport } from './transport.js'

export class SimulatedNetwork {
  private readonly transports = new Map<string, SimulatedTransport>()
  private readonly scheduler: DeterministicScheduler
  private readonly faultPolicy: FaultPolicy
  private readonly forwardTimeoutMs: number

  constructor(scheduler: DeterministicScheduler, faultPolicy: FaultPolicy, forwardTimeoutMs = 30_000) {
    this.scheduler = scheduler
    this.faultPolicy = faultPolicy
    this.forwardTimeoutMs = forwardTimeoutMs
    this.scheduler.setDeliverFn(event => this.deliverEvent(event))
  }

  createTransport(): SimulatedTransport {
    return new SimulatedTransport(this)
  }

  getTransport(nodeId: string): SimulatedTransport | undefined {
    return this.transports.get(nodeId)
  }

  get policy(): FaultPolicy {
    return this.faultPolicy
  }

  connectedPeerTransports(excludingNodeId: string): Array<[string, SimulatedTransport]> {
    const peers: Array<[string, SimulatedTransport]> = []
    for (const [peerId, transport] of this.transports) {
      if (peerId !== excludingNodeId && transport.isConnected) {
        peers.push([peerId, transport])
      }
    }
    return peers
  }

  register(nodeId: string, transport: SimulatedTransport): void {
    this.transports.set(nodeId, transport)
  }

  unregister(nodeId: string): void {
    this.transports.delete(nodeId)
  }

  scheduleMessage(from: string, to: string, kind: EventKind, payload: unknown): void {
    if (this.faultPolicy.shouldDrop(from, to, kind)) return
    const latency = this.faultPolicy.sampleLatency(from, to, kind)
    this.scheduler.enqueue({
      deliverAt: this.scheduler.now + latency,
      from,
      to,
      kind,
      payload,
    })
  }

  scheduleForward(from: string, to: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    return new Promise<ForwardedTransactionResult>((resolve, reject) => {
      if (this.faultPolicy.shouldDrop(from, to, 'forward_request')) {
        const timer = setTimeout(() => {
          reject(new TransportError('Forward request timed out (dropped by fault policy)'))
        }, this.forwardTimeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
        timer.unref?.()
        return
      }

      const latency = this.faultPolicy.sampleLatency(from, to, 'forward_request')
      this.scheduler.enqueue({
        deliverAt: this.scheduler.now + latency,
        from,
        to,
        kind: 'forward_request',
        payload: { request, resolve, reject },
      })
    })
  }

  private async deliverEvent(event: ScheduledEvent): Promise<void> {
    const target = this.transports.get(event.to)
    if (!target || !target.isConnected) return

    switch (event.kind) {
      case 'batch':
        try {
          await target.deliverBatch(event.payload as ReplicationBatch, event.from)
        } catch {
          /* swallow errors; matches InMemoryTransport fire-and-forget semantics */
        }
        break

      case 'ack':
        try {
          target.deliverAck(event.payload as ReplicationAck, event.from)
        } catch {
          /* swallow */
        }
        break

      case 'forward_request':
        await this.deliverForwardRequest(event, target)
        break

      case 'forward_response': {
        const resp = event.payload as {
          result: ForwardedTransactionResult
          resolve: (v: ForwardedTransactionResult) => void
        }
        resp.resolve(resp.result)
        break
      }

      case 'sync_request':
        try {
          await target.deliverSyncRequest(event.payload as SyncRequest, event.from)
        } catch {
          /* swallow */
        }
        break

      case 'sync_batch':
        try {
          await target.deliverSyncBatch(event.payload as SyncBatch, event.from)
        } catch {
          /* swallow */
        }
        break

      case 'sync_complete':
        try {
          await target.deliverSyncComplete(event.payload as SyncComplete, event.from)
        } catch {
          /* swallow */
        }
        break

      case 'sync_ack':
        try {
          target.deliverSyncAck(event.payload as SyncAck, event.from)
        } catch {
          /* swallow */
        }
        break
    }
  }

  private async deliverForwardRequest(event: ScheduledEvent, target: SimulatedTransport): Promise<void> {
    const fwd = event.payload as {
      request: ForwardedTransaction
      resolve: (v: ForwardedTransactionResult) => void
      reject: (e: Error) => void
    }
    try {
      const result = await target.deliverForward(fwd.request, event.from)
      if (this.faultPolicy.shouldDrop(event.to, event.from, 'forward_response')) {
        const timer = setTimeout(() => {
          fwd.reject(new TransportError('Forward response timed out (dropped by fault policy)'))
        }, this.forwardTimeoutMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
        timer.unref?.()
        return
      }
      const latency = this.faultPolicy.sampleLatency(event.to, event.from, 'forward_response')
      this.scheduler.enqueue({
        deliverAt: this.scheduler.now + latency,
        from: event.to,
        to: event.from,
        kind: 'forward_response',
        payload: { result, resolve: fwd.resolve },
      })
    } catch (err) {
      fwd.reject(err instanceof Error ? err : new TransportError(String(err)))
    }
  }
}
