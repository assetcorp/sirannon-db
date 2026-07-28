import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { ResultOp } from '../../core/live/types.js'
import type { WSLiveMessage, WSLiveOp, WSSubscribeMessage } from '../../server/protocol.js'
import type { LiveHandlers, RegistryDigestSource, RemoteSubscription } from '../types.js'
import { RemoteError } from '../types.js'

interface ActiveLiveQuery {
  name: string
  args: Record<string, unknown> | undefined
  handlers: LiveHandlers
  digest: RegistryDigestSource | undefined
  established: boolean
}

export interface LiveSubscribePort {
  request(message: WSSubscribeMessage): Promise<void>
  sendUnsubscribe(id: string): void
  isClosed(): boolean
}

export class LiveQueryRegistry {
  private readonly queries = new Map<string, ActiveLiveQuery>()

  constructor(private readonly port: LiveSubscribePort) {}

  get size(): number {
    return this.queries.size
  }

  async open(
    id: string,
    name: string,
    args: Record<string, unknown> | undefined,
    handlers: LiveHandlers,
    digest: RegistryDigestSource | undefined,
  ): Promise<RemoteSubscription> {
    const live: ActiveLiveQuery = { name, args, handlers, digest, established: false }
    this.queries.set(id, live)

    try {
      await this.send(id, live)
    } catch (err) {
      this.queries.delete(id)
      throw err
    }

    return {
      unsubscribe: () => {
        this.queries.delete(id)
        this.port.sendUnsubscribe(id)
      },
    }
  }

  deliverSubscribed(id: string, rows: unknown[]): void {
    this.queries.get(id)?.handlers.onRows(decodeLiveRows(rows))
  }

  deliver(id: string, message: WSLiveMessage): void {
    const live = this.queries.get(id)
    if (live !== undefined) deliverLiveMessage(live, message)
  }

  fail(id: string, error: RemoteError): void {
    this.queries.get(id)?.handlers.onError(error)
  }

  markDisconnected(): void {
    for (const live of this.queries.values()) {
      live.established = false
      live.handlers.onRevalidating()
    }
  }

  clear(): void {
    this.queries.clear()
  }

  async restart(): Promise<void> {
    let unreachable: RemoteError | null = null

    for (const [id, live] of [...this.queries.entries()]) {
      if (this.port.isClosed() || live.established) continue
      try {
        await this.send(id, live)
      } catch (err) {
        const failure =
          err instanceof RemoteError ? err : new RemoteError('CONNECTION_ERROR', 'The live query could not restart')
        if (failure.code === 'CONNECTION_ERROR' || failure.code === 'TIMEOUT') {
          unreachable = failure
          continue
        }
        this.queries.delete(id)
        live.handlers.onError(failure)
      }
    }

    if (unreachable !== null && !this.port.isClosed()) throw unreachable
  }

  private async send(id: string, live: ActiveLiveQuery): Promise<void> {
    const digest = await live.digest?.(false)
    try {
      await this.port.request(liveSubscribeMessage(id, live, digest))
    } catch (err) {
      if (!(err instanceof RemoteError) || err.code !== 'REGISTRY_MISMATCH' || live.digest === undefined) {
        throw err
      }
      await this.port.request(liveSubscribeMessage(id, live, await live.digest(true)))
    }
    live.established = true
  }
}

function liveSubscribeMessage(
  id: string,
  live: ActiveLiveQuery,
  registryDigest: string | undefined,
): WSSubscribeMessage {
  return {
    type: 'subscribe',
    id,
    name: live.name,
    ...(live.args === undefined ? {} : { args: encodeTaggedValues(live.args) as Record<string, unknown> }),
    ...(registryDigest === undefined ? {} : { registryDigest }),
  }
}

function decodeLiveRows(rows: unknown[]): Record<string, unknown>[] {
  return decodeTaggedValues(rows) as Record<string, unknown>[]
}

function deliverLiveMessage(live: ActiveLiveQuery, msg: WSLiveMessage): void {
  if (msg.rows !== undefined) {
    live.handlers.onRows(decodeLiveRows(msg.rows))
    return
  }
  if (msg.ops !== undefined) {
    live.handlers.onOps(msg.ops.map(decodeLiveOp))
    return
  }
  if (msg.revalidating === true) {
    live.handlers.onRevalidating()
    return
  }
  live.handlers.onError(new RemoteError('INVALID_RESPONSE', 'A live message carried no rows, operations, or status'))
}

function decodeLiveOp(op: WSLiveOp): ResultOp<Record<string, unknown>> {
  if (op.op === 'delete') return { op: 'delete', index: op.index }
  return { op: op.op, index: op.index, row: decodeTaggedValues(op.row) as Record<string, unknown> }
}
