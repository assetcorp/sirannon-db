import type { InMemoryTransport } from './transport.js'

/**
 * Connects {@link InMemoryTransport} instances inside one process. Each
 * transport joins the bus in `connect()`. A sending transport looks up its
 * target on the bus and calls the target's receive method, with no network I/O.
 *
 * @public
 */
export class MemoryBus {
  private readonly transports = new Map<string, InMemoryTransport>()

  /** @internal */
  join(peerId: string, transport: InMemoryTransport): void {
    this.transports.set(peerId, transport)
  }

  /** @internal */
  leave(peerId: string): void {
    this.transports.delete(peerId)
  }

  /** @internal */
  getTransport(peerId: string): InMemoryTransport | undefined {
    return this.transports.get(peerId)
  }

  /** @internal */
  peerIds(): IterableIterator<string> {
    return this.transports.keys()
  }

  /** @internal */
  get size(): number {
    return this.transports.size
  }
}
