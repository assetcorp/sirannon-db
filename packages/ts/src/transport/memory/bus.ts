import type { InMemoryTransport } from './transport.js'

/**
 * Shared message bus that connects InMemoryTransport instances within the
 * same process. Each transport registers itself on `connect()` and messages
 * are delivered via direct method calls on the target transport through
 * microtask scheduling, simulating async network delivery with no I/O.
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
