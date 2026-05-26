import type { InMemoryTransport } from './transport.js'

/**
 * Shared message bus that connects InMemoryTransport instances within the
 * same process. Each transport registers itself on `connect()` and messages
 * are delivered via direct method calls on the target transport through
 * microtask scheduling, simulating async network delivery without actual I/O.
 */
export class MemoryBus {
  private readonly transports = new Map<string, InMemoryTransport>()

  join(peerId: string, transport: InMemoryTransport): void {
    this.transports.set(peerId, transport)
  }

  leave(peerId: string): void {
    this.transports.delete(peerId)
  }

  getTransport(peerId: string): InMemoryTransport | undefined {
    return this.transports.get(peerId)
  }

  peerIds(): IterableIterator<string> {
    return this.transports.keys()
  }

  get size(): number {
    return this.transports.size
  }
}
