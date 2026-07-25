import { vi } from 'vitest'

export interface FakeSocket extends EventTarget {
  readyState: number
  readonly sent: string[]
  close(): void
  deliver(message: unknown): void
}

export function installFakeWebSockets(): { sockets: FakeSocket[]; restore: () => void } {
  const originalWebSocket = globalThis.WebSocket
  const sockets: FakeSocket[] = []

  class FakeWebSocket extends EventTarget {
    static readonly CONNECTING = 0
    static readonly OPEN = 1
    static readonly CLOSING = 2
    static readonly CLOSED = 3

    readyState = FakeWebSocket.CONNECTING
    readonly sent: string[] = []

    constructor(readonly url: string | URL) {
      super()
      sockets.push(this)
      queueMicrotask(() => {
        this.readyState = FakeWebSocket.OPEN
        this.dispatchEvent(new Event('open'))
      })
    }

    send(data: string): void {
      this.sent.push(data)
    }

    close(): void {
      this.readyState = FakeWebSocket.CLOSED
      this.dispatchEvent(new Event('close'))
    }

    deliver(message: unknown): void {
      const event = new Event('message') as Event & { data: string }
      event.data = JSON.stringify(message)
      this.dispatchEvent(event)
    }
  }

  vi.stubGlobal('WebSocket', FakeWebSocket)
  return { sockets, restore: () => vi.stubGlobal('WebSocket', originalWebSocket) }
}

export function firstFrameOfType(socket: FakeSocket, type: string): Record<string, unknown> | undefined {
  return socket.sent.map(s => JSON.parse(s) as Record<string, unknown>).find(m => m.type === type)
}

export function firstSubscribeFrame(socket: FakeSocket): Record<string, unknown> | undefined {
  return firstFrameOfType(socket, 'subscribe')
}

export async function until(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`until timed out after ${timeout}ms`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}
