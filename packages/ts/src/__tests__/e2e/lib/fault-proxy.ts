import net from 'node:net'
import { closeListener, listenOnLoopback, relayToTarget } from './tcp-relay.js'

export type ProxyDirection = 'to-target' | 'to-client'

export interface FaultProxy {
  readonly port: number
  readonly opened: number
  readonly closed: number
  readonly bytesToTarget: number
  readonly bytesToClient: number
  delay(direction: ProxyDirection, ms: number): void
  stall(): void
  resume(): void
  cut(): void
  refuse(refusing: boolean): void
  retarget(port: number): void
  close(): Promise<void>
}

interface QueuedChunk {
  chunk: Buffer
  arrivedAt: number
}

class Lane {
  private readonly queue: QueuedChunk[] = []
  private timer: ReturnType<typeof setTimeout> | null = null

  constructor(
    private readonly destination: net.Socket,
    private readonly isStalled: () => boolean,
    private readonly delayMs: () => number,
    private readonly onDelivered: (bytes: number) => void,
  ) {}

  push(chunk: Buffer): void {
    this.queue.push({ chunk, arrivedAt: Date.now() })
    this.pump()
  }

  reschedule(): void {
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
    this.pump()
  }

  pump(): void {
    if (this.timer !== null) return
    while (this.queue.length > 0 && !this.isStalled()) {
      const head = this.queue[0]
      if (head === undefined) return
      const waitMs = head.arrivedAt + this.delayMs() - Date.now()
      if (waitMs > 0) {
        this.timer = setTimeout(() => {
          this.timer = null
          this.pump()
        }, waitMs)
        return
      }
      this.queue.shift()
      if (this.destination.destroyed) continue
      this.destination.write(head.chunk)
      this.onDelivered(head.chunk.length)
    }
  }

  drop(): void {
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
    this.queue.length = 0
  }
}

function asBuffer(chunk: string | Buffer): Buffer {
  return typeof chunk === 'string' ? Buffer.from(chunk) : chunk
}

interface Pairing {
  client: net.Socket
  target: net.Socket
  toTarget: Lane
  toClient: Lane
}

export async function startFaultProxy(targetPort: number, targetHost = '127.0.0.1'): Promise<FaultProxy> {
  const pairings = new Set<Pairing>()
  const delays: Record<ProxyDirection, number> = { 'to-target': 0, 'to-client': 0 }
  let currentTarget = targetPort
  let stalled = false
  let refusing = false
  let opened = 0
  let closed = 0
  let bytesToTarget = 0
  let bytesToClient = 0

  const server = net.createServer(client => {
    opened += 1
    client.on('error', () => {})
    if (refusing) {
      closed += 1
      client.destroy()
      return
    }

    const { upstream: target } = relayToTarget(
      client,
      currentTarget,
      () => {
        closed += 1
        pairings.delete(pairing)
        pairing.toTarget.drop()
        pairing.toClient.drop()
      },
      targetHost,
    )
    const pairing: Pairing = {
      client,
      target,
      toTarget: new Lane(
        target,
        () => stalled,
        () => delays['to-target'],
        bytes => {
          bytesToTarget += bytes
        },
      ),
      toClient: new Lane(
        client,
        () => stalled,
        () => delays['to-client'],
        bytes => {
          bytesToClient += bytes
        },
      ),
    }
    pairings.add(pairing)

    client.on('data', chunk => pairing.toTarget.push(asBuffer(chunk)))
    target.on('data', chunk => pairing.toClient.push(asBuffer(chunk)))
  })

  const port = await listenOnLoopback(server)

  return {
    port,
    get opened() {
      return opened
    },
    get closed() {
      return closed
    },
    get bytesToTarget() {
      return bytesToTarget
    },
    get bytesToClient() {
      return bytesToClient
    },
    delay(direction, ms) {
      delays[direction] = ms
      for (const pairing of pairings) {
        pairing.toTarget.reschedule()
        pairing.toClient.reschedule()
      }
    },
    stall() {
      stalled = true
    },
    resume() {
      stalled = false
      for (const pairing of pairings) {
        pairing.toTarget.pump()
        pairing.toClient.pump()
      }
    },
    cut() {
      for (const pairing of [...pairings]) {
        pairing.client.destroy()
        pairing.target.destroy()
      }
    },
    refuse(value) {
      refusing = value
    },
    retarget(port) {
      currentTarget = port
    },
    async close() {
      refusing = true
      for (const pairing of [...pairings]) {
        pairing.client.destroy()
        pairing.target.destroy()
      }
      await closeListener(server)
    },
  }
}
