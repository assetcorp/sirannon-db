import net from 'node:net'
import { closeListener, listenOnLoopback, type RelayedSockets, relayToTarget } from './tcp-relay.js'

export class ServerProxy {
  private server: net.Server | null = null
  private readonly connections = new Set<RelayedSockets>()
  private targetPort: number
  port = 0

  constructor(targetPort: number) {
    this.targetPort = targetPort
  }

  async listen(): Promise<void> {
    const server = net.createServer(client => {
      const relayed = relayToTarget(client, this.targetPort, ended => this.connections.delete(ended))
      this.connections.add(relayed)
      client.pipe(relayed.upstream)
      relayed.upstream.pipe(client)
    })
    this.server = server
    this.port = await listenOnLoopback(server)
  }

  pointAt(targetPort: number): void {
    this.targetPort = targetPort
  }

  killAllConnections(): number {
    const killed = this.connections.size
    for (const { client, upstream } of [...this.connections]) {
      client.destroy()
      upstream.destroy()
    }
    this.connections.clear()
    return killed
  }

  close(): Promise<void> {
    this.killAllConnections()
    const server = this.server
    this.server = null
    return closeListener(server)
  }
}
