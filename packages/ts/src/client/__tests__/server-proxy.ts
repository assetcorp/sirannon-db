import net from 'node:net'

export class ServerProxy {
  private server: net.Server | null = null
  private readonly sockets = new Set<net.Socket>()
  private targetPort: number
  port = 0

  constructor(targetPort: number) {
    this.targetPort = targetPort
  }

  listen(): Promise<void> {
    const server = net.createServer(clientSocket => {
      const upstream = net.connect(this.targetPort, '127.0.0.1')
      this.sockets.add(clientSocket)
      this.sockets.add(upstream)
      clientSocket.pipe(upstream)
      upstream.pipe(clientSocket)
      const drop = () => {
        this.sockets.delete(clientSocket)
        this.sockets.delete(upstream)
        clientSocket.destroy()
        upstream.destroy()
      }
      clientSocket.on('close', drop)
      upstream.on('close', drop)
      clientSocket.on('error', () => {})
      upstream.on('error', () => {})
    })
    this.server = server
    return new Promise(resolve => {
      server.listen(0, '127.0.0.1', () => {
        const address = server.address()
        if (address !== null && typeof address === 'object') {
          this.port = address.port
        }
        resolve()
      })
    })
  }

  pointAt(targetPort: number): void {
    this.targetPort = targetPort
  }

  killAllConnections(): number {
    const killed = this.sockets.size
    for (const socket of [...this.sockets]) {
      socket.destroy()
    }
    this.sockets.clear()
    return killed
  }

  close(): Promise<void> {
    this.killAllConnections()
    const server = this.server
    if (server === null) return Promise.resolve()
    return new Promise(resolve => server.close(() => resolve()))
  }
}
