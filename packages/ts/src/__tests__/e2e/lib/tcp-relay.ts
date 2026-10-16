import net from 'node:net'

const LOOPBACK_HOST = '127.0.0.1'

export interface RelayedSockets {
  readonly client: net.Socket
  readonly upstream: net.Socket
}

export function relayToTarget(
  client: net.Socket,
  targetPort: number,
  onEnd: (sockets: RelayedSockets) => void,
  targetHost = LOOPBACK_HOST,
): RelayedSockets {
  const upstream = net.connect(targetPort, targetHost)
  const sockets: RelayedSockets = { client, upstream }
  let ended = false
  const end = (): void => {
    if (ended) return
    ended = true
    onEnd(sockets)
    client.destroy()
    upstream.destroy()
  }
  client.on('error', () => {})
  upstream.on('error', () => {})
  client.on('close', end)
  upstream.on('close', end)
  return sockets
}

export async function listenOnLoopback(server: net.Server): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, LOOPBACK_HOST, () => {
      server.off('error', reject)
      resolve()
    })
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('The proxy did not bind a TCP port')
  return address.port
}

export function closeListener(server: net.Server | null): Promise<void> {
  if (server === null) return Promise.resolve()
  return new Promise(resolve => server.close(() => resolve()))
}
