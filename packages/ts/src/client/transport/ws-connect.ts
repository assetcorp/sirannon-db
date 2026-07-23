import { RemoteError } from '../types.js'

export type ClientWebSocket = InstanceType<typeof WebSocket>

export interface WSConnectCallbacks {
  onConnected: (ws: ClientWebSocket) => void
  onDisconnected: () => void
  onMessage: (raw: string) => void
}

export function openWebSocket(
  url: string,
  protocols: string | string[] | undefined,
  callbacks: WSConnectCallbacks,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let settled = false
    const ws = protocols === undefined ? new WebSocket(url) : new WebSocket(url, protocols)

    const onOpen = () => {
      settled = true
      callbacks.onConnected(ws)
      resolve()
    }

    const onError = () => {
      if (!settled) {
        settled = true
        reject(new RemoteError('CONNECTION_ERROR', `Failed to connect to ${url}`))
      }
    }

    const onClose = (event: CloseEvent) => {
      ws.removeEventListener('open', onOpen)
      ws.removeEventListener('error', onError)

      if (!settled) {
        settled = true
        reject(new RemoteError('CONNECTION_ERROR', `Connection closed during handshake: ${event.code} ${event.reason}`))
        return
      }

      callbacks.onDisconnected()
    }

    const onMessage = (event: MessageEvent) => {
      callbacks.onMessage(String(event.data))
    }

    ws.addEventListener('open', onOpen)
    ws.addEventListener('error', onError)
    ws.addEventListener('close', onClose)
    ws.addEventListener('message', onMessage)
  })
}
