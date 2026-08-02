import { isRefusalCloseCode, refusalErrorCode } from '../../core/ws-handshake.js'
import { RemoteError } from '../types.js'
import { runtimeSupportsHandshakeHeaders } from './ws-headers.js'

export type ClientWebSocket = InstanceType<typeof WebSocket>

export interface WSCloseInfo {
  code: number
  reason: string
}

export interface WSConnectCallbacks {
  onConnected: (ws: ClientWebSocket) => void
  onRefused: (error: RemoteError) => void
  onDisconnected: () => void
  onMessage: (raw: string) => void
}

export interface WSHandshakeOptions {
  protocols?: string | string[]
  headers?: Record<string, string>
}

interface HeaderCapableWebSocket {
  new (
    url: string,
    init: { headers: Record<string, string>; protocols?: string | string[] },
  ): InstanceType<typeof WebSocket>
}

function createSocket(url: string, options: WSHandshakeOptions): ClientWebSocket {
  const { protocols, headers } = options

  if (headers !== undefined && Object.keys(headers).length > 0 && runtimeSupportsHandshakeHeaders()) {
    const construct = WebSocket as unknown as HeaderCapableWebSocket
    return new construct(url, protocols === undefined ? { headers } : { headers, protocols })
  }

  return protocols === undefined ? new WebSocket(url) : new WebSocket(url, protocols)
}

function readCloseInfo(event: CloseEvent): WSCloseInfo {
  const raw = event as { code?: number; reason?: string }
  return {
    code: typeof raw.code === 'number' ? raw.code : 0,
    reason: typeof raw.reason === 'string' ? raw.reason : '',
  }
}

function refusalError(close: WSCloseInfo): RemoteError {
  const detail = close.reason.length > 0 ? close.reason : 'no reason given'
  return new RemoteError(refusalErrorCode(close.code), `The server refused the WebSocket connection: ${detail}`)
}

export function openWebSocket(url: string, options: WSHandshakeOptions, callbacks: WSConnectCallbacks): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let settled = false
    const ws = createSocket(url, options)

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
      const close = readCloseInfo(event)
      const refusal = isRefusalCloseCode(close.code) ? refusalError(close) : null

      if (refusal) {
        callbacks.onRefused(refusal)
      }

      if (!settled) {
        settled = true
        reject(
          refusal ??
            new RemoteError('CONNECTION_ERROR', `Connection closed during handshake: ${close.code} ${close.reason}`),
        )
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
