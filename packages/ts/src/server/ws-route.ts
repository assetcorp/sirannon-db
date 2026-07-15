import type uWS from 'uWebSockets.js'
import type { OnRequestHook, RequestContext } from '../core/types.js'
import { initAbortHandler } from './http-common.js'
import { decodeRemoteAddress, runOnRequest } from './request-hook.js'
import type { WSConnection, WSSendOutcome } from './ws-connection.js'
import type { WSHandler } from './ws-handler.js'

export interface WSUserData {
  databaseId: string
  conn?: WSConnection
}

export interface WebSocketRouteOptions {
  app: uWS.TemplatedApp
  wsHandler: WSHandler
  onRequestHook: OnRequestHook | undefined
  maxBodyBytes: number
  maxBackpressureBytes: number
}

function toSendOutcome(result: number): WSSendOutcome {
  if (result === 2) return 'dropped'
  if (result === 0) return 'buffered'
  return 'sent'
}

function selectWebSocketProtocol(header: string): string {
  const [firstProtocol] = header.split(',')
  return firstProtocol?.trim() ?? ''
}

export function registerWebSocketRoute(options: WebSocketRouteOptions): void {
  const { app, wsHandler, onRequestHook } = options

  app.ws<WSUserData>('/db/:id', {
    maxPayloadLength: options.maxBodyBytes,
    maxBackpressure: options.maxBackpressureBytes,
    idleTimeout: 120,
    sendPingsAutomatically: true,

    upgrade: (res, req, context) => {
      const dbId = req.getParameter(0) ?? ''
      const url = req.getUrl()
      const method = req.getMethod()
      const secWebSocketKey = req.getHeader('sec-websocket-key')
      const secWebSocketProtocol = req.getHeader('sec-websocket-protocol')
      const selectedWebSocketProtocol = selectWebSocketProtocol(secWebSocketProtocol)
      const secWebSocketExtensions = req.getHeader('sec-websocket-extensions')

      const headers: Record<string, string> = {}
      req.forEach((key, value) => {
        headers[key] = value
      })

      const remoteAddress = decodeRemoteAddress(res)
      const abort = initAbortHandler(res)

      if (!onRequestHook) {
        if (!abort.aborted) {
          res.upgrade<WSUserData>(
            { databaseId: dbId },
            secWebSocketKey,
            selectedWebSocketProtocol,
            secWebSocketExtensions,
            context,
          )
        }
        return
      }

      const ctx: RequestContext = {
        headers,
        method,
        path: url,
        databaseId: dbId,
        remoteAddress,
      }

      runOnRequest(res, abort, ctx, onRequestHook)
        .then(allowed => {
          if (abort.aborted || !allowed) return
          res.upgrade<WSUserData>(
            { databaseId: dbId },
            secWebSocketKey,
            selectedWebSocketProtocol,
            secWebSocketExtensions,
            context,
          )
        })
        .catch(() => {})
    },

    open: ws => {
      const userData = ws.getUserData()
      const conn: WSConnection = {
        send(data: string): WSSendOutcome {
          try {
            return toSendOutcome(ws.send(data, false))
          } catch {
            return 'dropped'
          }
        },
        close(code?: number, reason?: string) {
          try {
            ws.end(code, reason)
          } catch {
            /* already closed */
          }
        },
      }
      userData.conn = conn
      wsHandler.handleOpen(conn, userData.databaseId).catch(() => {})
    },

    message: (ws, message) => {
      const userData = ws.getUserData()
      if (!userData.conn) return
      const text = Buffer.from(message).toString('utf-8')
      wsHandler.handleMessage(userData.conn, text)
    },

    dropped: ws => {
      const userData = ws.getUserData()
      if (userData.conn) {
        wsHandler.handleOverload(userData.conn)
      }
    },

    close: ws => {
      const userData = ws.getUserData()
      if (!userData.conn) return
      wsHandler.handleClose(userData.conn)
      userData.conn = undefined
    },
  })
}
