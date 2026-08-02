import type uWS from 'uWebSockets.js'
import type { AuthenticateHook, RequestContext } from '../core/types.js'
import { SIRANNON_WS_SUBPROTOCOL, selectSubprotocol } from '../core/ws-handshake.js'
import { initAbortHandler, sendError } from './http-common.js'
import type { UpgradeRefusal } from './request-hook.js'
import { authenticateUpgrade, decodeRemoteAddress } from './request-hook.js'
import type { WSConnection, WSSendOutcome } from './ws-connection.js'
import type { WSHandler } from './ws-handler.js'

export interface WSUserData {
  databaseId: string
  identity?: unknown
  refusal?: UpgradeRefusal
  conn?: WSConnection
}

const UNSUPPORTED_SUBPROTOCOL_MESSAGE = `The upgrade offered no subprotocol this server supports. Offer '${SIRANNON_WS_SUBPROTOCOL}' alongside any credential-bearing value, or offer none at all.`

export interface WebSocketRouteOptions {
  app: uWS.TemplatedApp
  wsHandler: WSHandler
  authenticateHook: AuthenticateHook<unknown> | undefined
  maxBodyBytes: number
  maxBackpressureBytes: number
}

function toSendOutcome(result: number): WSSendOutcome {
  if (result === 2) return 'dropped'
  if (result === 0) return 'buffered'
  return 'sent'
}

export function registerWebSocketRoute(options: WebSocketRouteOptions): void {
  const { app, wsHandler, authenticateHook } = options

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
      const negotiated = selectSubprotocol(secWebSocketProtocol)
      const secWebSocketExtensions = req.getHeader('sec-websocket-extensions')

      const headers: Record<string, string> = {}
      req.forEach((key, value) => {
        headers[key] = value
      })

      const remoteAddress = decodeRemoteAddress(res)
      const abort = initAbortHandler(res)

      if (!negotiated.ok) {
        if (abort.claim()) {
          sendError(res, 400, 'UNSUPPORTED_SUBPROTOCOL', UNSUPPORTED_SUBPROTOCOL_MESSAGE)
        }
        return
      }

      const selectedWebSocketProtocol = negotiated.protocol

      if (!authenticateHook) {
        if (abort.claim()) {
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

      authenticateUpgrade(res, abort, ctx, authenticateHook)
        .then(authenticated => {
          if (!authenticated.ok && !authenticated.refusal) return
          if (!abort.claim()) return

          const userData: WSUserData = authenticated.ok
            ? { databaseId: dbId, identity: authenticated.identity }
            : { databaseId: dbId, refusal: authenticated.refusal }

          res.upgrade<WSUserData>(userData, secWebSocketKey, selectedWebSocketProtocol, secWebSocketExtensions, context)
        })
        .catch(() => {})
    },

    open: ws => {
      const userData = ws.getUserData()
      const refusal = userData.refusal
      if (refusal) {
        try {
          ws.end(refusal.code, refusal.reason)
        } catch {}
        return
      }

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
          } catch {}
        },
      }
      userData.conn = conn
      wsHandler.handleOpen(conn, userData.databaseId, userData.identity).catch(() => {})
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

    drain: ws => {
      const userData = ws.getUserData()
      if (userData.conn) {
        wsHandler.handleSocketDrain(userData.conn)
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
