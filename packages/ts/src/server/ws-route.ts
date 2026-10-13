import type uWS from 'uWebSockets.js'
import type { AuthenticateHook, RequestContext } from '../core/types.js'
import { SIRANNON_WS_SUBPROTOCOL, selectSubprotocol } from '../core/ws-handshake.js'
import {
  DATABASE_ID_SEGMENT,
  decodePathSegment,
  initAbortHandler,
  sendError,
  sendUndecodablePathSegment,
} from './http-common.js'
import { UWS_MAX_LIMIT_BYTES } from './limits.js'
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

const UWS_SEND_BACKPRESSURE = 0
const UWS_SEND_DROPPED = 2
const UWS_UNLIMITED_BACKPRESSURE = 0
const OVERSIZED_MESSAGE_CEILING_FACTOR = 2

function toSendOutcome(result: number): WSSendOutcome {
  if (result === UWS_SEND_DROPPED) return 'dropped'
  if (result === UWS_SEND_BACKPRESSURE) return 'buffered'
  return 'sent'
}

export function registerWebSocketRoute(options: WebSocketRouteOptions): void {
  const { app, wsHandler, authenticateHook } = options

  const { maxBackpressureBytes } = options

  app.ws<WSUserData>('/db/:id', {
    maxPayloadLength: Math.min(options.maxBodyBytes * OVERSIZED_MESSAGE_CEILING_FACTOR, UWS_MAX_LIMIT_BYTES),
    maxBackpressure: UWS_UNLIMITED_BACKPRESSURE,
    idleTimeout: 120,
    sendPingsAutomatically: true,

    upgrade: (res, req, context) => {
      const rawId = req.getParameter(0) ?? ''
      const dbId = decodePathSegment(rawId)
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
        if (!abort.claim()) return
        if (dbId === null) {
          sendUndecodablePathSegment(res, DATABASE_ID_SEGMENT, rawId)
        } else {
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
        databaseId: dbId ?? rawId,
        remoteAddress,
      }

      authenticateUpgrade(res, abort, ctx, authenticateHook)
        .then(authenticated => {
          if (!authenticated.ok && !authenticated.refusal) return
          if (!abort.claim()) return
          if (authenticated.ok && dbId === null) {
            sendUndecodablePathSegment(res, DATABASE_ID_SEGMENT, rawId)
            return
          }

          const userData: WSUserData = authenticated.ok
            ? { databaseId: dbId ?? rawId, identity: authenticated.identity }
            : { databaseId: dbId ?? rawId, refusal: authenticated.refusal }

          res.cork(() => {
            res.upgrade<WSUserData>(
              userData,
              secWebSocketKey,
              selectedWebSocketProtocol,
              secWebSocketExtensions,
              context,
            )
          })
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

      let closing = false
      let backpressured = false
      const conn: WSConnection = {
        send(data: string): WSSendOutcome {
          if (closing) return 'dropped'
          try {
            if (backpressured) {
              const buffered = ws.getBufferedAmount()
              if (buffered > maxBackpressureBytes) return 'dropped'
              backpressured = buffered > 0
            }
            const result = ws.send(data, false)
            if (result === UWS_SEND_BACKPRESSURE) backpressured = true
            return toSendOutcome(result)
          } catch {
            return 'dropped'
          }
        },
        bufferedAmount(): number {
          try {
            return ws.getBufferedAmount()
          } catch {
            return 0
          }
        },
        flush(): void {
          try {
            ws.ping()
          } catch {}
        },
        close(code?: number, reason?: string) {
          closing = true
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
