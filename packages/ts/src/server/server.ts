import type { us_listen_socket } from 'uWebSockets.js'
import uWS from 'uWebSockets.js'
import type { Sirannon } from '../core/sirannon.js'
import type { CorsOptions, OnRequestHook, RequestContext, RequestDenial, ServerOptions } from '../core/types.js'
import { handleLiveness, handleReadiness } from './health.js'
import type { DbRouteHandler } from './http-handler.js'
import { handleExecute, handleQuery, handleTransaction, initAbortHandler, readBody, sendError } from './http-handler.js'
import type { WSConnection } from './ws-handler.js'
import { WSHandler } from './ws-handler.js'

interface ResolvedCors {
  origin: string
  methods: string
  headers: string
}

function resolveCors(cors: boolean | CorsOptions | undefined): ResolvedCors | null {
  if (!cors) return null
  if (cors === true) {
    return {
      origin: '*',
      methods: 'GET, POST, OPTIONS',
      headers: 'Content-Type, Authorization',
    }
  }
  const origins = Array.isArray(cors.origin) ? cors.origin.join(', ') : (cors.origin ?? '*')
  return {
    origin: origins,
    methods: cors.methods?.join(', ') ?? 'GET, POST, OPTIONS',
    headers: cors.headers?.join(', ') ?? 'Content-Type, Authorization',
  }
}

function decodeRemoteAddress(res: uWS.HttpResponse): string {
  return Buffer.from(res.getRemoteAddressAsText()).toString()
}

function isRequestDenial(value: unknown): value is RequestDenial {
  return typeof value === 'object' && value !== null && 'status' in value
}

async function runOnRequest(res: uWS.HttpResponse, ctx: RequestContext, hook: OnRequestHook): Promise<boolean> {
  try {
    const result = await hook(ctx)
    if (isRequestDenial(result)) {
      sendError(res, result.status, result.code, result.message)
      return false
    }
    return true
  } catch {
    sendError(res, 500, 'HOOK_ERROR', 'onRequest hook threw an error')
    return false
  }
}

interface WSUserData {
  databaseId: string
  conn?: WSConnection
}

export class SirannonServer {
  private app: uWS.TemplatedApp
  private listenSocket: us_listen_socket | null = null
  private readonly host: string
  private readonly port: number
  private readonly cors: ResolvedCors | null
  private readonly onRequestHook: OnRequestHook | undefined
  private readonly sirannon: Sirannon
  private readonly wsHandler: WSHandler

  constructor(sirannon: Sirannon, options?: ServerOptions) {
    this.sirannon = sirannon
    this.host = options?.host ?? '127.0.0.1'
    this.port = options?.port ?? 9876
    this.cors = resolveCors(options?.cors)
    this.onRequestHook = options?.onRequest
    this.wsHandler = new WSHandler(sirannon)
    this.app = uWS.App()
    this.registerRoutes()
  }

  listen(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.app.listen(this.host, this.port, socket => {
        if (socket) {
          this.listenSocket = socket
          resolve()
        } else {
          reject(new Error(`Failed to listen on ${this.host}:${this.port}`))
        }
      })
    })
  }

  close(): Promise<void> {
    return new Promise(resolve => {
      this.wsHandler.close()
      if (this.listenSocket) {
        uWS.us_listen_socket_close(this.listenSocket)
        this.listenSocket = null
      }
      resolve()
    })
  }

  get listeningPort(): number {
    if (!this.listenSocket) return -1
    return uWS.us_socket_local_port(this.listenSocket as unknown as uWS.us_socket)
  }

  private registerRoutes(): void {
    if (this.cors) {
      const cors = this.cors
      this.app.options('/*', res => {
        res.cork(() => {
          res
            .writeStatus('204 No Content')
            .writeHeader('Access-Control-Allow-Origin', cors.origin)
            .writeHeader('Access-Control-Allow-Methods', cors.methods)
            .writeHeader('Access-Control-Allow-Headers', cors.headers)
            .writeHeader('Access-Control-Max-Age', '86400')
            .endWithoutBody()
        })
      })
    }

    this.app.get('/health', this.withCors(handleLiveness()))
    this.app.get('/health/ready', this.withCors(handleReadiness(this.sirannon)))

    this.app.post('/db/:id/query', this.wrapDbRoute(handleQuery(this.sirannon)))
    this.app.post('/db/:id/execute', this.wrapDbRoute(handleExecute(this.sirannon)))
    this.app.post('/db/:id/transaction', this.wrapDbRoute(handleTransaction(this.sirannon)))

    this.registerWebSocketRoute()

    this.app.any('/*', res => {
      sendError(res, 404, 'NOT_FOUND', 'Route not found')
    })
  }

  private registerWebSocketRoute(): void {
    const wsHandler = this.wsHandler
    const onRequestHook = this.onRequestHook

    this.app.ws<WSUserData>('/db/:id', {
      maxPayloadLength: 1_048_576,
      idleTimeout: 120,
      sendPingsAutomatically: true,

      upgrade: (res, req, context) => {
        const dbId = req.getParameter(0) ?? ''
        const url = req.getUrl()
        const method = req.getMethod()
        const secWebSocketKey = req.getHeader('sec-websocket-key')
        const secWebSocketProtocol = req.getHeader('sec-websocket-protocol')
        const secWebSocketExtensions = req.getHeader('sec-websocket-extensions')

        const headers: Record<string, string> = {}
        req.forEach((key, value) => {
          headers[key] = value
        })

        const remoteAddress = decodeRemoteAddress(res)
        let aborted = false
        res.onAborted(() => {
          aborted = true
        })

        if (!onRequestHook) {
          if (!aborted) {
            res.upgrade<WSUserData>(
              { databaseId: dbId },
              secWebSocketKey,
              secWebSocketProtocol,
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

        runOnRequest(res, ctx, onRequestHook)
          .then(allowed => {
            if (aborted || !allowed) return
            res.upgrade<WSUserData>(
              { databaseId: dbId },
              secWebSocketKey,
              secWebSocketProtocol,
              secWebSocketExtensions,
              context,
            )
          })
          .catch(() => {})
      },

      open: ws => {
        const userData = ws.getUserData()
        const conn: WSConnection = {
          send(data: string) {
            try {
              ws.send(data, false)
            } catch {
              /* connection may already be closing */
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
        wsHandler.handleOpen(conn, userData.databaseId)
      },

      message: (ws, message) => {
        const userData = ws.getUserData()
        if (!userData.conn) return
        const text = Buffer.from(message).toString('utf-8')
        wsHandler.handleMessage(userData.conn, text)
      },

      close: ws => {
        const userData = ws.getUserData()
        if (!userData.conn) return
        wsHandler.handleClose(userData.conn)
        userData.conn = undefined
      },
    })
  }

  private withCors(
    handler: (res: uWS.HttpResponse, req: uWS.HttpRequest) => void,
  ): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    if (!this.cors) return handler

    const corsHeaders = this.cors
    return (res, req) => {
      res.writeHeader('Access-Control-Allow-Origin', corsHeaders.origin)
      handler(res, req)
    }
  }

  private wrapDbRoute(handler: DbRouteHandler): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    const onRequestHook = this.onRequestHook
    const corsHeaders = this.cors
    const MAX_BODY = 1_048_576

    return (res, req) => {
      const dbId = req.getParameter(0) ?? ''
      const method = req.getMethod()
      const path = req.getUrl()

      if (corsHeaders) {
        res.writeHeader('Access-Control-Allow-Origin', corsHeaders.origin)
      }

      const abort = initAbortHandler(res)
      const bodyPromise = readBody(res, MAX_BODY, abort)

      if (!onRequestHook) {
        bodyPromise
          .then(rawBody => {
            if (abort.aborted) return
            handler(res, dbId, rawBody)
          })
          .catch(() => {})
        return
      }

      const headers: Record<string, string> = {}
      req.forEach((key, value) => {
        headers[key] = value
      })

      const remoteAddress = decodeRemoteAddress(res)
      const ctx: RequestContext = {
        headers,
        method,
        path,
        databaseId: dbId,
        remoteAddress,
      }

      const hookPromise = runOnRequest(res, ctx, onRequestHook)

      Promise.all([bodyPromise, hookPromise])
        .then(([rawBody, allowed]) => {
          if (abort.aborted || !allowed) return
          handler(res, dbId, rawBody)
        })
        .catch(() => {})
    }
  }
}

export function createServer(sirannon: Sirannon, options?: ServerOptions): SirannonServer {
  return new SirannonServer(sirannon, options)
}
