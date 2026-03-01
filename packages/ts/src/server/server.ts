import type { us_listen_socket } from 'uWebSockets.js'
import uWS from 'uWebSockets.js'
import type { Sirannon } from '../core/sirannon.js'
import type { CorsOptions, ServerOptions } from '../core/types.js'
import { handleLiveness, handleReadiness } from './health.js'
import type { DbRouteHandler } from './http-handler.js'
import { handleExecute, handleQuery, handleTransaction, initAbortHandler, readBody, sendError } from './http-handler.js'

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

/**
 * Run the user-supplied auth function against pre-extracted request headers.
 * Headers must be captured synchronously from req before calling this,
 * since uWS HttpRequest is stack-allocated and invalid after any await.
 *
 * Returns true if the request is allowed, false if denied (and the
 * 401 response has already been sent).
 */
async function checkAuth(
  res: uWS.HttpResponse,
  headers: Record<string, string>,
  authFn: NonNullable<ServerOptions['auth']>,
): Promise<boolean> {
  let allowed: boolean
  try {
    allowed = await authFn({ headers })
  } catch {
    sendError(res, 500, 'AUTH_ERROR', 'Auth handler threw an error')
    return false
  }

  if (!allowed) {
    sendError(res, 401, 'UNAUTHORIZED', 'Authentication required')
    return false
  }
  return true
}

export class SirannonServer {
  private app: uWS.TemplatedApp
  private listenSocket: us_listen_socket | null = null
  private readonly host: string
  private readonly port: number
  private readonly cors: ResolvedCors | null
  private readonly authFn: ServerOptions['auth']
  private readonly sirannon: Sirannon

  constructor(sirannon: Sirannon, options?: ServerOptions) {
    this.sirannon = sirannon
    this.host = options?.host ?? '127.0.0.1'
    this.port = options?.port ?? 9876
    this.cors = resolveCors(options?.cors)
    this.authFn = options?.auth
    this.app = uWS.App()
    this.registerRoutes()
  }

  /**
   * Start listening for connections. Resolves once the server is bound
   * to the configured host and port.
   */
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

  /**
   * Gracefully close the server. Stops accepting new connections and
   * closes the listen socket.
   */
  close(): Promise<void> {
    return new Promise(resolve => {
      if (this.listenSocket) {
        uWS.us_listen_socket_close(this.listenSocket)
        this.listenSocket = null
      }
      // uWS closes synchronously once the listen socket is closed
      resolve()
    })
  }

  /**
   * Returns the port the server is listening on, or -1 if not listening.
   */
  get listeningPort(): number {
    if (!this.listenSocket) return -1
    return uWS.us_socket_local_port(this.listenSocket as unknown as uWS.us_socket)
  }

  private registerRoutes(): void {
    // CORS preflight — writeStatus MUST come before writeHeader in uWS
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

    // Health endpoints (no auth required)
    this.app.get('/health', this.withCors(handleLiveness()))
    this.app.get('/health/ready', this.withCors(handleReadiness(this.sirannon)))

    // Database endpoints — parameter extraction happens in wrapDbRoute
    // to capture the route param synchronously before any async work,
    // since uWS HttpRequest is stack-allocated and invalid after return.
    this.app.post('/db/:id/query', this.wrapDbRoute(handleQuery(this.sirannon)))
    this.app.post('/db/:id/execute', this.wrapDbRoute(handleExecute(this.sirannon)))
    this.app.post('/db/:id/transaction', this.wrapDbRoute(handleTransaction(this.sirannon)))

    // Catch-all for unmatched routes
    this.app.any('/*', res => {
      sendError(res, 404, 'NOT_FOUND', 'Route not found')
    })
  }

  /**
   * Wrap a handler to add CORS headers on the response.
   */
  private withCors(
    handler: (res: uWS.HttpResponse, req: uWS.HttpRequest) => void,
  ): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    if (!this.cors) return handler

    const corsHeaders = this.cors
    return (res, req) => {
      // Prepend CORS headers. The handler's cork() call will include them.
      res.writeHeader('Access-Control-Allow-Origin', corsHeaders.origin)
      handler(res, req)
    }
  }

  /**
   * Wrap a database route handler with parameter extraction, CORS,
   * body reading, and auth.
   *
   * All data is extracted from the stack-allocated uWS HttpRequest
   * synchronously before the callback returns. Body reading (onData)
   * and abort tracking (onAborted) are both registered in this
   * synchronous window so that uWS doesn't discard body data during
   * an async auth check.
   *
   * The handler receives a pre-read Buffer so it never touches
   * HttpRequest or body reading itself.
   */
  private wrapDbRoute(handler: DbRouteHandler): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    const authFn = this.authFn
    const corsHeaders = this.cors
    const MAX_BODY = 1_048_576

    return (res, req) => {
      // Capture route param synchronously — req is invalid after return
      const dbId = req.getParameter(0) ?? ''

      if (corsHeaders) {
        res.writeHeader('Access-Control-Allow-Origin', corsHeaders.origin)
      }

      const abort = initAbortHandler(res)
      const bodyPromise = readBody(res, MAX_BODY, abort)

      if (!authFn) {
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

      const authPromise = checkAuth(res, headers, authFn)

      Promise.all([bodyPromise, authPromise])
        .then(([rawBody, allowed]) => {
          if (abort.aborted || !allowed) return
          handler(res, dbId, rawBody)
        })
        .catch(() => {})
    }
  }
}

/**
 * Create a sirannon-db HTTP server wrapping the given Sirannon instance.
 *
 * ```ts
 * const sirannon = new Sirannon()
 * sirannon.open('main', './data/main.db')
 *
 * const server = createServer(sirannon, { port: 3000, cors: true })
 * await server.listen()
 * ```
 */
export function createServer(sirannon: Sirannon, options?: ServerOptions): SirannonServer {
  return new SirannonServer(sirannon, options)
}
