import type { us_listen_socket } from 'uWebSockets.js'
import uWS from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type {
  ClusterStatusInfo,
  OnRequestHook,
  ReplicationStatusInfo,
  RequestContext,
  ServerExecutionTargetResolver,
  ServerOptions,
} from '../core/types.js'
import type { ResolvedCors } from './cors.js'
import { resolveCors, writeCorsOrigin } from './cors.js'
import { handleLiveness, handleReadiness } from './health.js'
import type { DbRouteHandler } from './http-handler.js'
import {
  handleBatch,
  handleChanges,
  handleClusterStatus,
  handleExecute,
  handleLoad,
  handleQuery,
  handleTransaction,
  initAbortHandler,
  readBody,
  sendError,
} from './http-handler.js'
import { handleSnapshotManifest, handleSnapshotPage } from './http-snapshot.js'
import { decodeRemoteAddress, runOnRequest } from './request-hook.js'
import { WSHandler } from './ws-handler.js'
import { registerWebSocketRoute } from './ws-route.js'

const DEFAULT_MAX_BODY_BYTES = 1_048_576
const DEFAULT_WS_BACKPRESSURE_BYTES = 16 * 1_048_576
/**
 * uWebSockets.js reads maxPayloadLength and maxBackpressure into unsigned
 * 32-bit fields (ToInt32 in the addon, unsigned int in the C++ core), so any
 * larger value is silently applied modulo 2^32. Reject those at construction
 * instead of running with a limit the caller never configured.
 */
const UWS_MAX_LIMIT_BYTES = 4_294_967_295

function resolveMaxBodyBytes(value: number | undefined): number {
  if (value === undefined) return DEFAULT_MAX_BODY_BYTES
  if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) {
    throw new SirannonError(
      'ServerOptions.maxBodyBytes must be a positive integer number of bytes',
      'INVALID_MAX_BODY_BYTES',
    )
  }
  if (value > UWS_MAX_LIMIT_BYTES) {
    throw new SirannonError(
      `ServerOptions.maxBodyBytes must be at most ${UWS_MAX_LIMIT_BYTES} bytes; uWebSockets.js stores the limit as an unsigned 32-bit integer and would silently wrap a larger value modulo 2^32`,
      'INVALID_MAX_BODY_BYTES',
    )
  }
  return value
}

function resolveWsBackpressure(value: number | undefined, maxBodyBytes: number): number {
  const resolved = value ?? Math.max(DEFAULT_WS_BACKPRESSURE_BYTES, maxBodyBytes)
  if (typeof resolved !== 'number' || !Number.isInteger(resolved) || resolved <= 0) {
    throw new SirannonError(
      'ServerOptions.maxWebSocketBackpressureBytes must be a positive integer number of bytes',
      'INVALID_WS_BACKPRESSURE',
    )
  }
  if (resolved > UWS_MAX_LIMIT_BYTES) {
    throw new SirannonError(
      `ServerOptions.maxWebSocketBackpressureBytes must be at most ${UWS_MAX_LIMIT_BYTES} bytes; uWebSockets.js stores the limit as an unsigned 32-bit integer and would silently wrap a larger value modulo 2^32`,
      'INVALID_WS_BACKPRESSURE',
    )
  }
  if (resolved < maxBodyBytes) {
    throw new SirannonError(
      'ServerOptions.maxWebSocketBackpressureBytes must be at least maxBodyBytes so a single frame fits',
      'INVALID_WS_BACKPRESSURE',
    )
  }
  return resolved
}

export class SirannonServer {
  private app: uWS.TemplatedApp
  private listenSocket: us_listen_socket | null = null
  private readonly host: string
  private readonly port: number
  private readonly cors: ResolvedCors | null
  private readonly onRequestHook: OnRequestHook | undefined
  private readonly resolveExecutionTarget: ServerExecutionTargetResolver | undefined
  private readonly getReplicationStatus: (() => ReplicationStatusInfo | null) | undefined
  private readonly getClusterStatus: ((databaseId: string) => ClusterStatusInfo | null) | undefined
  private readonly sirannon: Sirannon
  private readonly wsHandler: WSHandler
  private readonly maxBodyBytes: number
  private readonly maxWsBackpressureBytes: number

  constructor(sirannon: Sirannon, options?: ServerOptions) {
    this.sirannon = sirannon
    this.host = options?.host ?? '127.0.0.1'
    this.port = options?.port ?? 9876
    this.cors = resolveCors(options?.cors)
    this.onRequestHook = options?.onRequest
    this.resolveExecutionTarget = options?.resolveExecutionTarget
    this.getReplicationStatus = options?.getReplicationStatus
    this.getClusterStatus = options?.getClusterStatus
    this.maxBodyBytes = resolveMaxBodyBytes(options?.maxBodyBytes)
    this.maxWsBackpressureBytes = resolveWsBackpressure(options?.maxWebSocketBackpressureBytes, this.maxBodyBytes)
    this.wsHandler = new WSHandler(sirannon, {
      resolveExecutionTarget: this.resolveExecutionTarget,
      maxPayloadLength: this.maxBodyBytes,
      cdcRetentionMs: options?.cdcRetentionMs,
      deviceCursorRetentionMs: options?.deviceCursorRetentionMs,
    })
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

  async close(): Promise<void> {
    try {
      await this.wsHandler.close()
    } finally {
      if (this.listenSocket) {
        uWS.us_listen_socket_close(this.listenSocket)
        this.listenSocket = null
      }
    }
  }

  get listeningPort(): number {
    if (!this.listenSocket) return -1
    return uWS.us_socket_local_port(this.listenSocket as unknown as uWS.us_socket)
  }

  private registerRoutes(): void {
    if (this.cors) {
      const cors = this.cors
      this.app.options('/*', (res, req) => {
        const requestOrigin = req.getHeader('origin')
        res.cork(() => {
          res.writeStatus('204 No Content')
          writeCorsOrigin(res, cors, requestOrigin)
          res
            .writeHeader('Access-Control-Allow-Methods', cors.methods)
            .writeHeader('Access-Control-Allow-Headers', cors.headers)
            .writeHeader('Access-Control-Max-Age', '86400')
            .endWithoutBody()
        })
      })
    }

    this.app.get('/health', this.withCors(handleLiveness()))
    this.app.get('/health/ready', this.withCors(handleReadiness(this.sirannon, this.getReplicationStatus)))
    this.app.get('/db/:id/cluster', this.wrapDbGetRoute(handleClusterStatus(this.getClusterStatus)))

    this.app.post('/db/:id/query', this.wrapDbRoute(handleQuery(this.sirannon, this.resolveExecutionTarget)))
    this.app.post('/db/:id/execute', this.wrapDbRoute(handleExecute(this.sirannon, this.resolveExecutionTarget)))
    this.app.post(
      '/db/:id/transaction',
      this.wrapDbRoute(handleTransaction(this.sirannon, this.resolveExecutionTarget)),
    )
    this.app.post('/db/:id/batch', this.wrapDbRoute(handleBatch(this.sirannon, this.resolveExecutionTarget)))
    this.app.post('/db/:id/load', this.wrapDbRoute(handleLoad(this.sirannon, this.resolveExecutionTarget)))
    this.app.post('/db/:id/changes', this.wrapDbRoute(handleChanges(this.sirannon, this.resolveExecutionTarget)))
    this.app.post('/db/:id/snapshot', this.wrapDbRoute(handleSnapshotManifest(this.sirannon)))
    this.app.post('/db/:id/snapshot/page', this.wrapDbRoute(handleSnapshotPage(this.sirannon)))

    this.registerWebSocketRoute()

    this.app.any('/*', res => {
      sendError(res, 404, 'NOT_FOUND', 'Route not found')
    })
  }

  private registerWebSocketRoute(): void {
    registerWebSocketRoute({
      app: this.app,
      wsHandler: this.wsHandler,
      onRequestHook: this.onRequestHook,
      maxBodyBytes: this.maxBodyBytes,
      maxBackpressureBytes: this.maxWsBackpressureBytes,
    })
  }

  private withCors(
    handler: (res: uWS.HttpResponse, req: uWS.HttpRequest) => void,
  ): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    if (!this.cors) return handler

    const cors = this.cors
    return (res, req) => {
      writeCorsOrigin(res, cors, req.getHeader('origin'))
      handler(res, req)
    }
  }

  private wrapDbRoute(handler: DbRouteHandler): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    const onRequestHook = this.onRequestHook
    const corsHeaders = this.cors
    const maxBody = this.maxBodyBytes

    return (res, req) => {
      const dbId = req.getParameter(0) ?? ''
      const method = req.getMethod()
      const path = req.getUrl()

      if (corsHeaders) {
        writeCorsOrigin(res, corsHeaders, req.getHeader('origin'))
      }

      const abort = initAbortHandler(res)
      const bodyPromise = readBody(res, maxBody, abort)

      if (!onRequestHook) {
        bodyPromise
          .then(async rawBody => {
            if (!abort.claim()) return
            try {
              await handler(res, dbId, rawBody, abort)
            } catch {
              if (!abort.aborted) {
                sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
              }
            }
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

      const hookPromise = runOnRequest(res, abort, ctx, onRequestHook)

      Promise.all([bodyPromise, hookPromise])
        .then(async ([rawBody, allowed]) => {
          if (!allowed || !abort.claim()) return
          try {
            await handler(res, dbId, rawBody, abort)
          } catch {
            if (!abort.aborted) {
              sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
            }
          }
        })
        .catch(() => {})
    }
  }

  private wrapDbGetRoute(
    handler: (res: uWS.HttpResponse, dbId: string) => void,
  ): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    const onRequestHook = this.onRequestHook
    const corsHeaders = this.cors

    return (res, req) => {
      const dbId = req.getParameter(0) ?? ''
      const method = req.getMethod()
      const path = req.getUrl()

      if (corsHeaders) {
        writeCorsOrigin(res, corsHeaders, req.getHeader('origin'))
      }

      if (!onRequestHook) {
        handler(res, dbId)
        return
      }

      const headers: Record<string, string> = {}
      req.forEach((key, value) => {
        headers[key] = value
      })

      const ctx: RequestContext = {
        headers,
        method,
        path,
        databaseId: dbId,
        remoteAddress: decodeRemoteAddress(res),
      }

      const abort = initAbortHandler(res)
      runOnRequest(res, abort, ctx, onRequestHook)
        .then(allowed => {
          if (!allowed || !abort.claim()) return
          handler(res, dbId)
        })
        .catch(() => {})
    }
  }
}

export function createServer(sirannon: Sirannon, options?: ServerOptions): SirannonServer {
  return new SirannonServer(sirannon, options)
}
