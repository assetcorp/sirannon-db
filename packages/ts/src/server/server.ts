import type { us_listen_socket } from 'uWebSockets.js'
import uWS from 'uWebSockets.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type {
  AuthenticateHook,
  ClusterStatusAuthorizer,
  ClusterStatusInfo,
  OperationRegistry,
  ReplicationStatusInfo,
  RequestContext,
  ServerExecutionTargetResolver,
  ServerOptions,
} from '../core/types.js'
import { handleCapabilities } from './capabilities.js'
import type { ResolvedCors } from './cors.js'
import { resolveCors, writeCorsOrigin } from './cors.js'
import { handleLiveness, handleReadiness } from './health.js'
import { SQL_NOT_ACCEPTED_MESSAGE } from './http-common.js'
import type { DbGetRouteHandler, DbRouteHandler } from './http-handler.js'
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
import { handleMigrationList } from './http-migrations.js'
import type { OperationRouteHandler } from './http-operations.js'
import { handleOperationExecute, handleOperationQuery } from './http-operations.js'
import { handleSnapshotManifest, handleSnapshotPage } from './http-snapshot.js'
import { operationRegistryDigest } from './operation-lookup.js'
import { wrapOperationRoute } from './operation-route.js'
import { decodeRemoteAddress, runAuthenticate } from './request-hook.js'
import { WSHandler } from './ws-handler.js'
import { registerWebSocketRoute } from './ws-route.js'

const SQL_ROUTES = ['/db/:id/query', '/db/:id/execute', '/db/:id/transaction', '/db/:id/batch', '/db/:id/load'] as const

function refuseSql(res: uWS.HttpResponse): void {
  sendError(res, 403, 'SQL_NOT_ACCEPTED', SQL_NOT_ACCEPTED_MESSAGE)
}

const DEFAULT_MAX_BODY_BYTES = 1_048_576
const DEFAULT_WS_BACKPRESSURE_BYTES = 16 * 1_048_576
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

/**
 * @public
 *
 * Serves a `Sirannon` database registry over HTTP and WebSocket.
 *
 * Build one with {@link createServer}, then call {@link SirannonServer.listen}.
 */
export class SirannonServer<Identity = unknown> {
  private app: uWS.TemplatedApp
  private listenSocket: us_listen_socket | null = null
  private readonly host: string
  private readonly port: number
  private readonly cors: ResolvedCors | null
  private readonly authenticateHook: AuthenticateHook<Identity> | undefined
  private readonly acceptSql: boolean
  private readonly operations: OperationRegistry<Identity> | undefined
  private readonly registryDigest: string | undefined
  private readonly resolveExecutionTarget: ServerExecutionTargetResolver | undefined
  private readonly getReplicationStatus: (() => ReplicationStatusInfo | null) | undefined
  private readonly getClusterStatus: ((databaseId: string) => ClusterStatusInfo | null) | undefined
  private readonly authorizeClusterStatus: ClusterStatusAuthorizer | undefined
  private readonly sirannon: Sirannon
  private readonly wsHandler: WSHandler<Identity>
  private readonly maxBodyBytes: number
  private readonly maxWsBackpressureBytes: number

  constructor(sirannon: Sirannon, options?: ServerOptions<Identity>) {
    this.sirannon = sirannon
    this.host = options?.host ?? '127.0.0.1'
    this.port = options?.port ?? 9876
    this.cors = resolveCors(options?.cors)
    this.authenticateHook = options?.authenticate
    this.acceptSql = options?.acceptSql === true
    this.operations = options?.operations
    this.registryDigest = operationRegistryDigest(options?.operations)
    this.resolveExecutionTarget = options?.resolveExecutionTarget
    this.getReplicationStatus = options?.getReplicationStatus
    this.getClusterStatus = options?.getClusterStatus
    this.authorizeClusterStatus = options?.authorizeClusterStatus
    this.maxBodyBytes = resolveMaxBodyBytes(options?.maxBodyBytes)
    this.maxWsBackpressureBytes = resolveWsBackpressure(options?.maxWebSocketBackpressureBytes, this.maxBodyBytes)
    this.wsHandler = new WSHandler<Identity>(sirannon, {
      resolveExecutionTarget: this.resolveExecutionTarget,
      maxPayloadLength: this.maxBodyBytes,
      cdcRetentionMs: options?.cdcRetentionMs,
      deviceCursorRetentionMs: options?.deviceCursorRetentionMs,
      maxUnacknowledgedChanges: options?.maxUnacknowledgedChanges,
      acceptSql: this.acceptSql,
      operations: options?.operations,
    })
    this.app = uWS.App()
    this.registerRoutes()
  }

  /**
   * Binds the configured host and port and starts serving.
   *
   * @throws When the port is already in use.
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
   * Stops serving and closes every open connection.
   */
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

  /**
   * Port the server bound to, which is the resolved port when you asked for 0.
   */
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

    this.app.get(
      '/capabilities',
      this.withCors(handleCapabilities({ registryDigest: this.registryDigest, acceptSql: this.acceptSql })),
    )
    this.app.get('/health', this.withCors(handleLiveness()))
    this.app.get('/health/ready', this.withCors(handleReadiness(this.sirannon, this.getReplicationStatus)))
    this.app.get(
      '/db/:id/cluster',
      this.wrapDbGetRoute(handleClusterStatus(this.getClusterStatus, this.authorizeClusterStatus)),
    )

    if (this.acceptSql) {
      this.app.post('/db/:id/query', this.wrapDbRoute(handleQuery(this.sirannon, this.resolveExecutionTarget)))
      this.app.post('/db/:id/execute', this.wrapDbRoute(handleExecute(this.sirannon, this.resolveExecutionTarget)))
      this.app.post(
        '/db/:id/transaction',
        this.wrapDbRoute(handleTransaction(this.sirannon, this.resolveExecutionTarget)),
      )
      this.app.post('/db/:id/batch', this.wrapDbRoute(handleBatch(this.sirannon, this.resolveExecutionTarget)))
      this.app.post('/db/:id/load', this.wrapDbRoute(handleLoad(this.sirannon, this.resolveExecutionTarget)))
    } else {
      for (const route of SQL_ROUTES) {
        this.app.post(route, this.withCors(refuseSql))
      }
    }

    this.app.post(
      '/db/:id/query/:name',
      this.wrapOperationRoute(handleOperationQuery(this.sirannon, this.operations, this.resolveExecutionTarget)),
    )
    this.app.post(
      '/db/:id/execute/:name',
      this.wrapOperationRoute(handleOperationExecute(this.sirannon, this.operations, this.resolveExecutionTarget)),
    )

    this.app.post('/db/:id/changes', this.wrapDbRoute(handleChanges(this.sirannon, this.resolveExecutionTarget)))
    this.app.post('/db/:id/migrations', this.wrapDbRoute(handleMigrationList(this.sirannon)))
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
      authenticateHook: this.authenticateHook,
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
    const authenticateHook = this.authenticateHook
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

      if (!authenticateHook) {
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

      const hookPromise = runAuthenticate(res, abort, ctx, authenticateHook)

      Promise.all([bodyPromise, hookPromise])
        .then(async ([rawBody, authenticated]) => {
          if (!authenticated.ok || !abort.claim()) return
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

  private wrapOperationRoute(handler: OperationRouteHandler): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    return wrapOperationRoute<Identity>(
      {
        cors: this.cors,
        maxBodyBytes: this.maxBodyBytes,
        authenticateHook: this.authenticateHook,
      },
      handler,
    )
  }

  private wrapDbGetRoute(handler: DbGetRouteHandler): (res: uWS.HttpResponse, req: uWS.HttpRequest) => void {
    const authenticateHook = this.authenticateHook
    const corsHeaders = this.cors

    return (res, req) => {
      const dbId = req.getParameter(0) ?? ''
      const method = req.getMethod()
      const path = req.getUrl()

      if (corsHeaders) {
        writeCorsOrigin(res, corsHeaders, req.getHeader('origin'))
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
      const run = async (): Promise<void> => {
        if (authenticateHook && !(await runAuthenticate(res, abort, ctx, authenticateHook)).ok) return
        if (!abort.claim()) return
        try {
          await handler(res, dbId, ctx, abort)
        } catch {
          if (!abort.aborted) {
            sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected error occurred')
          }
        }
      }
      run().catch(() => {})
    }
  }
}

/**
 * @public
 *
 * Builds a server over a database registry.
 *
 * @param sirannon - The registry whose databases the server exposes.
 * @param options - Address, cross-origin rules, size limits, authentication, registered operations, and whether the server accepts SQL.
 * @returns The server, ready to listen.
 */
export function createServer<Identity = unknown>(
  sirannon: Sirannon,
  options?: ServerOptions<Identity>,
): SirannonServer<Identity> {
  return new SirannonServer<Identity>(sirannon, options)
}
