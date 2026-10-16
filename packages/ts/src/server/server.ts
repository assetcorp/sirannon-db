import type { HttpRequest, HttpResponse, TemplatedApp, us_listen_socket, us_socket } from 'uWebSockets.js'
import type { Sirannon } from '../core/sirannon.js'
import type {
  AuthenticateHook,
  ClusterStatusAuthorizer,
  ClusterStatusInfo,
  ReplicationStatusInfo,
  ServerExecutionTargetResolver,
  ServerOptions,
} from '../core/types.js'
import { BackupRestoreRuns } from './backup-restore-runs.js'
import { handleCapabilities } from './capabilities.js'
import { ConnectionDrain, endResponse, type RouteRegistrar } from './connection-drain.js'
import type { ResolvedCors } from './cors.js'
import { resolveCors, writeCorsOrigin } from './cors.js'
import { wrapDbGetRoute, wrapDbRoute } from './db-route.js'
import { handleLiveness, handleReadiness } from './health.js'
import { handleBackupRestore, handleBackupRestoreStatus } from './http-backup-restore.js'
import {
  handleBackupChain,
  handleBackupSafeToDelete,
  handleBackupStatus,
  handleBackupTrigger,
  handleBackupVerify,
} from './http-backups.js'
import { handleChanges } from './http-changes.js'
import { DEVICE_SYNC_NOT_ACCEPTED_MESSAGE, SQL_NOT_ACCEPTED_MESSAGE } from './http-common.js'
import type { DbGetRouteHandler, DbRouteHandler } from './http-handler.js'
import {
  handleBatch,
  handleClusterStatus,
  handleExecute,
  handleLoad,
  handleQuery,
  handleTransaction,
  sendError,
} from './http-handler.js'
import { handleMigrationList } from './http-migrations.js'
import type { OperationRouteHandler } from './http-operations.js'
import { handleOperationExecute, handleOperationQuery } from './http-operations.js'
import { handleSnapshotManifest, handleSnapshotPage } from './http-snapshot.js'
import {
  assertBackupRestoreAuthenticated,
  assertDeviceSyncAuthenticated,
  resolveCloseTimeout,
  resolveMaxBodyBytes,
  resolveWsBackpressure,
} from './limits.js'
import { type OperationSets, operationRegistryDigest } from './operation-lookup.js'
import { type OperationRouteDeps, wrapOperationRoute } from './operation-route.js'
import { loadUWebSockets, type UWebSockets } from './uws-loader.js'
import { WSHandler } from './ws-handler.js'
import { registerWebSocketRoute } from './ws-route.js'

const SQL_ROUTES = ['/db/:id/query', '/db/:id/execute', '/db/:id/transaction', '/db/:id/batch', '/db/:id/load'] as const

interface ServerRuntime {
  uws: UWebSockets
  app: TemplatedApp
}

const DEVICE_SYNC_ROUTES = [
  '/db/:id/changes',
  '/db/:id/migrations',
  '/db/:id/snapshot',
  '/db/:id/snapshot/page',
] as const

function refuseSql(res: HttpResponse): void {
  sendError(res, 403, 'SQL_NOT_ACCEPTED', SQL_NOT_ACCEPTED_MESSAGE)
}

function refuseDeviceSync(res: HttpResponse): void {
  sendError(res, 403, 'DEVICE_SYNC_NOT_ACCEPTED', DEVICE_SYNC_NOT_ACCEPTED_MESSAGE)
}

/**
 * The settings for {@link SirannonServer.close}.
 *
 * @public
 */
export interface ServerCloseOptions {
  /**
   * How long, in milliseconds, the server waits for requests in progress before it ends every connection. The value
   * must be a whole number from 0 to 2_147_483_647, and 0 ends every connection at once. The default is 30_000, the
   * client's default request timeout, because a client on default settings stops waiting for a reply after that long.
   */
  timeoutMs?: number
}

/**
 * Serves a `Sirannon` database registry over HTTP and WebSocket.
 *
 * Build one with {@link createServer}, then call {@link SirannonServer.listen}.
 *
 * @public
 */
export class SirannonServer<Identity = unknown> {
  private runtime: ServerRuntime | null = null
  private runtimeLoad: Promise<ServerRuntime> | null = null
  private listenSocket: us_listen_socket | null = null
  private readonly host: string
  private readonly port: number
  private readonly cors: ResolvedCors | null
  private readonly authenticateHook: AuthenticateHook<Identity> | undefined
  private readonly acceptSql: boolean
  private readonly acceptBackupRestore: boolean
  private readonly acceptDeviceSync: boolean
  private readonly restoreRuns = new BackupRestoreRuns()
  private readonly drain = new ConnectionDrain()
  private readonly operations: OperationSets<Identity>
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
    this.acceptBackupRestore = options?.acceptBackupRestore === true
    assertBackupRestoreAuthenticated(this.acceptBackupRestore, this.authenticateHook !== undefined)
    this.acceptDeviceSync = options?.acceptDeviceSync === true
    assertDeviceSyncAuthenticated(this.acceptDeviceSync, this.authenticateHook !== undefined)
    this.operations = { registry: options?.operations, shared: options?.sharedOperations }
    this.registryDigest = operationRegistryDigest(this.operations.registry, this.operations.shared)
    this.resolveExecutionTarget = options?.resolveExecutionTarget
    this.getReplicationStatus = options?.getReplicationStatus
    this.getClusterStatus = options?.getClusterStatus
    this.authorizeClusterStatus = options?.authorizeClusterStatus
    this.maxBodyBytes = resolveMaxBodyBytes(options?.maxBodyBytes)
    this.maxWsBackpressureBytes = resolveWsBackpressure(options?.maxWebSocketBackpressureBytes, this.maxBodyBytes)
    this.wsHandler = new WSHandler<Identity>(sirannon, {
      resolveExecutionTarget: this.resolveExecutionTarget,
      maxPayloadLength: this.maxBodyBytes,
      maxBackpressureBytes: this.maxWsBackpressureBytes,
      cdcRetentionMs: options?.cdcRetentionMs,
      deviceCursorRetentionMs: options?.deviceCursorRetentionMs,
      maxChangesHeldForDevice: options?.maxChangesHeldForDevice,
      maxUnacknowledgedChanges: options?.maxUnacknowledgedChanges,
      acceptSql: this.acceptSql,
      acceptDeviceSync: this.acceptDeviceSync,
      operations: this.operations.registry,
      sharedOperations: this.operations.shared,
    })
  }

  /**
   * Loads uWebSockets.js and starts serving on the configured host and port.
   *
   * @throws A `SirannonError` with code `SERVER_DEPENDENCY_MISSING` when this process cannot load uWebSockets.js.
   * @throws An `Error` when uWebSockets.js cannot listen on the host and port, for example because the port is already in use.
   */
  async listen(): Promise<void> {
    const { app } = await this.loadRuntime()
    await new Promise<void>((resolve, reject) => {
      app.listen(this.host, this.port, socket => {
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
   * Resolves once the server has stopped listening and has ended every connection, after each request in progress has
   * sent its reply, so that `sirannon.shutdown()` can close the databases next.
   *
   * The server stops listening at once and stops every subscription. It closes each WebSocket connection with close
   * code 1001 once the requests on that connection have replied and its outbound buffer has emptied. Once no request
   * is in progress and every WebSocket connection has closed, it ends every remaining connection, idle keep-alive
   * connections included, so that a client reconnects to whichever server listens next.
   *
   * While the server closes, a request on an open HTTP connection finds that connection closed, and a WebSocket
   * request receives the error `HANDLER_CLOSED`. An upload whose body is still arriving loses its connection at once.
   * When `timeoutMs` passes before every request has replied, the server ends every remaining connection at that
   * moment, and the promise resolves.
   *
   * @param options - How long the server waits for requests in progress.
   * @returns A promise that resolves once the server has ended every connection.
   * @throws A `SirannonError` with code `INVALID_CLOSE_TIMEOUT` when `timeoutMs` is not a whole number from 0 to
   * 2_147_483_647.
   */
  async close(options?: ServerCloseOptions): Promise<void> {
    const deadline = new AbortController()
    const timer = setTimeout(() => deadline.abort(), resolveCloseTimeout(options?.timeoutMs))
    const runtime = this.runtime
    if (this.listenSocket && runtime) {
      runtime.uws.us_listen_socket_close(this.listenSocket)
      this.listenSocket = null
    }
    try {
      await Promise.all([this.wsHandler.close(deadline.signal), this.drain.close(deadline.signal)])
    } finally {
      clearTimeout(timer)
      runtime?.app.close()
    }
  }

  /**
   * The port that the server listens on, which the operating system chooses when you set `port` to 0, or -1 while the server is not listening.
   */
  get listeningPort(): number {
    if (!this.listenSocket || !this.runtime) return -1
    return this.runtime.uws.us_socket_local_port(this.listenSocket as unknown as us_socket)
  }

  private loadRuntime(): Promise<ServerRuntime> {
    if (!this.runtimeLoad) {
      this.runtimeLoad = loadUWebSockets().then(
        uws => {
          const app = uws.App()
          this.registerRoutes(app)
          const runtime = { uws, app }
          this.runtime = runtime
          return runtime
        },
        (err: unknown) => {
          this.runtimeLoad = null
          throw err
        },
      )
    }
    return this.runtimeLoad
  }

  private registerRoutes(app: TemplatedApp): void {
    const routes = this.drain.routesOf(app)
    if (this.cors) {
      const cors = this.cors
      routes.options('/*', (res, req) => {
        const requestOrigin = req.getHeader('origin')
        endResponse(res, () => {
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

    routes.get(
      '/capabilities',
      this.withCors(
        handleCapabilities({
          registryDigest: this.registryDigest,
          acceptSql: this.acceptSql,
          acceptDeviceSync: this.acceptDeviceSync,
        }),
      ),
    )
    routes.get('/health', this.withCors(handleLiveness()))
    routes.get('/health/ready', this.withCors(handleReadiness(this.sirannon, this.getReplicationStatus)))
    routes.get(
      '/db/:id/cluster',
      this.wrapDbGetRoute(handleClusterStatus(this.getClusterStatus, this.authorizeClusterStatus)),
    )

    if (this.acceptSql) {
      routes.post('/db/:id/query', this.wrapDbRoute(handleQuery(this.sirannon, this.resolveExecutionTarget)))
      routes.post('/db/:id/execute', this.wrapDbRoute(handleExecute(this.sirannon, this.resolveExecutionTarget)))
      routes.post(
        '/db/:id/transaction',
        this.wrapDbRoute(handleTransaction(this.sirannon, this.resolveExecutionTarget)),
      )
      routes.post('/db/:id/batch', this.wrapDbRoute(handleBatch(this.sirannon, this.resolveExecutionTarget)))
      routes.post('/db/:id/load', this.wrapDbRoute(handleLoad(this.sirannon, this.resolveExecutionTarget)))
    } else {
      for (const route of SQL_ROUTES) {
        routes.post(route, this.withCors(refuseSql))
      }
    }

    routes.post(
      '/db/:id/query/:name',
      this.wrapOperationRoute(handleOperationQuery(this.sirannon, this.operations, this.resolveExecutionTarget)),
    )
    routes.post(
      '/db/:id/execute/:name',
      this.wrapOperationRoute(handleOperationExecute(this.sirannon, this.operations, this.resolveExecutionTarget)),
    )

    this.registerBackupRoutes(routes)

    if (this.acceptDeviceSync) {
      routes.post('/db/:id/changes', this.wrapDbRoute(handleChanges(this.sirannon, this.resolveExecutionTarget)))
      routes.post('/db/:id/migrations', this.wrapDbRoute(handleMigrationList(this.sirannon)))
      routes.post('/db/:id/snapshot', this.wrapDbRoute(handleSnapshotManifest(this.sirannon)))
      routes.post('/db/:id/snapshot/page', this.wrapDbRoute(handleSnapshotPage(this.sirannon)))
    } else {
      for (const route of DEVICE_SYNC_ROUTES) {
        routes.post(route, this.withCors(refuseDeviceSync))
      }
    }

    registerWebSocketRoute({
      app,
      wsHandler: this.wsHandler,
      drain: this.drain,
      authenticateHook: this.authenticateHook,
      maxBodyBytes: this.maxBodyBytes,
      maxBackpressureBytes: this.maxWsBackpressureBytes,
    })

    routes.any('/*', res => {
      sendError(res, 404, 'NOT_FOUND', 'Route not found')
    })
  }

  private registerBackupRoutes(routes: RouteRegistrar): void {
    routes.post('/db/:id/backup', this.wrapDbRoute(handleBackupTrigger(this.sirannon)))
    routes.get('/db/:id/backup', this.wrapDbGetRoute(handleBackupStatus(this.sirannon)))
    routes.get('/db/:id/backup/chain', this.wrapDbGetRoute(handleBackupChain(this.sirannon)))
    routes.post('/db/:id/backup/verify', this.wrapDbRoute(handleBackupVerify(this.sirannon)))
    routes.post('/db/:id/backup/safe-to-delete', this.wrapDbRoute(handleBackupSafeToDelete(this.sirannon)))
    routes.post(
      '/db/:id/backup/restore',
      this.wrapDbRoute(handleBackupRestore(this.sirannon, this.acceptBackupRestore, this.restoreRuns)),
    )
    routes.get('/db/:id/backup/restore', this.wrapDbGetRoute(handleBackupRestoreStatus(this.restoreRuns)))
  }

  private withCors(
    handler: (res: HttpResponse, req: HttpRequest) => void,
  ): (res: HttpResponse, req: HttpRequest) => void {
    if (!this.cors) return handler

    const cors = this.cors
    return (res, req) => {
      writeCorsOrigin(res, cors, req.getHeader('origin'))
      handler(res, req)
    }
  }

  private routeDeps(): OperationRouteDeps<Identity> {
    return {
      cors: this.cors,
      maxBodyBytes: this.maxBodyBytes,
      authenticateHook: this.authenticateHook,
    }
  }

  private wrapDbRoute(handler: DbRouteHandler): (res: HttpResponse, req: HttpRequest) => void {
    return wrapDbRoute<Identity>(this.routeDeps(), handler)
  }

  private wrapDbGetRoute(handler: DbGetRouteHandler): (res: HttpResponse, req: HttpRequest) => void {
    return wrapDbGetRoute<Identity>(this.routeDeps(), handler)
  }

  private wrapOperationRoute(handler: OperationRouteHandler): (res: HttpResponse, req: HttpRequest) => void {
    return wrapOperationRoute<Identity>(this.routeDeps(), handler)
  }
}

/**
 * Builds a server that exposes the databases of one registry.
 *
 * @param sirannon - The registry whose databases the server exposes.
 * @param options - Address, cross-origin rules, size limits, authentication, registered operations, and whether the server accepts SQL.
 * @returns The server, which starts serving when you call {@link SirannonServer.listen}.
 *
 * @public
 */
export function createServer<Identity = unknown>(
  sirannon: Sirannon,
  options?: ServerOptions<Identity>,
): SirannonServer<Identity> {
  return new SirannonServer<Identity>(sirannon, options)
}
