import type { HttpRequest, HttpResponse, TemplatedApp, us_listen_socket, us_socket } from 'uWebSockets.js'
import type { Sirannon } from '../core/sirannon.js'
import type {
  AuthenticateHook,
  ClusterStatusAuthorizer,
  ClusterStatusInfo,
  OperationRegistry,
  ReplicationStatusInfo,
  ServerExecutionTargetResolver,
  ServerOptions,
} from '../core/types.js'
import { BackupRestoreRuns } from './backup-restore-runs.js'
import { handleCapabilities } from './capabilities.js'
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
  resolveMaxBodyBytes,
  resolveWsBackpressure,
} from './limits.js'
import { operationRegistryDigest } from './operation-lookup.js'
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
    this.acceptBackupRestore = options?.acceptBackupRestore === true
    assertBackupRestoreAuthenticated(this.acceptBackupRestore, this.authenticateHook !== undefined)
    this.acceptDeviceSync = options?.acceptDeviceSync === true
    assertDeviceSyncAuthenticated(this.acceptDeviceSync, this.authenticateHook !== undefined)
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
      maxBackpressureBytes: this.maxWsBackpressureBytes,
      cdcRetentionMs: options?.cdcRetentionMs,
      deviceCursorRetentionMs: options?.deviceCursorRetentionMs,
      maxChangesHeldForDevice: options?.maxChangesHeldForDevice,
      maxUnacknowledgedChanges: options?.maxUnacknowledgedChanges,
      acceptSql: this.acceptSql,
      acceptDeviceSync: this.acceptDeviceSync,
      operations: options?.operations,
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
   * Stops listening for new connections and closes every open WebSocket connection.
   */
  async close(): Promise<void> {
    try {
      await this.wsHandler.close()
    } finally {
      if (this.listenSocket && this.runtime) {
        this.runtime.uws.us_listen_socket_close(this.listenSocket)
        this.listenSocket = null
      }
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
    if (this.cors) {
      const cors = this.cors
      app.options('/*', (res, req) => {
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

    app.get(
      '/capabilities',
      this.withCors(
        handleCapabilities({
          registryDigest: this.registryDigest,
          acceptSql: this.acceptSql,
          acceptDeviceSync: this.acceptDeviceSync,
        }),
      ),
    )
    app.get('/health', this.withCors(handleLiveness()))
    app.get('/health/ready', this.withCors(handleReadiness(this.sirannon, this.getReplicationStatus)))
    app.get(
      '/db/:id/cluster',
      this.wrapDbGetRoute(handleClusterStatus(this.getClusterStatus, this.authorizeClusterStatus)),
    )

    if (this.acceptSql) {
      app.post('/db/:id/query', this.wrapDbRoute(handleQuery(this.sirannon, this.resolveExecutionTarget)))
      app.post('/db/:id/execute', this.wrapDbRoute(handleExecute(this.sirannon, this.resolveExecutionTarget)))
      app.post('/db/:id/transaction', this.wrapDbRoute(handleTransaction(this.sirannon, this.resolveExecutionTarget)))
      app.post('/db/:id/batch', this.wrapDbRoute(handleBatch(this.sirannon, this.resolveExecutionTarget)))
      app.post('/db/:id/load', this.wrapDbRoute(handleLoad(this.sirannon, this.resolveExecutionTarget)))
    } else {
      for (const route of SQL_ROUTES) {
        app.post(route, this.withCors(refuseSql))
      }
    }

    app.post(
      '/db/:id/query/:name',
      this.wrapOperationRoute(handleOperationQuery(this.sirannon, this.operations, this.resolveExecutionTarget)),
    )
    app.post(
      '/db/:id/execute/:name',
      this.wrapOperationRoute(handleOperationExecute(this.sirannon, this.operations, this.resolveExecutionTarget)),
    )

    this.registerBackupRoutes(app)

    if (this.acceptDeviceSync) {
      app.post('/db/:id/changes', this.wrapDbRoute(handleChanges(this.sirannon, this.resolveExecutionTarget)))
      app.post('/db/:id/migrations', this.wrapDbRoute(handleMigrationList(this.sirannon)))
      app.post('/db/:id/snapshot', this.wrapDbRoute(handleSnapshotManifest(this.sirannon)))
      app.post('/db/:id/snapshot/page', this.wrapDbRoute(handleSnapshotPage(this.sirannon)))
    } else {
      for (const route of DEVICE_SYNC_ROUTES) {
        app.post(route, this.withCors(refuseDeviceSync))
      }
    }

    registerWebSocketRoute({
      app,
      wsHandler: this.wsHandler,
      authenticateHook: this.authenticateHook,
      maxBodyBytes: this.maxBodyBytes,
      maxBackpressureBytes: this.maxWsBackpressureBytes,
    })

    app.any('/*', res => {
      sendError(res, 404, 'NOT_FOUND', 'Route not found')
    })
  }

  private registerBackupRoutes(app: TemplatedApp): void {
    app.post('/db/:id/backup', this.wrapDbRoute(handleBackupTrigger(this.sirannon)))
    app.get('/db/:id/backup', this.wrapDbGetRoute(handleBackupStatus(this.sirannon)))
    app.get('/db/:id/backup/chain', this.wrapDbGetRoute(handleBackupChain(this.sirannon)))
    app.post('/db/:id/backup/verify', this.wrapDbRoute(handleBackupVerify(this.sirannon)))
    app.post('/db/:id/backup/safe-to-delete', this.wrapDbRoute(handleBackupSafeToDelete(this.sirannon)))
    app.post(
      '/db/:id/backup/restore',
      this.wrapDbRoute(handleBackupRestore(this.sirannon, this.acceptBackupRestore, this.restoreRuns)),
    )
    app.get('/db/:id/backup/restore', this.wrapDbGetRoute(handleBackupRestoreStatus(this.restoreRuns)))
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
