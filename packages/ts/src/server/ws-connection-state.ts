import type { Database } from '../core/database.js'
import type { ServerExecutionTarget, Subscription } from '../core/types.js'
import type { DeviceChangeStream } from './ws-device-stream.js'
import type { WSOperationContext } from './ws-operations.js'
import type { InFlightRequests } from './ws-shutdown.js'

/**
 * Holds what the WebSocket handler keeps for one open connection: its database, its subscriptions and device streams,
 * its requests in progress, and the context that its statements reply through.
 *
 * @internal
 */
export interface ConnectionState {
  databaseId: string
  database: Database
  executionTarget: ServerExecutionTarget
  identity: unknown
  subscriptions: Map<string, Subscription>
  deviceStreams: Map<string, DeviceChangeStream>
  requests: InFlightRequests
  overloaded: boolean
  operationContext?: WSOperationContext
}
