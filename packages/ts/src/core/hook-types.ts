import type { Params, ReadConcern, WriteConcern } from './query-types.js'

/** The context that Sirannon passes to each query hook.
 * @public
 */
export interface QueryHookContext {
  /** The identifier of the database that the statement executes against. */
  databaseId: string
  /** The statement that is about to execute, or the one that has finished executing. */
  sql: string
  /** The parameters bound to the statement. */
  params?: Params
  /** Values for the hooks to read. */
  metadata?: Record<string, unknown>
  /** The write concern that the caller set on the statement. */
  writeConcern?: WriteConcern
  /** The read concern that the caller set on the statement. */
  readConcern?: ReadConcern
}

/** A hook that Sirannon calls synchronously before it executes each statement; throw from it to reject the statement. Sirannon also fails the statement when the hook returns a promise.
 * @public
 */
export type BeforeQueryHook = (ctx: QueryHookContext) => void

/** The context that Sirannon passes to each after-query hook, which adds the statement's duration and outcome to the query context.
 * @public
 */
export interface AfterQueryHookContext extends QueryHookContext {
  /** The number of milliseconds from the start of the statement to its return, including any time that it waited for the writer. */
  durationMs: number
  /** The error that the statement threw, which the caller also receives, or undefined when the statement succeeded. Every statement of a failed transaction receives that transaction's error. */
  error?: unknown
}

/** A hook that Sirannon calls synchronously after each statement returns or throws. Sirannon ignores an error that the hook throws and a promise that it returns, and calls the next hook either way.
 * @public
 */
export type AfterQueryHook = (ctx: AfterQueryHookContext) => void

/** The context that Sirannon passes to each connection hook.
 * @public
 */
export interface ConnectionHookContext {
  /** The identifier of the database that Sirannon is opening or closing. */
  databaseId: string
  /** The file path of the SQLite database. */
  path: string
}

/** A hook that Sirannon calls synchronously before it opens a database; throw from it to stop the open. Sirannon also fails the open when the hook returns a promise.
 * @public
 */
export type BeforeConnectHook = (ctx: ConnectionHookContext) => void

/** A hook that Sirannon calls synchronously once a database is open. Sirannon ignores an error that the hook throws and a promise that it returns, and calls the next hook either way.
 * @public
 */
export type DatabaseOpenHook = (ctx: ConnectionHookContext) => void

/** A hook that Sirannon calls synchronously once a database is closed. Sirannon ignores an error that the hook throws and a promise that it returns, and calls the next hook either way.
 * @public
 */
export type DatabaseCloseHook = (ctx: ConnectionHookContext) => void

/** A hook that the server calls before it creates a change subscription; throw from it to reject the subscription.
 * @public
 */
export type BeforeSubscribeHook = (ctx: {
  /** The identifier of the database that the subscription is on. */
  databaseId: string
  /** The table that the subscription is on. */
  table: string
  /** The column values that a changed row must have for Sirannon to deliver the change to the subscriber. */
  filter?: Record<string, unknown>
  /** The identity that the `authenticate` hook returned for the connection, or undefined when the hook returned none. */
  identity?: unknown
  /** The identifier of the device that the subscription syncs, or undefined for a subscription without a device. */
  deviceId?: string
}) => void | Promise<void>

/** A hook that the server calls before it reads each table into a snapshot; throw from it to reject the snapshot.
 * @public
 */
export type BeforeSnapshotHook = (ctx: {
  /** The identifier of the database that the server copies into the snapshot. */
  databaseId: string
  /** The table that the server is about to read. */
  table: string
  /** The identity that the `authenticate` hook returned for the request, or undefined when the hook returned none. */
  identity?: unknown
}) => void | Promise<void>

/** A hook that the server calls before it writes a batch that a device pushed, once for each table in the batch; throw from it to reject the whole batch.
 * @public
 */
export type BeforePushHook = (ctx: {
  /** The identifier of the database that the server writes the batch to. */
  databaseId: string
  /** The table that the server writes these changes to. */
  table: string
  /** The identifier of the device that sent the batch. */
  deviceId: string
  /** The identity that the `authenticate` hook returned for the request, or undefined when the hook returned none. */
  identity?: unknown
}) => void | Promise<void>

/** The hooks that Sirannon calls for every database in a registry, keyed by event.
 * @public
 */
export interface HookConfig {
  /** Sirannon calls this before each statement; throw from it to reject the statement. */
  onBeforeQuery?: BeforeQueryHook | BeforeQueryHook[]
  /** Sirannon calls this after each statement, with the statement's duration. */
  onAfterQuery?: AfterQueryHook | AfterQueryHook[]
  /** Sirannon calls this before it opens a database. */
  onBeforeConnect?: BeforeConnectHook | BeforeConnectHook[]
  /** Sirannon calls this once a database is open. */
  onDatabaseOpen?: DatabaseOpenHook | DatabaseOpenHook[]
  /** Sirannon calls this once a database is closed. */
  onDatabaseClose?: DatabaseCloseHook | DatabaseCloseHook[]
  /** The server calls this before it creates a change subscription; throw from it to reject the subscription. */
  onBeforeSubscribe?: BeforeSubscribeHook | BeforeSubscribeHook[]
  /** The server calls this before it reads each table into a snapshot; throw from it to reject the snapshot. */
  onBeforeSnapshot?: BeforeSnapshotHook | BeforeSnapshotHook[]
  /** The server calls this before it writes a batch that a device pushed; throw from it to reject the batch. */
  onBeforePush?: BeforePushHook | BeforePushHook[]
}
