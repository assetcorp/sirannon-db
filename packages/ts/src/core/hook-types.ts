import type { Params, ReadConcern, WriteConcern } from './query-types.js'

/** Context passed to query hooks.
 * @public
 */
export interface QueryHookContext {
  /** Identifier of the database the statement runs against. */
  databaseId: string
  /** The statement about to run, or the one that just ran. */
  sql: string
  /** Parameters bound to the statement. */
  params?: Params
  /** Values a caller attached to the request for its own hooks to read. */
  metadata?: Record<string, unknown>
  /** Acknowledgements this write waits for. */
  writeConcern?: WriteConcern
  /** Currency this read requires. */
  readConcern?: ReadConcern
}

/** Hook invoked before a query is executed. Throw to deny.
 * @public
 */
export type BeforeQueryHook = (ctx: QueryHookContext) => void | Promise<void>

/** Hook invoked after a query is executed.
 * @public
 */
export type AfterQueryHook = (ctx: QueryHookContext & { durationMs: number }) => void | Promise<void>

/** Context passed to connection hooks.
 * @public
 */
export interface ConnectionHookContext {
  /** Identifier of the database being opened or closed. */
  databaseId: string
  /** File path of the SQLite database. */
  path: string
}

/** Hook invoked before a database connection is established.
 * @public
 */
export type BeforeConnectHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked when a database is opened.
 * @public
 */
export type DatabaseOpenHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked when a database is closed.
 * @public
 */
export type DatabaseCloseHook = (ctx: ConnectionHookContext) => void | Promise<void>

/** Hook invoked before a subscription is created. Throw to deny.
 * @public
 */
export type BeforeSubscribeHook = (ctx: {
  databaseId: string
  table: string
  filter?: Record<string, unknown>
}) => void | Promise<void>

/** Aggregated hook configuration.
 * @public
 */
export interface HookConfig {
  /** Runs before each statement. Throw to refuse it. */
  onBeforeQuery?: BeforeQueryHook | BeforeQueryHook[]
  /** Runs after each statement, with the time it took. */
  onAfterQuery?: AfterQueryHook | AfterQueryHook[]
  /** Runs before a database connection opens. */
  onBeforeConnect?: BeforeConnectHook | BeforeConnectHook[]
  /** Runs once a database is open. */
  onDatabaseOpen?: DatabaseOpenHook | DatabaseOpenHook[]
  /** Runs once a database is closed. */
  onDatabaseClose?: DatabaseCloseHook | DatabaseCloseHook[]
  /** Runs before a change subscription starts. Throw to refuse it. */
  onBeforeSubscribe?: BeforeSubscribeHook | BeforeSubscribeHook[]
}
