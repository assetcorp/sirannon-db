import type {
  BeforePushHook,
  BeforeSnapshotHook,
  BeforeSubscribeHook,
  ConnectionHookContext,
  QueryHookContext,
} from '../types.js'

/**
 * The lifecycle points at which you can register a hook.
 *
 * @internal
 */
export type HookEvent =
  | 'beforeQuery'
  | 'afterQuery'
  | 'beforeConnect'
  | 'databaseOpen'
  | 'databaseClose'
  | 'beforeSubscribe'
  | 'beforeSnapshot'
  | 'beforePush'

/**
 * The context that a subscribe hook receives.
 *
 * @internal
 */
export type SubscribeHookContext = Parameters<BeforeSubscribeHook>[0]

/**
 * The context that a snapshot hook receives.
 *
 * @internal
 */
export type SnapshotHookContext = Parameters<BeforeSnapshotHook>[0]

/**
 * The context that a push hook receives.
 *
 * @internal
 */
export type PushHookContext = Parameters<BeforePushHook>[0]

/**
 * Maps each lifecycle point to the context that its hooks receive.
 *
 * @internal
 */
export interface HookEventContextMap {
  beforeQuery: QueryHookContext
  afterQuery: QueryHookContext & { durationMs: number }
  beforeConnect: ConnectionHookContext
  databaseOpen: ConnectionHookContext
  databaseClose: ConnectionHookContext
  beforeSubscribe: SubscribeHookContext
  beforeSnapshot: SnapshotHookContext
  beforePush: PushHookContext
}

/**
 * The function that you register as a hook for one lifecycle point.
 *
 * @internal
 */
export type HookHandler<E extends HookEvent> = (ctx: HookEventContextMap[E]) => void | Promise<void>

/**
 * Removes the hook whose registration returned this function, and a second call has no effect.
 *
 * @public
 */
export type HookDispose = () => void
