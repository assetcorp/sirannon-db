import type { BeforeSubscribeHook, ConnectionHookContext, QueryHookContext } from '../types.js'

/**
 * Names of the lifecycle points a hook can attach to.
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

/**
 * Context a subscribe hook receives.
 *
 * @internal
 */
export type SubscribeHookContext = Parameters<BeforeSubscribeHook>[0]

/**
 * Maps each lifecycle point to the context its hooks receive.
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
}

/**
 * Function a hook registration stores for one lifecycle point.
 *
 * @internal
 */
export type HookHandler<E extends HookEvent> = (ctx: HookEventContextMap[E]) => void | Promise<void>

/**
 * Removes a registered hook when called.
 *
 * @internal
 */
export type HookDispose = () => void
