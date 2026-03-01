import type { BeforeSubscribeHook, ConnectionHookContext, QueryHookContext } from '../types.js'

export type HookEvent =
  | 'beforeQuery'
  | 'afterQuery'
  | 'beforeConnect'
  | 'databaseOpen'
  | 'databaseClose'
  | 'beforeSubscribe'

export type SubscribeHookContext = Parameters<BeforeSubscribeHook>[0]

export interface HookEventContextMap {
  beforeQuery: QueryHookContext
  afterQuery: QueryHookContext & { durationMs: number }
  beforeConnect: ConnectionHookContext
  databaseOpen: ConnectionHookContext
  databaseClose: ConnectionHookContext
  beforeSubscribe: SubscribeHookContext
}

export type HookHandler<E extends HookEvent> = (ctx: HookEventContextMap[E]) => void | Promise<void>

export type HookDispose = () => void
