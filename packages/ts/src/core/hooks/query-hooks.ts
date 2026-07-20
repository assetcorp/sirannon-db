import type { Params, QueryHookContext, QueryOptions } from '../types.js'
import type { HookRegistry } from './registry.js'

export function fireBeforeQueryHooks(
  parentHooks: HookRegistry | null,
  localHooks: HookRegistry,
  databaseId: string,
  sql: string,
  params?: Params,
  options?: QueryOptions,
): void {
  const hasParent = parentHooks?.has('beforeQuery')
  const hasLocal = localHooks.has('beforeQuery')
  if (!hasParent && !hasLocal) return

  const ctx: QueryHookContext = {
    databaseId,
    sql,
    params,
    writeConcern: options?.writeConcern,
    readConcern: options?.readConcern,
  }
  parentHooks?.invokeSync('beforeQuery', ctx)
  localHooks.invokeSync('beforeQuery', ctx)
}

export function fireAfterQueryHooks(
  parentHooks: HookRegistry | null,
  localHooks: HookRegistry,
  databaseId: string,
  sql: string,
  params: Params | undefined,
  durationMs: number,
): void {
  const hasParent = parentHooks?.has('afterQuery')
  const hasLocal = localHooks.has('afterQuery')
  if (!hasParent && !hasLocal) return

  const ctx = { databaseId, sql, params, durationMs }
  try {
    parentHooks?.invokeSync('afterQuery', ctx)
  } catch {
    /* non-fatal */
  }
  try {
    localHooks.invokeSync('afterQuery', ctx)
  } catch {
    /* non-fatal */
  }
}
