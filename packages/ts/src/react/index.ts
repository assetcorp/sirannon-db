import { useCallback, useMemo, useSyncExternalStore } from 'react'
import type { LiveQuery, LiveQueryOptions, LiveQueryState } from '../core/live/types.js'
import type { OperationRef } from '../core/operation-registry.js'
import { operationRef } from '../core/operation-registry.js'
import type { Params } from '../core/types.js'
import { createLiveQueryStore, getServerSnapshot } from './live-query-store.js'
import { useStableValue } from './stable-value.js'

export type { LiveQueryState } from '../core/live/types.js'

export interface LiveDatabase {
  live(
    operation: string | OperationRef<never, never>,
    args?: never,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<never>>
}

export interface UseLiveQueryOptions extends LiveQueryOptions {
  enabled?: boolean
}

export function useLiveQuery<Row = Record<string, unknown>>(
  database: LiveDatabase,
  operation: string,
  args?: Params,
  options?: UseLiveQueryOptions,
): LiveQueryState<Row>
export function useLiveQuery<Args, Row>(
  database: LiveDatabase,
  operation: OperationRef<Args, Row>,
  args: Args,
  options?: UseLiveQueryOptions,
): LiveQueryState<Row>
export function useLiveQuery<Row>(
  database: LiveDatabase,
  operation: string | OperationRef<unknown, Row>,
  args?: unknown,
  options?: UseLiveQueryOptions,
): LiveQueryState<Row> {
  const literal = typeof operation === 'string'
  const name = literal ? operation : operation.name
  const enabled = options?.enabled !== false
  const rereadJitterMs = options?.rereadJitterMs
  const maxTransactionChanges = options?.maxTransactionChanges
  const stableArgs = useStableValue(args)

  const store = useMemo(() => {
    if (!enabled) return createLiveQueryStore<Row>(null)
    const target = literal ? name : operationRef<never, never>(name)
    const queryOptions: LiveQueryOptions = {
      ...(rereadJitterMs === undefined ? {} : { rereadJitterMs }),
      ...(maxTransactionChanges === undefined ? {} : { maxTransactionChanges }),
    }
    return createLiveQueryStore<Row>(
      () => database.live(target, stableArgs as never, queryOptions) as Promise<LiveQuery<Row>>,
    )
  }, [database, literal, name, stableArgs, enabled, rereadJitterMs, maxTransactionChanges])

  return useSyncExternalStore(store.subscribe, store.getSnapshot, getServerSnapshot)
}

export function useCommand<Args, Result>(
  database: { execute(operation: OperationRef<Args, unknown>, args: Args): Promise<Result> },
  command: OperationRef<Args, unknown>,
): (args: Args) => Promise<Result>
export function useCommand<Result>(
  database: { execute(sql: string, params?: Params): Promise<Result> },
  command: string,
): (params?: Params) => Promise<Result>
export function useCommand<Args, Result>(
  database: { execute(operation: string | OperationRef<Args, unknown>, args?: Args): Promise<Result> },
  command: string | OperationRef<Args, unknown>,
): (args?: Args) => Promise<Result> {
  const literal = typeof command === 'string'
  const name = literal ? command : command.name
  const target = useMemo(() => (literal ? name : operationRef<Args, unknown>(name)), [literal, name])

  return useCallback((args?: Args) => database.execute(target, args), [database, target])
}
