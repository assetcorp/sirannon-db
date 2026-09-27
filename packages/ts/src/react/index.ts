import { useCallback, useMemo, useSyncExternalStore } from 'react'
import type { LiveQuery, LiveQueryOptions, LiveQueryState } from '../core/live/types.js'
import type { OperationRef } from '../core/operation-registry.js'
import { operationRef } from '../core/operation-registry.js'
import type { Params } from '../core/types.js'
import { createLiveQueryStore, getServerSnapshot } from './live-query-store.js'
import { useStableValue } from './stable-value.js'

export type { LiveQueryState } from '../core/live/types.js'

/**
 * The one database method that `useLiveQuery` calls, which both a local
 * `Database` and a `RemoteDatabase` provide.
 *
 * @public
 */
export interface LiveDatabase {
  /** Opens a live query for a registered read. */
  live(
    operation: string | OperationRef<never, never>,
    args?: never,
    options?: LiveQueryOptions,
  ): Promise<LiveQuery<unknown>>
}

/**
 * Settings for one call to `useLiveQuery`.
 *
 * @public
 */
export interface UseLiveQueryOptions extends LiveQueryOptions {
  /** Set it to `false` to keep the query closed, for example while the query's arguments depend on data that the component is still loading. */
  enabled?: boolean
}

/**
 * Returns the state of a live query for a registered read, and re-renders the component each time the rows change.
 *
 * The hook closes the query when the component unmounts.
 *
 * @param database - The database on which Sirannon runs the read.
 * @param operation - The name of the registered read.
 * @param args - The arguments for the read.
 * @param options - The `enabled` flag and the live-query settings.
 * @returns The query state, which is pending, ready with rows, or an error.
 *
 * @public
 */
export function useLiveQuery<Row = Record<string, unknown>>(
  database: LiveDatabase,
  operation: string,
  args?: Params,
  options?: UseLiveQueryOptions,
): LiveQueryState<Row>
/**
 * Returns the state of a live query for a registered read, and re-renders the component each time the rows change.
 *
 * @param database - The database on which Sirannon runs the read.
 * @param operation - A reference to the registered read, with its argument and row types.
 * @param args - The arguments for the read.
 * @param options - The `enabled` flag and the live-query settings.
 * @returns The query state, which is pending, ready with rows, or an error.
 *
 * @public
 */
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

/**
 * Returns a stable callback that runs a registered write.
 *
 * @param database - The database on which Sirannon runs the write.
 * @param command - A reference to the registered write, with its argument type.
 * @returns A callback that takes the write's arguments and resolves with its result.
 *
 * @public
 */
export function useCommand<Args, Result>(
  database: { execute(operation: OperationRef<Args, unknown>, args: Args): Promise<Result> },
  command: OperationRef<Args, unknown>,
): (args: Args) => Promise<Result>
/**
 * Returns a stable callback that runs a statement or a registered write by name.
 *
 * @param database - The database on which Sirannon runs the write.
 * @param command - The statement to run, or the name of a registered write.
 * @returns A callback that takes the parameters and resolves with the result.
 *
 * @public
 */
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
