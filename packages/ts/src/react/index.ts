import { useCallback, useMemo, useSyncExternalStore } from 'react'
import type { LiveQuery, LiveQueryOptions, LiveQueryState } from '../core/live/types.js'
import type { OperationRef } from '../core/operation-registry.js'
import { operationRef } from '../core/operation-registry.js'
import type { Params } from '../core/types.js'
import { createLiveQueryStore, getServerSnapshot } from './live-query-store.js'
import { useStableValue } from './stable-value.js'

export type { LiveQueryState } from '../core/live/types.js'

/**
 * The one method these hooks need from a database, so both a local
 * `Database` and a `RemoteDatabase` satisfy it.
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
  /** Set false to hold the query closed, which suits a query that depends on data you do not have yet. */
  enabled?: boolean
}

/**
 * Subscribes a component to a registered read and re-renders it as the rows change.
 *
 * The query closes when the component unmounts.
 *
 * @param database - Database the read runs against.
 * @param operation - Name of the registered read.
 * @param args - Arguments the read takes.
 * @param options - Whether the query runs, plus the live-query settings.
 * @returns Whether the query is pending, ready with rows, or failed.
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
 * Subscribes a component to a registered read and re-renders it as the rows change.
 *
 * @param database - Database the read runs against.
 * @param operation - Reference to the registered read, which carries its argument and row types.
 * @param args - Arguments the read takes.
 * @param options - Whether the query runs, plus the live-query settings.
 * @returns Whether the query is pending, ready with rows, or failed.
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
 * @param database - Database the write runs against.
 * @param command - Reference to the registered write, which carries its argument type.
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
 * @param database - Database the write runs against.
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
