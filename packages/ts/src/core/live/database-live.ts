import { CDCError } from '../errors.js'
import type { SubscriptionBuilder } from '../types.js'
import type { LiveQuery } from './live-query.js'
import { createLiveQuery } from './live-query.js'

export interface LiveQueryOptions {
  tables?: readonly string[]
}

export interface DatabaseLiveDeps<T> {
  sql: string
  watched: ReadonlySet<string>
  requested?: readonly string[]
  read: () => Promise<T[]>
  on: (table: string) => SubscriptionBuilder
}

export function createDatabaseLiveQuery<T>(deps: DatabaseLiveDeps<T>): LiveQuery<T> {
  const tables = deps.requested ?? [...deps.watched]
  if (tables.length === 0) {
    throw new CDCError(
      `Cannot open a live query for '${deps.sql}': watch the tables it reads, or name them in the options`,
    )
  }

  const unwatched = tables.filter(table => !deps.watched.has(table))
  if (unwatched.length > 0) {
    throw new CDCError(
      `Cannot open a live query for '${deps.sql}': ${unwatched.join(', ')} carries no change triggers, so watch it first`,
    )
  }

  return createLiveQuery<T>({
    tables,
    read: deps.read,
    watch: (table, onChange) => deps.on(table).subscribe(() => onChange()),
  })
}
