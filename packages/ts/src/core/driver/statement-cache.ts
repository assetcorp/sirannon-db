const STATEMENT_CACHE_CAPACITY = 128

/**
 * Reuses compiled statements across batch calls so a bulk load of many small
 * batches never recompiles the same SQL on every call. Bounded and LRU, so a
 * workload of unbounded distinct SQL cannot grow the cache without limit.
 */
export function createStatementCache<S>(prepare: (sql: string) => S): (sql: string) => S {
  const cache = new Map<string, S>()
  return (sql: string): S => {
    const cached = cache.get(sql)
    if (cached !== undefined) {
      cache.delete(sql)
      cache.set(sql, cached)
      return cached
    }
    const prepared = prepare(sql)
    cache.set(sql, prepared)
    if (cache.size > STATEMENT_CACHE_CAPACITY) {
      const oldest = cache.keys().next().value
      if (oldest !== undefined) cache.delete(oldest)
    }
    return prepared
  }
}
