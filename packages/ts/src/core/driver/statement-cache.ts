const STATEMENT_CACHE_CAPACITY = 128

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
