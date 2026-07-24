import type { SQLiteConnection } from '../driver/types.js'
import { INTERNAL_TABLE_PREFIX } from '../internal-tables.js'
import { referencedTables, userSchemaObjects, userTableNames } from '../system-catalog/index.js'
import { validateDdlSafety } from './validators.js'

export async function dumpSchema(
  conn: SQLiteConnection,
  excludePrefix: string = INTERNAL_TABLE_PREFIX,
): Promise<string[]> {
  const rows = await userSchemaObjects(conn, excludePrefix)

  const filtered = rows.filter(row => {
    if (row.name.startsWith(excludePrefix)) return false
    if (row.name.startsWith('sqlite_')) return false
    if (row.tbl_name.startsWith(excludePrefix)) return false
    if (row.tbl_name.startsWith('sqlite_')) return false
    return validateDdlSafety(row.sql)
  })

  const tables: Array<{ name: string; sql: string }> = []
  const indexes: Array<{ sql: string }> = []

  for (const row of filtered) {
    if (row.type === 'table') {
      tables.push({ name: row.name, sql: row.sql })
    } else {
      indexes.push({ sql: row.sql })
    }
  }

  const tableOrder = await tablesInFkOrder(conn)
  const orderMap = new Map<string, number>()
  for (let i = 0; i < tableOrder.length; i++) {
    orderMap.set(tableOrder[i], i)
  }

  tables.sort((a, b) => (orderMap.get(a.name) ?? 999) - (orderMap.get(b.name) ?? 999))

  const result: string[] = []
  for (const t of tables) {
    result.push(t.sql)
  }
  for (const idx of indexes) {
    result.push(idx.sql)
  }
  return result
}

export async function tablesInFkOrder(conn: SQLiteConnection): Promise<string[]> {
  const tableNames = await userTableNames(conn, INTERNAL_TABLE_PREFIX)

  const adjacency = new Map<string, Set<string>>()
  const inDegree = new Map<string, number>()

  for (const name of tableNames) {
    adjacency.set(name, new Set())
    inDegree.set(name, 0)
  }

  for (const name of tableNames) {
    for (const referenced of await referencedTables(conn, name)) {
      if (tableNames.includes(referenced) && referenced !== name) {
        const deps = adjacency.get(referenced)
        if (deps && !deps.has(name)) {
          deps.add(name)
          inDegree.set(name, (inDegree.get(name) ?? 0) + 1)
        }
      }
    }
  }

  const queue: string[] = []
  for (const name of tableNames) {
    if ((inDegree.get(name) ?? 0) === 0) {
      queue.push(name)
    }
  }

  const sorted: string[] = []
  while (queue.length > 0) {
    const current = queue.shift()
    if (current === undefined) break
    sorted.push(current)
    const deps = adjacency.get(current)
    if (deps) {
      for (const dep of deps) {
        const newDeg = (inDegree.get(dep) ?? 1) - 1
        inDegree.set(dep, newDeg)
        if (newDeg === 0) {
          queue.push(dep)
        }
      }
    }
  }

  if (sorted.length < tableNames.length) {
    const remaining = tableNames.filter(n => !sorted.includes(n)).sort()
    sorted.push(...remaining)
  }

  return sorted
}
