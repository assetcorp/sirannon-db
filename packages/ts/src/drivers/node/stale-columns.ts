import type { DatabaseSync, StatementSync } from 'node:sqlite'

let staleColumns: boolean | undefined

/**
 * Returns whether this Node.js release's `StatementSync.all` can return rows that lack a column after a schema change,
 * which node:sqlite in Node.js 22 does on the first call after an `ALTER TABLE ... ADD COLUMN`, because it counts the
 * columns before SQLite prepares the statement again. The answer comes from one probe on an in-memory database, which
 * runs once per process.
 *
 * @param databaseClass - The `DatabaseSync` class from `node:sqlite`.
 * @returns `true` when `all` returned a row without the added column.
 * @internal
 */
export function allReadsKeepStaleColumns(databaseClass: typeof DatabaseSync): boolean {
  if (staleColumns !== undefined) return staleColumns
  const probe = new databaseClass(':memory:')
  try {
    probe.exec('CREATE TABLE widths (a INTEGER); INSERT INTO widths VALUES (1)')
    const statement = probe.prepare('SELECT * FROM widths')
    statement.all()
    probe.exec('ALTER TABLE widths ADD COLUMN b INTEGER')
    const [row] = statement.all()
    staleColumns = row === undefined || !Object.hasOwn(row, 'b')
  } finally {
    probe.close()
  }
  return staleColumns
}

/**
 * Returns every row of a statement, reading through `iterate` on a runtime where `all` can return rows without a
 * column that the schema has, so that each row holds every column and SQLite executes the statement once.
 *
 * @param statement - The prepared node:sqlite statement.
 * @param stepEachRow - `true` when {@link allReadsKeepStaleColumns} found that `all` can lose a column.
 * @param params - The values to bind.
 * @returns The rows.
 * @internal
 */
export function readAllRows(statement: StatementSync, stepEachRow: boolean, params: readonly unknown[]): unknown[] {
  const bound = params as Parameters<StatementSync['all']>
  return stepEachRow ? Array.from(statement.iterate(...bound)) : statement.all(...bound)
}
