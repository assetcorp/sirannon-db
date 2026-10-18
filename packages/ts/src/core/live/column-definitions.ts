import { type SqlToken, tokenizeSql } from './sql-tokens.js'

const TABLE_CONSTRAINT_STARTS = new Set(['constraint', 'primary', 'unique', 'check', 'foreign'])

/**
 * Holds the parts of one column's definition that `PRAGMA table_xinfo` leaves out: its collation, and the
 * expression of a generated column.
 *
 * @internal
 */
export interface ColumnDefinition {
  collation: string | null
  generatedAs: string | null
}

/**
 * Returns the collation and the generated expression of every column that a `CREATE TABLE` statement declares,
 * keyed by column name, and leaves out the table constraints.
 *
 * @param ddl - The `CREATE TABLE` statement, as `sqlite_schema` stores it.
 * @returns One entry per declared column, or an empty map when the statement has no column list.
 * @internal
 */
export function parseColumnDefinitions(ddl: string): Map<string, ColumnDefinition> {
  const definitions = new Map<string, ColumnDefinition>()
  const tokens = tokenizeSql(ddl)

  const open = tokens.findIndex(token => token.kind === 'punct' && token.value === '(' && token.depth === 0)
  if (open === -1) return definitions

  let definition: SqlToken[] = []
  for (let i = open + 1; i < tokens.length; i++) {
    const token = tokens[i]
    if (token.depth === 0) break
    if (token.depth === 1 && token.kind === 'punct' && token.value === ',') {
      readDefinition(ddl, definition, definitions)
      definition = []
      continue
    }
    definition.push(token)
  }
  readDefinition(ddl, definition, definitions)

  return definitions
}

function readDefinition(ddl: string, definition: SqlToken[], definitions: Map<string, ColumnDefinition>): void {
  const name = definition[0]
  if (name === undefined || name.kind !== 'word') return
  if (!name.quoted && TABLE_CONSTRAINT_STARTS.has(name.lower)) return

  let collation: string | null = null
  let generatedAs: string | null = null
  for (let i = 1; i < definition.length - 1; i++) {
    const token = definition[i]
    if (token.depth !== 1 || token.kind !== 'word' || token.quoted) continue
    if (token.lower === 'collate' && collation === null) collation = definition[i + 1].value
    if (token.lower === 'as' && definition[i + 1].value === '(' && generatedAs === null) {
      generatedAs = parenthesised(ddl, definition, i + 1)
    }
  }
  definitions.set(name.value, { collation, generatedAs })
}

function parenthesised(ddl: string, definition: SqlToken[], openIndex: number): string | null {
  const open = definition[openIndex]
  for (let i = openIndex + 1; i < definition.length; i++) {
    const token = definition[i]
    if (token.kind === 'punct' && token.value === ')' && token.depth === open.depth) {
      return ddl.slice(open.end, token.start)
    }
  }
  return null
}
