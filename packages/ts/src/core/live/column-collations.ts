import { tokenizeSql } from './sql-tokens.js'

const TABLE_CONSTRAINT_STARTS = new Set(['constraint', 'primary', 'unique', 'check', 'foreign'])

export function parseColumnCollations(ddl: string): Map<string, string> {
  const collations = new Map<string, string>()
  const tokens = tokenizeSql(ddl)

  const open = tokens.findIndex(token => token.kind === 'punct' && token.value === '(' && token.depth === 0)
  if (open === -1) return collations

  let definition: typeof tokens = []
  for (let i = open + 1; i < tokens.length; i++) {
    const token = tokens[i]
    if (token.depth === 0) break
    if (token.depth === 1 && token.kind === 'punct' && token.value === ',') {
      readDefinition(definition, collations)
      definition = []
      continue
    }
    definition.push(token)
  }
  readDefinition(definition, collations)

  return collations
}

function readDefinition(definition: ReturnType<typeof tokenizeSql>, collations: Map<string, string>): void {
  const name = definition[0]
  if (name === undefined || name.kind !== 'word') return
  if (!name.quoted && TABLE_CONSTRAINT_STARTS.has(name.lower)) return

  for (let i = 1; i < definition.length - 1; i++) {
    const token = definition[i]
    if (token.kind === 'word' && !token.quoted && token.lower === 'collate') {
      collations.set(name.value, definition[i + 1].value)
      return
    }
  }
}
