/**
 * One token from the start of a statement, as the SQL guard records it: a bare word, a quoted name, a dot, or any
 * other symbol.
 *
 * @internal
 */
export interface StatementHeadToken {
  /** `word` for a bare word, `name` for a quoted name or string, `dot` for `.`, and `other` for every other symbol. */
  kind: 'word' | 'name' | 'dot' | 'other'
  /** The token's text in lower case, without its quotes. */
  text: string
}

/**
 * The number of tokens from the start of a statement that {@link namesTemporaryObject} reads, which covers
 * `CREATE VIRTUAL TABLE IF NOT EXISTS temp .` and `CREATE UNIQUE INDEX IF NOT EXISTS temp .`.
 *
 * @internal
 */
export const STATEMENT_HEAD_TOKENS = 8

const SCHEMA_OBJECT_KINDS = new Set(['table', 'view', 'index', 'trigger'])
const TEMPORARY_SCHEMA = 'temp'

/**
 * Returns whether a token is the bare word `word`.
 *
 * @param token - The token, or undefined past the end of the recorded tokens.
 * @param word - The word in lower case.
 * @returns `true` when the token is that bare word.
 * @internal
 */
export function isHeadWord(token: StatementHeadToken | undefined, word: string): boolean {
  return token?.kind === 'word' && token.text === word
}

/**
 * Returns whether the first tokens of a statement create or drop a temporary table, view, index, or trigger, through
 * `CREATE TEMP`, `CREATE TEMPORARY`, or a name in the `temp` schema, however the statement quotes that schema.
 *
 * @param head - The first tokens of the statement, at most {@link STATEMENT_HEAD_TOKENS} of them.
 * @returns `true` once the tokens show a temporary object, and `false` while they show none.
 * @internal
 */
export function namesTemporaryObject(head: readonly StatementHeadToken[]): boolean {
  const verb = head[0]?.text
  if (verb !== 'create' && verb !== 'drop') return false
  let index = 1
  if (verb === 'create') {
    if (isHeadWord(head[1], 'temp') || isHeadWord(head[1], 'temporary')) return true
    if (isHeadWord(head[index], 'unique')) index++
    if (isHeadWord(head[index], 'virtual')) index++
  }
  const objectKind = head[index]
  if (objectKind?.kind !== 'word' || !SCHEMA_OBJECT_KINDS.has(objectKind.text)) return false
  index++
  if (isHeadWord(head[index], 'if')) index += verb === 'create' ? 3 : 2
  const schema = head[index]
  return (
    (schema?.kind === 'word' || schema?.kind === 'name') &&
    schema.text === TEMPORARY_SCHEMA &&
    head[index + 1]?.kind === 'dot'
  )
}
