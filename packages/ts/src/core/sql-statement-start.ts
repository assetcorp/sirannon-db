/**
 * The regular expression source that matches the whitespace, line comments, and block comments before a statement's
 * first keyword, for a pattern that has to match that keyword wherever it starts.
 *
 * @internal
 */
export const LEADING_COMMENTS_PATTERN = String.raw`^(?:\s|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)*`

const LEADING_COMMENTS_RE = new RegExp(LEADING_COMMENTS_PATTERN)

const TRANSACTION_BOUND_VERB_RE = new RegExp(
  `${LEADING_COMMENTS_PATTERN}(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE|VACUUM|PRAGMA|ATTACH|DETACH)\\b`,
  'i',
)

/**
 * Returns a statement without the whitespace and comments that come before its first keyword.
 *
 * @param sql - The statement.
 * @returns The statement from its first keyword onwards.
 * @internal
 */
export function withoutLeadingComments(sql: string): string {
  return sql.replace(LEADING_COMMENTS_RE, '')
}

/**
 * Returns whether SQLite can execute a statement inside a transaction that Sirannon opens around it.
 *
 * SQLite refuses `VACUUM`, `ATTACH`, and `DETACH` inside a transaction, and it leaves `PRAGMA foreign_keys` unchanged there. A
 * statement that begins or ends a transaction or a savepoint would also end the transaction that Sirannon opened, so
 * this function counts every `PRAGMA` and every transaction control statement with them.
 *
 * @param sql - The statement, which can open with whitespace and comments.
 * @returns `false` for a transaction control statement, `VACUUM`, `PRAGMA`, `ATTACH`, and `DETACH`, and `true` for
 * every other statement.
 * @internal
 */
export function runsInsideTransaction(sql: string): boolean {
  return !TRANSACTION_BOUND_VERB_RE.test(sql)
}
