const SPACE = 0x20
const TAB = 0x09
const LINE_FEED = 0x0a
const VERTICAL_TAB = 0x0b
const FORM_FEED = 0x0c
const CARRIAGE_RETURN = 0x0d
const SEMICOLON = 0x3b
const HYPHEN = 0x2d
const SLASH = 0x2f
const ASTERISK = 0x2a

const NO_STATEMENT_MESSAGE = 'The supplied SQL string contains no statements'

function startsSqliteSpace(code: number): boolean {
  return code === SPACE || code === TAB || code === LINE_FEED || code === FORM_FEED || code === CARRIAGE_RETURN
}

function continuesSqliteSpace(code: number): boolean {
  return startsSqliteSpace(code) || code === VERTICAL_TAB
}

function holdsNoStatement(sql: string): boolean {
  const firstNul = sql.indexOf('\0')
  const text = firstNul === -1 ? sql : sql.slice(0, firstNul)
  let index = 0
  while (index < text.length) {
    const code = text.charCodeAt(index)
    if (startsSqliteSpace(code)) {
      index++
      while (index < text.length && continuesSqliteSpace(text.charCodeAt(index))) index++
      continue
    }
    if (code === SEMICOLON) {
      index++
      continue
    }
    const next = text.charCodeAt(index + 1)
    if (code === HYPHEN && next === HYPHEN) {
      const lineEnd = text.indexOf('\n', index + 2)
      if (lineEnd === -1) return true
      index = lineEnd
      continue
    }
    if (code === SLASH && next === ASTERISK && index + 2 < text.length) {
      const commentEnd = text.indexOf('*/', index + 2)
      if (commentEnd === -1) return true
      index = commentEnd + 2
      continue
    }
    return false
  }
  return true
}

/**
 * Refuses statement text from which SQLite would prepare no statement, so that every driver reports such text the
 * same way.
 *
 * SQLite reads the text only up to its first NUL character, and it prepares no statement when that part holds only
 * whitespace, comments, and semicolons. The scan follows SQLite's tokenizer: a vertical tab counts as whitespace only
 * after another whitespace character, a line comment ends before its newline, and a `/*` that ends the text is a
 * division sign. The scan stops at the first other character, which always starts a statement or a syntax error that
 * SQLite reports itself, so text that holds a statement always passes. The error carries the message that
 * better-sqlite3 raises for the same text, and the query layer turns it into a `QueryError`.
 *
 * @param sql - The statement text that a caller supplied.
 * @throws RangeError when SQLite would prepare no statement from `sql`.
 * @internal
 */
export function assertHoldsStatement(sql: string): void {
  if (holdsNoStatement(sql)) throw new RangeError(NO_STATEMENT_MESSAGE)
}
