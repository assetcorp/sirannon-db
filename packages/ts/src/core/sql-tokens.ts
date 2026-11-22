const SQLITE_SPACES = new Set([' ', '\t', '\n', '\v', '\f', '\r'])
const NAMED_PARAMETER_PREFIXES = new Set(['$', '@', ':', '#'])

/**
 * Returns whether a character can start a bare word, the way SQLite's tokenizer reads one.
 *
 * @param c - One character of the statement text.
 * @returns `true` for a letter, `_`, or any character above U+007F.
 * @internal
 */
export function isWordStart(c: string): boolean {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c === '_' || c >= '\u0080'
}

/**
 * Returns whether a character can continue a bare word, the way SQLite's tokenizer reads one.
 *
 * @param c - One character of the statement text.
 * @returns `true` for a character that {@link isWordStart} accepts, a digit, or `$`.
 * @internal
 */
export function isWordPart(c: string): boolean {
  return isWordStart(c) || (c >= '0' && c <= '9') || c === '$'
}

/**
 * Returns whether a character opens a named parameter such as `$total`, `@total`, `:total`, or `#total`.
 *
 * @param c - One character of the statement text.
 * @returns `true` for `$`, `@`, `:`, and `#`.
 * @internal
 */
export function opensNamedParameter(c: string): boolean {
  return NAMED_PARAMETER_PREFIXES.has(c)
}

/**
 * Returns the position just past the named parameter that starts at `start`, following SQLite's tokenizer. SQLite
 * reads `$name(...)` as one token that runs to the first `)` or space, so a `;`, quote, or comment marker inside those
 * parentheses belongs to the parameter.
 *
 * @param sql - The statement text.
 * @param start - The position of the parameter's `$`, `@`, `:`, or `#`.
 * @returns The position of the first character after the parameter.
 * @internal
 */
export function namedParameterEnd(sql: string, start: number): number {
  const n = sql.length
  let i = start + 1
  let nameLength = 0
  while (i < n) {
    const c = sql[i]
    if (isWordPart(c)) {
      nameLength++
      i++
      continue
    }
    if (c === '(' && nameLength > 0) {
      i++
      while (i < n && sql[i] !== ')' && !SQLITE_SPACES.has(sql[i])) i++
      return i < n && sql[i] === ')' ? i + 1 : i
    }
    if (c === ':' && sql[i + 1] === ':') {
      i += 2
      continue
    }
    break
  }
  return i
}

/**
 * Returns the position just past the quoted text that starts at `start`, where a doubled quote stands for one quote.
 *
 * @param sql - The statement text.
 * @param start - The position of the opening quote.
 * @param quote - The quote character.
 * @returns The position after the closing quote, or the end of the text when the quote never closes.
 * @internal
 */
export function skipQuoted(sql: string, start: number, quote: string): number {
  const n = sql.length
  let i = start + 1
  while (i < n) {
    if (sql[i] === quote) {
      if (sql[i + 1] === quote) {
        i += 2
        continue
      }
      return i + 1
    }
    i++
  }
  return n
}

/**
 * Returns the text between the quote at `start` and its closing quote, with each doubled quote read as one quote.
 *
 * @param sql - The statement text.
 * @param start - The position of the opening quote.
 * @param quote - The quote character.
 * @returns The unquoted text and the position after the closing quote, or the end of the text when the quote never
 * closes.
 * @internal
 */
export function readDelimited(sql: string, start: number, quote: string): { value: string; next: number } {
  const n = sql.length
  let i = start + 1
  let value = ''
  while (i < n) {
    if (sql[i] === quote) {
      if (sql[i + 1] === quote) {
        value += quote
        i += 2
        continue
      }
      return { value, next: i + 1 }
    }
    value += sql[i]
    i++
  }
  return { value, next: n }
}
