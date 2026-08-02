export type SqlTokenKind = 'word' | 'string' | 'number' | 'param' | 'punct'

export interface SqlToken {
  kind: SqlTokenKind
  value: string
  lower: string
  start: number
  end: number
  depth: number
  quoted: boolean
}

const WHITESPACE = new Set([' ', '\t', '\n', '\r', '\f', '\v'])

function isWordStart(c: string): boolean {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c === '_'
}

function isWordPart(c: string): boolean {
  return isWordStart(c) || (c >= '0' && c <= '9') || c === '$'
}

function isDigit(c: string): boolean {
  return c >= '0' && c <= '9'
}

export function tokenizeSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = []
  const n = sql.length
  let i = 0
  let depth = 0

  while (i < n) {
    const c = sql[i]

    if (WHITESPACE.has(c)) {
      i++
      continue
    }

    if (c === '-' && sql[i + 1] === '-') {
      i += 2
      while (i < n && sql[i] !== '\n') i++
      continue
    }

    if (c === '/' && sql[i + 1] === '*') {
      i += 2
      while (i < n && !(sql[i] === '*' && sql[i + 1] === '/')) i++
      i = i < n ? i + 2 : n
      continue
    }

    if (c === "'") {
      const end = skipQuoted(sql, i, "'")
      tokens.push(token('string', sql.slice(i, end), i, end, depth, false))
      i = end
      continue
    }

    if (c === '"' || c === '`') {
      const delimited = readDelimited(sql, i, c)
      tokens.push(token('word', delimited.value, i, delimited.next, depth, true))
      i = delimited.next
      continue
    }

    if (c === '[') {
      const close = sql.indexOf(']', i + 1)
      const end = close === -1 ? n : close + 1
      tokens.push(token('word', sql.slice(i + 1, end - 1), i, end, depth, true))
      i = end
      continue
    }

    if (c === '(' || c === ')') {
      const at = depth
      if (c === '(') depth++
      else if (depth > 0) depth--
      tokens.push(token('punct', c, i, i + 1, c === '(' ? at : depth, false))
      i++
      continue
    }

    if (c === '?' || c === ':' || c === '@' || c === '$') {
      const param = readParam(sql, i)
      if (param !== null) {
        tokens.push(token('param', param.name, i, param.next, depth, false))
        i = param.next
        continue
      }
    }

    if (isDigit(c) || (c === '.' && isDigit(sql[i + 1] ?? ''))) {
      let j = i
      while (j < n && (isDigit(sql[j]) || sql[j] === '.' || sql[j] === 'e' || sql[j] === 'E' || sql[j] === 'x')) {
        if ((sql[j] === 'e' || sql[j] === 'E') && (sql[j + 1] === '+' || sql[j + 1] === '-')) j++
        j++
      }
      tokens.push(token('number', sql.slice(i, j), i, j, depth, false))
      i = j
      continue
    }

    if (isWordStart(c)) {
      let j = i + 1
      while (j < n && isWordPart(sql[j])) j++
      const word = sql.slice(i, j)
      if ((word === 'x' || word === 'X') && sql[j] === "'") {
        const end = skipQuoted(sql, j, "'")
        tokens.push(token('string', sql.slice(i, end), i, end, depth, false))
        i = end
        continue
      }
      tokens.push(token('word', word, i, j, depth, false))
      i = j
      continue
    }

    tokens.push(token('punct', c, i, i + 1, depth, false))
    i++
  }

  return tokens
}

function token(
  kind: SqlTokenKind,
  value: string,
  start: number,
  end: number,
  depth: number,
  quoted: boolean,
): SqlToken {
  return { kind, value, lower: value.toLowerCase(), start, end, depth, quoted }
}

function readParam(sql: string, start: number): { name: string; next: number } | null {
  const marker = sql[start]
  let i = start + 1
  const n = sql.length

  if (marker === '?') {
    while (i < n && isDigit(sql[i])) i++
    return { name: sql.slice(start, i), next: i }
  }

  if (marker === ':' && sql[start + 1] === ':') return null

  while (i < n && isWordPart(sql[i])) i++
  if (i === start + 1) return null
  return { name: sql.slice(start, i), next: i }
}

function skipQuoted(sql: string, start: number, quote: string): number {
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

function readDelimited(sql: string, start: number, quote: string): { value: string; next: number } {
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

export function parameterName(token: SqlToken): string | null {
  if (token.kind !== 'param') return null
  if (token.value.startsWith('?')) return null
  return token.value.slice(1)
}
