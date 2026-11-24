const TERM = '\u0000term'
const PRAGMA_FUNCTION_PREFIX = 'pragma_'

const WORDS_BEFORE_A_VALUE = new Set([
  'select',
  'distinct',
  'all',
  'where',
  'having',
  'and',
  'or',
  'not',
  'is',
  'like',
  'glob',
  'match',
  'regexp',
  'escape',
  'between',
  'case',
  'when',
  'then',
  'else',
  'limit',
  'offset',
  'default',
  'returning',
  'values',
  'check',
  'rows',
  'range',
  'groups',
])

const SYMBOLS_BEFORE_A_VALUE = new Set(['=', '<', '>', '!', '+', '-', '*', '/', '%', '|', '&', '~'])

const WORDS_THAT_END_A_FROM_CLAUSE = new Set([
  'where',
  'group',
  'having',
  'order',
  'limit',
  'union',
  'except',
  'intersect',
  'returning',
])

const WORDS_THAT_OPEN_A_STATEMENT_BODY = new Set(['select', 'values', 'begin'])

const WORDS_THAT_OPEN_A_SUBQUERY = new Set(['select', 'values'])

/**
 * Follows the tokens of one statement so that the SQL guard can tell where SQLite reads a single-quoted string as a
 * name. SQLite reads a string as a table, schema, alias, or other name wherever its grammar expects a name and no
 * value can stand, such as after `FROM`, `JOIN`, `INTO`, `UPDATE`, `TABLE`, `IN`, or a `.`, and after the `,` that
 * separates two tables of a `FROM` clause. A `(` that opens where a table can stand in a `FROM` clause starts a
 * parenthesised list of tables, at any depth, so a string after that `(` or after a `,` inside it is a name too, until
 * a `SELECT` or `VALUES` turns the parentheses into a subquery. A string stays a value wherever an expression
 * can start, which is after an operator, any other `(`, and the keywords that introduce an expression.
 *
 * @internal
 */
export class StringNamePositions {
  private last = ''
  private beforeLast = ''
  private third = ''
  private readonly fromClauseDepths = new Set<number>()
  private readonly tableListDepths = new Set<number>()
  private readonly pragmaFunctionDepths = new Set<number>()
  private bodyOpened = false

  /**
   * Returns whether SQLite reads a single-quoted string at the current position as a name.
   *
   * @param depth - The parenthesis depth of the string.
   * @param verb - The verb of the statement that holds the string, in lower case.
   * @returns `true` where SQLite reads the string as a name, and `false` where it reads a value.
   */
  readsAsName(depth: number, verb: string): boolean {
    if (verb === 'pragma') return true
    const last = this.last
    if (last === '') return false
    if (last === '.') return true
    if (last === ',') return this.fromClauseDepths.has(depth) || this.pragmaFunctionDepths.has(depth)
    if (last === '(') return this.pragmaFunctionDepths.has(depth) || this.tableListDepths.has(depth)
    if (last === 'from') return !(this.beforeLast === 'distinct' && (this.third === 'is' || this.third === 'not'))
    if (last === 'on') return verb === 'create' && !this.bodyOpened
    if (last === 'by') return this.beforeLast === 'indexed'
    return !WORDS_BEFORE_A_VALUE.has(last) && !SYMBOLS_BEFORE_A_VALUE.has(last)
  }

  /**
   * Records a bare word.
   *
   * @param lowerWord - The word in lower case.
   * @param depth - The parenthesis depth of the word.
   */
  word(lowerWord: string, depth: number): void {
    if (lowerWord === 'from' && !this.followsIsDistinct()) {
      this.fromClauseDepths.add(depth)
    } else if (WORDS_THAT_END_A_FROM_CLAUSE.has(lowerWord)) {
      this.fromClauseDepths.delete(depth)
      this.tableListDepths.delete(depth)
    } else if (WORDS_THAT_OPEN_A_SUBQUERY.has(lowerWord) && this.tableListDepths.has(depth)) {
      this.tableListDepths.delete(depth)
      this.fromClauseDepths.delete(depth)
    }
    if (WORDS_THAT_OPEN_A_STATEMENT_BODY.has(lowerWord)) this.bodyOpened = true
    this.push(lowerWord)
  }

  /**
   * Records a symbol that is neither a quote nor the start of a word.
   *
   * @param symbol - The character.
   * @param depth - The parenthesis depth before the symbol.
   */
  symbol(symbol: string, depth: number): void {
    if (symbol === '(') {
      if (this.last.startsWith(PRAGMA_FUNCTION_PREFIX)) {
        this.pragmaFunctionDepths.add(depth + 1)
      } else if (this.opensTableList(depth)) {
        this.tableListDepths.add(depth + 1)
        this.fromClauseDepths.add(depth + 1)
      }
    } else if (symbol === ')') {
      this.fromClauseDepths.delete(depth)
      this.tableListDepths.delete(depth)
      this.pragmaFunctionDepths.delete(depth)
    }
    this.push(symbol)
  }

  private opensTableList(depth: number): boolean {
    const last = this.last
    if (last === '(') return this.tableListDepths.has(depth)
    if (last === 'from' && this.beforeLast === 'distinct' && (this.third === 'is' || this.third === 'not')) return false
    if (last === 'from' || last === 'join' || last === ',') return this.fromClauseDepths.has(depth)
    return false
  }

  /** Records a string, a quoted name, or a parameter, each of which ends a term. */
  term(): void {
    this.push(TERM)
  }

  /** Forgets every token, at the `;` that ends a statement. */
  reset(): void {
    this.last = ''
    this.beforeLast = ''
    this.third = ''
    this.fromClauseDepths.clear()
    this.tableListDepths.clear()
    this.pragmaFunctionDepths.clear()
    this.bodyOpened = false
  }

  private followsIsDistinct(): boolean {
    return this.last === 'distinct' && (this.beforeLast === 'is' || this.beforeLast === 'not')
  }

  private push(token: string): void {
    this.third = this.beforeLast
    this.beforeLast = this.last
    this.last = token
  }
}
