export interface CronExpression {
  readonly hasSeconds: boolean
  matches(date: Date): boolean
}

interface FieldSpec {
  readonly name: string
  readonly min: number
  readonly max: number
  readonly names?: Readonly<Record<string, number>>
}

const MONTH_NAMES: Readonly<Record<string, number>> = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12,
}

const WEEKDAY_NAMES: Readonly<Record<string, number>> = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6,
}

const SECOND: FieldSpec = { name: 'second', min: 0, max: 59 }
const MINUTE: FieldSpec = { name: 'minute', min: 0, max: 59 }
const HOUR: FieldSpec = { name: 'hour', min: 0, max: 23 }
const DAY_OF_MONTH: FieldSpec = { name: 'day-of-month', min: 1, max: 31 }
const MONTH: FieldSpec = { name: 'month', min: 1, max: 12, names: MONTH_NAMES }
const DAY_OF_WEEK: FieldSpec = { name: 'day-of-week', min: 0, max: 7, names: WEEKDAY_NAMES }

const NICKNAMES: Readonly<Record<string, string>> = {
  '@yearly': '0 0 1 1 *',
  '@annually': '0 0 1 1 *',
  '@monthly': '0 0 1 * *',
  '@weekly': '0 0 * * 0',
  '@daily': '0 0 * * *',
  '@midnight': '0 0 * * *',
  '@hourly': '0 * * * *',
}

const UNSUPPORTED_SYNTAX = /[LW#?]/i

function resolveValue(token: string, spec: FieldSpec): number {
  let value: number
  if (/^\d+$/.test(token)) {
    value = Number(token)
  } else if (spec.names) {
    const named = spec.names[token.toLowerCase()]
    if (named === undefined) {
      throw new Error(`unknown name '${token}' in ${spec.name} field`)
    }
    value = named
  } else {
    throw new Error(`invalid value '${token}' in ${spec.name} field`)
  }

  if (value < spec.min || value > spec.max) {
    throw new Error(`value ${value} out of range ${spec.min}-${spec.max} in ${spec.name} field`)
  }
  return value
}

function parseListItem(item: string, spec: FieldSpec, out: Set<number>): void {
  const slashParts = item.split('/')
  if (slashParts.length > 2) {
    throw new Error(`invalid step syntax '${item}' in ${spec.name} field`)
  }

  const [base, stepToken] = slashParts
  let step = 1
  if (stepToken !== undefined) {
    if (!/^\d+$/.test(stepToken)) {
      throw new Error(`invalid step '${stepToken}' in ${spec.name} field`)
    }
    step = Number(stepToken)
    if (step < 1) {
      throw new Error(`step must be at least 1 in ${spec.name} field`)
    }
  }

  let start: number
  let end: number
  if (base === '*') {
    start = spec.min
    end = spec.max
  } else if (base.includes('-')) {
    const rangeParts = base.split('-')
    if (rangeParts.length !== 2) {
      throw new Error(`invalid range '${base}' in ${spec.name} field`)
    }
    start = resolveValue(rangeParts[0], spec)
    end = resolveValue(rangeParts[1], spec)
  } else {
    start = resolveValue(base, spec)
    end = stepToken !== undefined ? spec.max : start
  }

  if (start > end) {
    throw new Error(`range start ${start} is after end ${end} in ${spec.name} field`)
  }

  for (let value = start; value <= end; value += step) {
    out.add(spec === DAY_OF_WEEK ? value % 7 : value)
  }
}

function parseField(raw: string, spec: FieldSpec): Set<number> {
  if (raw === '') {
    throw new Error(`empty ${spec.name} field`)
  }
  if (UNSUPPORTED_SYNTAX.test(raw)) {
    throw new Error(`unsupported syntax '${raw}' in ${spec.name} field`)
  }

  const values = new Set<number>()
  for (const item of raw.split(',')) {
    parseListItem(item, spec, values)
  }
  return values
}

function isFullRange(values: Set<number>, spec: FieldSpec): boolean {
  const span = spec === DAY_OF_WEEK ? 7 : spec.max - spec.min + 1
  return values.size >= span
}

function expandNickname(expression: string): string {
  const expanded = NICKNAMES[expression.toLowerCase()]
  if (expanded === undefined) {
    throw new Error(`unsupported nickname '${expression}'`)
  }
  return expanded
}

class ParsedCron implements CronExpression {
  constructor(
    readonly hasSeconds: boolean,
    private readonly seconds: Set<number>,
    private readonly minutes: Set<number>,
    private readonly hours: Set<number>,
    private readonly daysOfMonth: Set<number>,
    private readonly months: Set<number>,
    private readonly daysOfWeek: Set<number>,
    private readonly domRestricted: boolean,
    private readonly dowRestricted: boolean,
  ) {}

  matches(date: Date): boolean {
    if (this.hasSeconds && !this.seconds.has(date.getSeconds())) {
      return false
    }
    if (!this.minutes.has(date.getMinutes())) {
      return false
    }
    if (!this.hours.has(date.getHours())) {
      return false
    }
    if (!this.months.has(date.getMonth() + 1)) {
      return false
    }

    const domMatch = this.daysOfMonth.has(date.getDate())
    const dowMatch = this.daysOfWeek.has(date.getDay())
    if (this.domRestricted && this.dowRestricted) {
      return domMatch || dowMatch
    }
    return domMatch && dowMatch
  }
}

export function parseCron(expression: string): CronExpression {
  const trimmed = expression.trim()
  if (trimmed === '') {
    throw new Error('cron expression is empty')
  }

  const normalized = trimmed.startsWith('@') ? expandNickname(trimmed) : trimmed
  const fields = normalized.split(/\s+/)

  let hasSeconds: boolean
  let secondRaw: string
  let minuteRaw: string
  let hourRaw: string
  let domRaw: string
  let monthRaw: string
  let dowRaw: string

  if (fields.length === 6) {
    hasSeconds = true
    ;[secondRaw, minuteRaw, hourRaw, domRaw, monthRaw, dowRaw] = fields
  } else if (fields.length === 5) {
    hasSeconds = false
    secondRaw = '0'
    ;[minuteRaw, hourRaw, domRaw, monthRaw, dowRaw] = fields
  } else {
    throw new Error(`expected 5 or 6 fields but got ${fields.length}`)
  }

  const daysOfMonth = parseField(domRaw, DAY_OF_MONTH)
  const daysOfWeek = parseField(dowRaw, DAY_OF_WEEK)

  return new ParsedCron(
    hasSeconds,
    parseField(secondRaw, SECOND),
    parseField(minuteRaw, MINUTE),
    parseField(hourRaw, HOUR),
    daysOfMonth,
    parseField(monthRaw, MONTH),
    daysOfWeek,
    !isFullRange(daysOfMonth, DAY_OF_MONTH),
    !isFullRange(daysOfWeek, DAY_OF_WEEK),
  )
}
