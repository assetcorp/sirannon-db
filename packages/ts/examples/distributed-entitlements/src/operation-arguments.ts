import { RequestDeniedError } from '@delali/sirannon-db'

export const PLANS = ['free', 'growth', 'scale', 'enterprise'] as const
export const CUSTOMER_STATUSES = ['active', 'past_due', 'suspended'] as const
export const SUPPORT_TIERS = ['community', 'standard', 'priority', 'named'] as const

export const MAX_TEXT_LENGTH = 120
export const MAX_SEATS = 100_000
export const MAX_API_QUOTA = 100_000_000
export const MAX_USAGE_UNITS = 1_000_000
export const MAX_VERSION = 1_000_000

function refuse(message: string): never {
  throw new RequestDeniedError(400, 'INVALID_ARGUMENT', message)
}

export function readText(value: unknown, field: string): string {
  const text = typeof value === 'string' ? value.trim() : ''
  if (text.length === 0 || text.length > MAX_TEXT_LENGTH) {
    refuse(`${field} must be between 1 and ${MAX_TEXT_LENGTH} characters`)
  }
  return text
}

export function readCount(value: unknown, field: string, max: number, min = 0): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    refuse(`${field} must be an integer between ${min} and ${max}`)
  }
  return value
}

export function readChoice<T extends string>(value: unknown, field: string, allowed: readonly T[]): T {
  if (typeof value !== 'string' || !allowed.includes(value as T)) {
    refuse(`${field} must be one of ${allowed.join(', ')}`)
  }
  return value as T
}

export function readFlag(value: unknown, field: string): 0 | 1 {
  if (typeof value !== 'boolean') {
    refuse(`${field} must be a boolean`)
  }
  return value ? 1 : 0
}

export function readActor(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0) {
    refuse('actor is filled from the authenticated identity and was missing')
  }
  return value
}

export function toExternalId(name: string, suffix: string): string {
  const slug = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 32)
  return `cus_${slug.length > 0 ? slug : 'customer'}_${suffix}`
}
