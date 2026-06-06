import type { ActivityRecord, Product } from './schemas'

export interface CDCEvent {
  type: 'insert' | 'update' | 'delete'
  table: string
  row: Record<string, unknown>
  oldRow?: Record<string, unknown>
  seq: bigint
  timestamp: number
}

function toSafeInteger(value: unknown): number | null {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isSafeInteger(parsed) ? parsed : null
}

function toFiniteNumber(value: unknown): number | null {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isFinite(parsed) ? parsed : null
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0
}

function isValidTimestamp(value: unknown): value is string {
  return isNonEmptyString(value) && !Number.isNaN(Date.parse(value))
}

export function normaliseProduct(row: Record<string, unknown> | undefined): Product | null {
  const id = toSafeInteger(row?.id)
  const price = toFiniteNumber(row?.price)
  const stock = toSafeInteger(row?.stock)
  const name = row?.name

  if (id === null || id < 0 || price === null || price <= 0 || stock === null || stock < 0 || !isNonEmptyString(name)) {
    return null
  }

  return { id, name, price, stock }
}

export function normaliseActivityRecord(row: Record<string, unknown> | undefined): ActivityRecord | null {
  const id = toSafeInteger(row?.id)
  const quantity = toSafeInteger(row?.quantity)
  const productName = row?.product_name
  const action = row?.action
  const createdAt = row?.created_at

  if (
    id === null ||
    quantity === null ||
    id < 0 ||
    quantity < 0 ||
    !isNonEmptyString(productName) ||
    (action !== 'allocated' && action !== 'received' && action !== 'created') ||
    !isValidTimestamp(createdAt)
  ) {
    return null
  }

  return {
    id,
    product_name: productName,
    action,
    quantity,
    created_at: createdAt,
  }
}
