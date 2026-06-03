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

export function normaliseProduct(row: Record<string, unknown> | undefined): Product | null {
  const id = toSafeInteger(row?.id)
  const price = toFiniteNumber(row?.price)
  const stock = toSafeInteger(row?.stock)
  const name = row?.name

  if (id === null || price === null || stock === null || typeof name !== 'string') {
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
    typeof productName !== 'string' ||
    (action !== 'allocated' && action !== 'received' && action !== 'created') ||
    typeof createdAt !== 'string'
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
