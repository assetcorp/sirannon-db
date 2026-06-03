import type { CDCEvent } from '../../lib/cdc'
import { normaliseActivityRecord, normaliseProduct } from '../../lib/cdc'
import type { ActivityRecord, Product } from '../../lib/schemas'
import type { ConnectionState, ProductStats } from './types'

export function applyProductEvent(products: Product[], event: CDCEvent): Product[] {
  if (event.type === 'delete') {
    const deleted = normaliseProduct(event.oldRow ?? event.row)
    if (!deleted) {
      return products
    }

    return products.filter(product => product.id !== deleted.id)
  }

  const nextProduct = normaliseProduct(event.row)
  if (!nextProduct) {
    return products
  }

  const byId = new Map(products.map(product => [product.id, product]))
  byId.set(nextProduct.id, nextProduct)
  return [...byId.values()].sort(compareProducts)
}

export function applyActivityEvent(records: ActivityRecord[], event: CDCEvent): ActivityRecord[] {
  if (event.type === 'delete') {
    const deleted = normaliseActivityRecord(event.oldRow ?? event.row)
    if (!deleted) {
      return records
    }

    return records.filter(record => record.id !== deleted.id)
  }

  if (event.type !== 'insert') {
    return records
  }

  const nextRecord = normaliseActivityRecord(event.row)
  if (!nextRecord || records.some(record => record.id === nextRecord.id)) {
    return records
  }

  return [nextRecord, ...records].slice(0, 20)
}

export function getProductStats(products: Product[]): ProductStats {
  return products.reduce(
    (stats, product) => ({
      totalProducts: stats.totalProducts + 1,
      totalStock: stats.totalStock + product.stock,
      lowStock: stats.lowStock + (product.stock <= 10 ? 1 : 0),
    }),
    { totalProducts: 0, totalStock: 0, lowStock: 0 },
  )
}

export function formatPrice(price: number): string {
  return `$${price.toFixed(2)}`
}

export function formatTimestamp(value: string): string {
  const date = new Date(`${value}Z`)
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

export function activityLabel(record: ActivityRecord): string {
  if (record.action === 'allocated') {
    return `Allocated ${record.quantity} unit from ${record.product_name}`
  }

  if (record.action === 'received') {
    return `Received ${record.quantity} units for ${record.product_name}`
  }

  return `Created ${record.product_name} with ${record.quantity} units`
}

export function formatEventLabel(event: CDCEvent): string {
  return `${event.table} ${event.type} #${event.seq.toString()}`
}

export function statusLabel(state: ConnectionState): string {
  if (state === 'live') {
    return 'Live'
  }

  if (state === 'offline') {
    return 'Offline'
  }

  return 'Connecting'
}

export function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export function createEmptyDispose(): () => void {
  return () => undefined
}

function compareProducts(left: Product, right: Product): number {
  return left.id - right.id
}
