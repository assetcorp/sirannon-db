import type { LiveQueryState } from '@delali/sirannon-db/react'
import type { ZodType } from 'zod'
import type { ActivityRecord, Product } from '../../lib/schemas'
import type { ConnectionState, ProductStats } from './types'

export interface ParsedRows<T> {
  rows: T[]
  rejected: number
}

export function parseRows<T>(state: LiveQueryState<unknown>, schema: ZodType<T>): ParsedRows<T> {
  if (state.status !== 'ready') {
    return { rows: [], rejected: 0 }
  }

  const rows: T[] = []
  let rejected = 0

  for (const row of state.rows) {
    const result = schema.safeParse(row)
    if (result.success) {
      rows.push(result.data)
    } else {
      rejected += 1
    }
  }

  return { rows, rejected }
}

export function toConnectionState(states: readonly LiveQueryState<unknown>[]): ConnectionState {
  if (states.some(state => state.status === 'error')) {
    return 'offline'
  }

  return states.every(state => state.status === 'ready') ? 'live' : 'connecting'
}

export function isRevalidating(states: readonly LiveQueryState<unknown>[]): boolean {
  return states.some(state => state.status === 'ready' && state.revalidating)
}

export function firstLiveError(states: readonly LiveQueryState<unknown>[]): string | null {
  for (const state of states) {
    if (state.status === 'error') {
      return state.error.message
    }
  }

  return null
}

export function getProductStats(products: readonly Product[]): ProductStats {
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
