import type { CDCEvent } from '../../lib/cdc'
import type { ClusterNode, ControlPlaneSnapshot, CustomerEntitlement } from '../../lib/schemas'
import type { ControlPlaneStats } from './types'

export function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message
  }
  return String(error)
}

export function getControlPlaneStats(snapshot: ControlPlaneSnapshot): ControlPlaneStats {
  let activeCustomers = 0
  let totalSeats = 0
  let totalQuota = 0

  for (const customer of snapshot.customers) {
    if (customer.active === 1 && customer.status === 'active') {
      activeCustomers += 1
    }
    totalSeats += customer.seats
    totalQuota += customer.api_quota
  }

  const primaryNode = snapshot.clusterNodes.find(node => node.currentPrimary !== null)?.currentPrimary ?? 'unknown'
  const unavailableNodes = snapshot.clusterNodes.filter(node => !node.reachable || node.health === 'unavailable').length

  return {
    activeCustomers,
    totalSeats,
    totalQuota,
    primaryNode,
    unavailableNodes,
  }
}

export function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat('en-US', {
    notation: value >= 10000 ? 'compact' : 'standard',
    maximumFractionDigits: 1,
  }).format(value)
}

export function formatDateTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

export function formatEventLabel(event: CDCEvent): string {
  return `${event.table.replace(/_/g, ' ')} ${event.type} synced`
}

export function selectedCustomerOrFirst(
  customers: CustomerEntitlement[],
  selectedCustomerId: number | null,
): CustomerEntitlement | null {
  if (customers.length === 0) {
    return null
  }
  const selected = customers.find(customer => customer.id === selectedCustomerId)
  return selected ?? customers[0] ?? null
}

export function clusterHealthLabel(node: ClusterNode): string {
  if (!node.reachable) {
    return 'offline'
  }
  return node.health ?? 'unknown'
}

export function nextBillingVersion(customer: CustomerEntitlement | null): number {
  return customer ? customer.version + 1 : 1
}

export function createIdempotencyKey(prefix: string, customerId: number): string {
  const random = Math.random().toString(36).slice(2, 10)
  return `${prefix}-${customerId}-${Date.now().toString(36)}-${random}`
}
