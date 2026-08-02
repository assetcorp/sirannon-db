import type { OperationRef } from '@delali/sirannon-db'

export const registryDigest = "f46bb7001bbeec718973253ed0903f6794eda7838ee17e200211ab37544530d0"

export interface EntitlementsAuditLogRow {
  id: unknown
  actor: unknown
  action: unknown
  target: unknown
  detail: unknown
  created_at: unknown
}

export interface EntitlementsBillingEventsRow {
  id: unknown
  provider_event_id: unknown
  event_type: unknown
  customer_external_id: unknown
  version: unknown
  outcome: unknown
  processed_at: unknown
}

export interface EntitlementsCustomerEntitlementsRow {
  id: unknown
  external_id: unknown
  name: unknown
  plan: unknown
  status: unknown
  created_at: unknown
  seats: unknown
  api_quota: unknown
  support_tier: unknown
  active: unknown
  version: unknown
  updated_at: unknown
}

export interface EntitlementsUsageEventsRow {
  id: unknown
  customer_id: unknown
  customer_name: unknown
  units: unknown
  source: unknown
  idempotency_key: unknown
  created_at: unknown
}

export const entitlements = {
  reads: {
    auditLog: { name: "auditLog" } as OperationRef<Record<string, never>, EntitlementsAuditLogRow>,
    billingEvents: { name: "billingEvents" } as OperationRef<Record<string, never>, EntitlementsBillingEventsRow>,
    customerEntitlements: { name: "customerEntitlements" } as OperationRef<Record<string, never>, EntitlementsCustomerEntitlementsRow>,
    usageEvents: { name: "usageEvents" } as OperationRef<Record<string, never>, EntitlementsUsageEventsRow>,
  },
  writes: {
    applyBillingEvent: { name: "applyBillingEvent" } as OperationRef<{ providerEventId: unknown; eventType: unknown; customerExternalId: unknown; customerName: unknown; version: unknown; plan: unknown; status: unknown; seats: unknown; apiQuota: unknown; supportTier: unknown; active: unknown }, never>,
    createCustomer: { name: "createCustomer" } as OperationRef<{ name: unknown; plan: unknown; seats: unknown; apiQuota: unknown; supportTier: unknown }, never>,
    recordUsage: { name: "recordUsage" } as OperationRef<{ customerId: unknown; customerName: unknown; units: unknown; source: unknown; idempotencyKey: unknown }, never>,
    resetControlPlane: { name: "resetControlPlane" } as OperationRef<Record<string, never>, never>,
  },
}
