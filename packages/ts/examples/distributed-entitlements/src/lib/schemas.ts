import { z } from 'zod'

export const planSchema = z.enum(['free', 'growth', 'scale', 'enterprise'])
export const statusSchema = z.enum(['active', 'past_due', 'suspended'])
export const supportTierSchema = z.enum(['community', 'standard', 'priority', 'named'])
export const usageSourceSchema = z.enum(['api_gateway', 'billing_replay', 'manual_adjustment'])
export const billingEventTypeSchema = z.enum(['subscription.created', 'subscription.updated', 'invoice.payment_failed'])

export const customerEntitlementSchema = z.object({
  id: z.coerce.number().int().positive(),
  external_id: z.string(),
  name: z.string(),
  plan: planSchema,
  status: statusSchema,
  created_at: z.string(),
  seats: z.coerce.number().int().nonnegative(),
  api_quota: z.coerce.number().int().nonnegative(),
  support_tier: supportTierSchema,
  active: z.coerce.number().int().min(0).max(1),
  version: z.coerce.number().int().nonnegative(),
  updated_at: z.string(),
})

export const usageEventSchema = z.object({
  id: z.coerce.number().int().positive(),
  customer_id: z.coerce.number().int().positive(),
  customer_name: z.string(),
  units: z.coerce.number().int().positive(),
  source: usageSourceSchema,
  idempotency_key: z.string(),
  created_at: z.string(),
})

export const billingEventSchema = z.object({
  id: z.coerce.number().int().positive(),
  provider_event_id: z.string(),
  event_type: billingEventTypeSchema,
  customer_external_id: z.string(),
  version: z.coerce.number().int().nonnegative(),
  outcome: z.enum(['accepted', 'duplicate', 'stale']),
  processed_at: z.string(),
})

export const auditRecordSchema = z.object({
  id: z.coerce.number().int().positive(),
  actor: z.string(),
  action: z.string(),
  target: z.string(),
  detail: z.string(),
  created_at: z.string(),
})

export const clusterNodeSchema = z.object({
  nodeId: z.string(),
  endpoint: z.string(),
  reachable: z.boolean(),
  role: z.string().optional(),
  health: z.enum(['healthy', 'degraded', 'failing_over', 'unavailable', 'repairing', 'syncing']).optional(),
  currentPrimary: z.string().nullable(),
  primaryTerm: z.string().nullable(),
  readEndpoints: z.coerce.number().int().nonnegative(),
  error: z.string().nullable(),
})

export const controlPlaneSnapshotSchema = z.object({
  customers: z.array(customerEntitlementSchema),
  usage: z.array(usageEventSchema),
  billingEvents: z.array(billingEventSchema),
  auditLog: z.array(auditRecordSchema),
  clusterNodes: z.array(clusterNodeSchema),
})

export const createCustomerInputSchema = z.object({
  name: z.string().trim().min(2).max(80),
  plan: planSchema,
  seats: z.coerce.number().int().min(1).max(1000),
  apiQuota: z.coerce.number().int().min(1000).max(10000000),
  supportTier: supportTierSchema,
})

export const recordUsageInputSchema = z.object({
  customerId: z.coerce.number().int().positive(),
  customerName: z.string().trim().min(1).max(80),
  units: z.coerce.number().int().min(1).max(100000),
  source: usageSourceSchema,
  idempotencyKey: z.string().trim().min(8).max(120),
})

export const applyBillingEventInputSchema = z.object({
  providerEventId: z.string().trim().min(8).max(120),
  customerExternalId: z.string().trim().min(4).max(120),
  customerName: z.string().trim().min(1).max(80),
  eventType: billingEventTypeSchema,
  plan: planSchema,
  status: statusSchema,
  seats: z.coerce.number().int().min(0).max(1000),
  apiQuota: z.coerce.number().int().min(0).max(10000000),
  supportTier: supportTierSchema,
  active: z.boolean(),
  version: z.coerce.number().int().min(1).max(1000000),
})

export type Plan = z.infer<typeof planSchema>
export type SupportTier = z.infer<typeof supportTierSchema>
export type CustomerEntitlement = z.infer<typeof customerEntitlementSchema>
export type UsageEvent = z.infer<typeof usageEventSchema>
export type BillingEvent = z.infer<typeof billingEventSchema>
export type AuditRecord = z.infer<typeof auditRecordSchema>
export type ClusterNode = z.infer<typeof clusterNodeSchema>
export type ControlPlaneSnapshot = z.infer<typeof controlPlaneSnapshotSchema>
export type CreateCustomerInput = z.infer<typeof createCustomerInputSchema>
export type RecordUsageInput = z.infer<typeof recordUsageInputSchema>
export type ApplyBillingEventInput = z.infer<typeof applyBillingEventInputSchema>
