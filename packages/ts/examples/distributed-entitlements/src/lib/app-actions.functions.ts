import { randomUUID } from 'node:crypto'
import type { RemoteDatabase } from '@delali/sirannon-db/client'
import { TopologyAwareClient } from '@delali/sirannon-db/client/topology'
import { createServerFn, createServerOnlyFn } from '@tanstack/react-start'
import { assertMajorityWriteAvailable, fetchClusterNodes } from './cluster-status'
import type {
  ApplyBillingEventInput,
  AuditRecord,
  BillingEvent,
  CustomerEntitlement,
  RecordUsageInput,
  UsageEvent,
} from './schemas'
import {
  applyBillingEventInputSchema,
  controlPlaneSnapshotSchema,
  createCustomerInputSchema,
  recordUsageInputSchema,
} from './schemas'
import {
  AUDIT_LOG_SQL,
  BILLING_EVENTS_SQL,
  CUSTOMER_ENTITLEMENTS_SQL,
  clusterEndpointsFromEnv,
  DATABASE_ID,
  DECREMENT_USAGE_QUOTA_SQL,
  DEFAULT_CLUSTER_TOKEN,
  DELETE_AUDIT_LOG_SQL,
  DELETE_BILLING_EVENTS_SQL,
  DELETE_CUSTOMERS_SQL,
  DELETE_ENTITLEMENTS_SQL,
  DELETE_USAGE_EVENTS_SQL,
  FINALIZE_BILLING_EVENT_SQL,
  INSERT_AUDIT_SQL,
  INSERT_BILLING_AUDIT_SQL,
  INSERT_BILLING_EVENT_SQL,
  INSERT_CUSTOMER_SQL,
  INSERT_ENTITLEMENT_SQL,
  INSERT_USAGE_AUDIT_SQL,
  INSERT_USAGE_EVENT_SQL,
  RESET_SEQUENCE_SQL,
  SEED_CUSTOMERS,
  UPDATE_CUSTOMER_FROM_BILLING_SQL,
  UPDATE_ENTITLEMENT_FROM_BILLING_SQL,
  USAGE_EVENTS_SQL,
} from './sql'
import { setProxyEnabled } from './toxiproxy'

const getServerHttpDb = createServerOnlyFn((): RemoteDatabase => {
  const token = process.env.SIRANNON_CLUSTER_TOKEN ?? DEFAULT_CLUSTER_TOKEN
  const endpoints = clusterEndpointsFromEnv(process.env.SIRANNON_CLUSTER_ENDPOINTS)
  const client = new TopologyAwareClient({
    endpoints,
    discovery: 'coordinator',
    transport: 'http',
    readPreference: 'replica',
    readConcern: 'majority',
    headers: {
      Authorization: `Bearer ${token}`,
    },
  })

  return client.database(DATABASE_ID)
})

export const getControlPlaneSnapshot = createServerFn({
  method: 'GET',
}).handler(async () => {
  const db = getServerHttpDb()
  const [customers, usage, billingEvents, auditLog, clusterNodes] = await Promise.all([
    db.query<CustomerEntitlement>(CUSTOMER_ENTITLEMENTS_SQL),
    db.query<UsageEvent>(USAGE_EVENTS_SQL),
    db.query<BillingEvent>(BILLING_EVENTS_SQL),
    db.query<AuditRecord>(AUDIT_LOG_SQL),
    fetchClusterNodes(),
  ])

  return controlPlaneSnapshotSchema.parse({ customers, usage, billingEvents, auditLog, clusterNodes })
})

export const getClusterStatus = createServerFn({
  method: 'GET',
}).handler(fetchClusterNodes)

export const createCustomer = createServerFn({
  method: 'POST',
})
  .inputValidator(data => createCustomerInputSchema.parse(data))
  .handler(async ({ data }) => {
    await assertMajorityWriteAvailable()
    const db = getServerHttpDb()
    const externalId = createExternalId(data.name)
    await db.transaction([
      {
        sql: INSERT_CUSTOMER_SQL,
        params: [externalId, data.name, data.plan, 'active'],
      },
      {
        sql: INSERT_ENTITLEMENT_SQL,
        params: [data.seats, data.apiQuota, data.supportTier, externalId],
      },
      {
        sql: INSERT_AUDIT_SQL,
        params: ['operator', 'customer_created', externalId, `Created ${data.name} with ${data.plan} entitlements`],
      },
    ])
  })

export const recordUsage = createServerFn({
  method: 'POST',
})
  .inputValidator(data => recordUsageInputSchema.parse(data))
  .handler(async ({ data }) => {
    await assertMajorityWriteAvailable()
    const db = getServerHttpDb()
    await recordUsageInternal(db, data)
  })

export const applyBillingEvent = createServerFn({
  method: 'POST',
})
  .inputValidator(data => applyBillingEventInputSchema.parse(data))
  .handler(async ({ data }) => {
    await assertMajorityWriteAvailable()
    const db = getServerHttpDb()
    await applyBillingEventInternal(db, data)
  })

export const replayDuplicateUsage = createServerFn({
  method: 'POST',
})
  .inputValidator(data => recordUsageInputSchema.parse(data))
  .handler(async ({ data }) => {
    await assertMajorityWriteAvailable()
    const db = getServerHttpDb()
    const replay = {
      ...data,
      idempotencyKey: `usage-replay-${data.customerId}`,
      source: 'billing_replay' as const,
    }
    await recordUsageInternal(db, replay)
    await recordUsageInternal(db, replay)
  })

export const resetControlPlane = createServerFn({
  method: 'POST',
}).handler(async () => {
  await assertMajorityWriteAvailable()
  const db = getServerHttpDb()
  await db.transaction([
    { sql: DELETE_BILLING_EVENTS_SQL },
    { sql: DELETE_USAGE_EVENTS_SQL },
    { sql: DELETE_AUDIT_LOG_SQL },
    { sql: DELETE_ENTITLEMENTS_SQL },
    { sql: DELETE_CUSTOMERS_SQL },
    {
      sql: RESET_SEQUENCE_SQL,
      params: ['customers', 'entitlements', 'usage_events', 'billing_events', 'audit_log'],
    },
    ...SEED_CUSTOMERS.flatMap(customer => [
      {
        sql: INSERT_CUSTOMER_SQL,
        params: [customer.externalId, customer.name, customer.plan, customer.status],
      },
      {
        sql: INSERT_ENTITLEMENT_SQL,
        params: [customer.seats, customer.apiQuota, customer.supportTier, customer.externalId],
      },
    ]),
    {
      sql: INSERT_AUDIT_SQL,
      params: ['operator', 'reset', 'control-plane', 'Reset entitlements to the seeded control-plane state'],
    },
  ])
})

export const isolateCurrentPrimary = createServerFn({
  method: 'POST',
}).handler(async () => {
  const nodes = await fetchClusterNodes()
  const primaryNodeId = nodes.find(node => node.currentPrimary !== null)?.currentPrimary
  if (!primaryNodeId) {
    throw new Error('No current primary is visible from coordinator discovery')
  }
  await setProxyEnabled(`etcd-entitlements-${primaryNodeId}`, false)
})

export const healClusterLinks = createServerFn({
  method: 'POST',
}).handler(async () => {
  const nodes = ['node-a', 'node-b', 'node-c']
  await Promise.all([
    ...nodes.map(nodeId => setProxyEnabled(`etcd-entitlements-${nodeId}`, true)),
    ...nodes.map(nodeId => setProxyEnabled(`grpc-entitlements-${nodeId}`, true)),
  ])
})

async function recordUsageInternal(db: RemoteDatabase, data: RecordUsageInput) {
  await db.transaction([
    {
      sql: INSERT_USAGE_EVENT_SQL,
      params: [data.customerId, data.units, data.source, data.idempotencyKey],
    },
    {
      sql: DECREMENT_USAGE_QUOTA_SQL,
      params: [data.units, data.customerId],
    },
    {
      sql: INSERT_USAGE_AUDIT_SQL,
      params: [
        data.source,
        'usage_recorded',
        String(data.customerId),
        `Recorded ${data.units} units for ${data.customerName}`,
      ],
    },
  ])
}

async function applyBillingEventInternal(db: RemoteDatabase, data: ApplyBillingEventInput): Promise<void> {
  const payload = JSON.stringify({
    plan: data.plan,
    seats: data.seats,
    apiQuota: data.apiQuota,
    supportTier: data.supportTier,
    active: data.active,
  })

  await db.transaction([
    {
      sql: INSERT_BILLING_EVENT_SQL,
      params: [data.providerEventId, data.eventType, data.customerExternalId, data.version, payload],
    },
    {
      sql: UPDATE_CUSTOMER_FROM_BILLING_SQL,
      params: [data.plan, data.status, data.customerExternalId, data.version, data.providerEventId],
    },
    {
      sql: UPDATE_ENTITLEMENT_FROM_BILLING_SQL,
      params: [
        data.seats,
        data.apiQuota,
        data.supportTier,
        data.active ? 1 : 0,
        data.version,
        data.customerExternalId,
        data.version,
        data.providerEventId,
      ],
    },
    {
      sql: FINALIZE_BILLING_EVENT_SQL,
      params: [data.providerEventId],
    },
    {
      sql: INSERT_BILLING_AUDIT_SQL,
      params: [
        'billing-webhook',
        'billing_event_applied',
        data.customerExternalId,
        `${data.eventType} updated ${data.customerName} to version ${data.version}`,
        data.providerEventId,
      ],
    },
  ])
}

function createExternalId(name: string): string {
  const slug = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 32)
  const safeSlug = slug.length > 0 ? slug : 'customer'
  return `cus_${safeSlug}_${randomUUID().slice(0, 8)}`
}
