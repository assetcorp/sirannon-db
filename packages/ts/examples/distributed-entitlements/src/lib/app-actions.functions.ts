import type { RemoteDatabase } from '@delali/sirannon-db/client'
import { TopologyAwareClient } from '@delali/sirannon-db/client/topology'
import { createServerFn, createServerOnlyFn } from '@tanstack/react-start'
import { entitlements } from '../generated/operations'
import { clusterEndpointsFromEnv, DATABASE_ID, DEFAULT_CLUSTER_TOKEN } from './cluster-config'
import { assertMajorityWriteAvailable, fetchClusterNodes } from './cluster-status'
import type { ApplyBillingEventInput, RecordUsageInput } from './schemas'
import {
  applyBillingEventInputSchema,
  controlPlaneSnapshotSchema,
  createCustomerInputSchema,
  recordUsageInputSchema,
} from './schemas'
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
    db.query(entitlements.reads.customerEntitlements, {}),
    db.query(entitlements.reads.usageEvents, {}),
    db.query(entitlements.reads.billingEvents, {}),
    db.query(entitlements.reads.auditLog, {}),
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
    await getServerHttpDb().execute(entitlements.writes.createCustomer, {
      name: data.name,
      plan: data.plan,
      seats: data.seats,
      apiQuota: data.apiQuota,
      supportTier: data.supportTier,
    })
  })

export const recordUsage = createServerFn({
  method: 'POST',
})
  .inputValidator(data => recordUsageInputSchema.parse(data))
  .handler(async ({ data }) => {
    await assertMajorityWriteAvailable()
    await recordUsageInternal(getServerHttpDb(), data)
  })

export const applyBillingEvent = createServerFn({
  method: 'POST',
})
  .inputValidator(data => applyBillingEventInputSchema.parse(data))
  .handler(async ({ data }) => {
    await assertMajorityWriteAvailable()
    await applyBillingEventInternal(getServerHttpDb(), data)
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
  await getServerHttpDb().execute(entitlements.writes.resetControlPlane, {})
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

async function recordUsageInternal(db: RemoteDatabase, data: RecordUsageInput): Promise<void> {
  await db.execute(entitlements.writes.recordUsage, {
    customerId: data.customerId,
    customerName: data.customerName,
    units: data.units,
    source: data.source,
    idempotencyKey: data.idempotencyKey,
  })
}

async function applyBillingEventInternal(db: RemoteDatabase, data: ApplyBillingEventInput): Promise<void> {
  await db.execute(entitlements.writes.applyBillingEvent, {
    providerEventId: data.providerEventId,
    eventType: data.eventType,
    customerExternalId: data.customerExternalId,
    customerName: data.customerName,
    version: data.version,
    plan: data.plan,
    status: data.status,
    seats: data.seats,
    apiQuota: data.apiQuota,
    supportTier: data.supportTier,
    active: data.active,
  })
}
