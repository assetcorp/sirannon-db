import { randomUUID } from 'node:crypto'
import type { RemoteDatabase } from '@delali/sirannon-db/client'
import { SirannonClient } from '@delali/sirannon-db/client'
import { createServerFn, createServerOnlyFn } from '@tanstack/react-start'
import type {
  ApplyBillingEventInput,
  AuditRecord,
  BillingEvent,
  ClusterNode,
  CustomerEntitlement,
  RecordUsageInput,
  UsageEvent,
} from './schemas'
import {
  applyBillingEventInputSchema,
  clusterNodeSchema,
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
  INSERT_AUDIT_SQL,
  INSERT_BILLING_AUDIT_SQL,
  INSERT_BILLING_EVENT_SQL,
  INSERT_CUSTOMER_SQL,
  INSERT_ENTITLEMENT_SQL,
  INSERT_USAGE_AUDIT_SQL,
  INSERT_USAGE_EVENT_SQL,
  RESET_SEQUENCE_SQL,
  SEED_CUSTOMERS,
  toServerBaseUrl,
  UPDATE_CUSTOMER_FROM_BILLING_SQL,
  UPDATE_ENTITLEMENT_FROM_BILLING_SQL,
  USAGE_EVENTS_SQL,
} from './sql'

interface ClusterStatusResponse {
  databaseId?: unknown
  role?: unknown
  currentPrimary?: unknown
  primaryTerm?: unknown
  readEndpoints?: unknown
  health?: unknown
}

const getServerHttpDb = createServerOnlyFn((): RemoteDatabase => {
  const token = process.env.SIRANNON_CLUSTER_TOKEN ?? DEFAULT_CLUSTER_TOKEN
  const endpoints = clusterEndpointsFromEnv(process.env.SIRANNON_CLUSTER_ENDPOINTS)
  const client = new SirannonClient({
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

export const createCustomer = createServerFn({
  method: 'POST',
})
  .inputValidator(data => createCustomerInputSchema.parse(data))
  .handler(async ({ data }) => {
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
    const db = getServerHttpDb()
    await recordUsageInternal(db, data)
  })

export const applyBillingEvent = createServerFn({
  method: 'POST',
})
  .inputValidator(data => applyBillingEventInputSchema.parse(data))
  .handler(async ({ data }) => {
    const db = getServerHttpDb()
    await applyBillingEventInternal(db, data)
  })

export const replayDuplicateUsage = createServerFn({
  method: 'POST',
})
  .inputValidator(data => recordUsageInputSchema.parse(data))
  .handler(async ({ data }) => {
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
      params: [data.plan, data.status, data.customerExternalId, data.version],
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
      ],
    },
    {
      sql: INSERT_BILLING_AUDIT_SQL,
      params: [
        'billing-webhook',
        'billing_event_applied',
        data.customerExternalId,
        `${data.eventType} updated ${data.customerName} to version ${data.version}`,
      ],
    },
  ])
}

async function fetchClusterNodes(): Promise<ClusterNode[]> {
  const token = process.env.SIRANNON_CLUSTER_TOKEN ?? DEFAULT_CLUSTER_TOKEN
  const endpoints = clusterEndpointsFromEnv(process.env.SIRANNON_CLUSTER_ENDPOINTS)
  const results = await Promise.all(endpoints.map(endpoint => fetchClusterNode(endpoint, token)))
  return results.map(result => clusterNodeSchema.parse(result))
}

async function fetchClusterNode(endpoint: string, token: string): Promise<ClusterNode> {
  const baseUrl = toServerBaseUrl(endpoint)
  const nodeId = nodeIdFromEndpoint(baseUrl)

  try {
    const response = await fetch(`${baseUrl}/db/${DATABASE_ID}/cluster`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
      signal: AbortSignal.timeout(2_000),
    })
    if (!response.ok) {
      return {
        nodeId,
        endpoint: baseUrl,
        reachable: false,
        currentPrimary: null,
        primaryTerm: null,
        readEndpoints: 0,
        error: `HTTP ${response.status}`,
      }
    }

    const data = (await response.json()) as ClusterStatusResponse
    return {
      nodeId,
      endpoint: baseUrl,
      reachable: true,
      role: typeof data.role === 'string' ? data.role : undefined,
      health: parseHealth(data.health),
      currentPrimary: parseCurrentPrimary(data.currentPrimary),
      primaryTerm: data.primaryTerm === undefined || data.primaryTerm === null ? null : String(data.primaryTerm),
      readEndpoints: Array.isArray(data.readEndpoints) ? data.readEndpoints.length : 0,
      error: null,
    }
  } catch (error) {
    return {
      nodeId,
      endpoint: baseUrl,
      reachable: false,
      currentPrimary: null,
      primaryTerm: null,
      readEndpoints: 0,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

async function setProxyEnabled(proxyName: string, enabled: boolean): Promise<void> {
  const url = process.env.TOXIPROXY_URL ?? 'http://127.0.0.1:8474'
  const response = await fetch(`${url}/proxies/${encodeURIComponent(proxyName)}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify({ enabled }),
  })

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`Toxiproxy failed to update ${proxyName}: HTTP ${response.status} ${body}`)
  }
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

function nodeIdFromEndpoint(endpoint: string): string {
  if (endpoint.includes('7301')) return 'node-a'
  if (endpoint.includes('7302')) return 'node-b'
  if (endpoint.includes('7303')) return 'node-c'
  return endpoint
}

function parseCurrentPrimary(value: unknown): string | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return null
  }
  const nodeId = (value as Record<string, unknown>).nodeId
  return typeof nodeId === 'string' ? nodeId : null
}

function parseHealth(value: unknown): ClusterNode['health'] {
  if (
    value === 'healthy' ||
    value === 'degraded' ||
    value === 'failing_over' ||
    value === 'unavailable' ||
    value === 'repairing' ||
    value === 'syncing'
  ) {
    return value
  }
  return undefined
}
