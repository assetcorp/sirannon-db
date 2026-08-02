import type { ChangeEvent } from '@delali/sirannon-db'
import type { RemoteSubscription } from '@delali/sirannon-db/client'
import { TopologyAwareClient } from '@delali/sirannon-db/client/topology'
import { clusterEndpointsFromEnv, DATABASE_ID, DEFAULT_CLUSTER_TOKEN, toWebSocketAuthProtocol } from './cluster-config'

const REPLICATED_TABLES = ['customers', 'entitlements', 'usage_events', 'billing_events', 'audit_log'] as const

const endpoints = clusterEndpointsFromEnv(import.meta.env.VITE_SIRANNON_CLUSTER_ENDPOINTS)
const token = import.meta.env.VITE_SIRANNON_CLUSTER_TOKEN ?? DEFAULT_CLUSTER_TOKEN
const authProtocol = toWebSocketAuthProtocol(token)
const authHeaders = { Authorization: `Bearer ${token}` }

const wsClient = new TopologyAwareClient({
  endpoints,
  discovery: 'coordinator',
  transport: 'websocket',
  readPreference: 'replica',
  readConcern: 'majority',
  headers: authHeaders,
  webSocketProtocols: [authProtocol],
})

const wsDb = wsClient.database(DATABASE_ID)

export interface ControlPlaneSubscriptionHandlers {
  onChange: (event: ChangeEvent) => void
  onReset: () => void
}

export async function subscribeControlPlane(handlers: ControlPlaneSubscriptionHandlers): Promise<RemoteSubscription[]> {
  const results = await Promise.allSettled(
    REPLICATED_TABLES.map(table => wsDb.on(table).subscribe(handlers.onChange, { onReset: handlers.onReset })),
  )

  const subscriptions: RemoteSubscription[] = []
  const errors: unknown[] = []

  for (const result of results) {
    if (result.status === 'fulfilled') {
      subscriptions.push(result.value)
    } else {
      errors.push(result.reason)
    }
  }

  if (errors.length === 0) {
    return subscriptions
  }

  for (const subscription of subscriptions) {
    try {
      subscription.unsubscribe()
    } catch (error) {
      errors.push(error)
    }
  }

  if (errors.length === 1) {
    throw errors[0]
  }

  throw new AggregateError(errors, 'Failed to establish control-plane subscriptions')
}
