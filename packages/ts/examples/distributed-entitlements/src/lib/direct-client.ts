import type { RemoteSubscription } from '@delali/sirannon-db/client'
import { SirannonClient } from '@delali/sirannon-db/client'
import type { CDCEvent } from './cdc'
import { clusterEndpointsFromEnv, DATABASE_ID, DEFAULT_CLUSTER_TOKEN, toWebSocketAuthProtocol } from './sql'

const endpoints = clusterEndpointsFromEnv(import.meta.env.VITE_SIRANNON_CLUSTER_ENDPOINTS)
const token = import.meta.env.VITE_SIRANNON_CLUSTER_TOKEN ?? DEFAULT_CLUSTER_TOKEN
const authProtocol = toWebSocketAuthProtocol(token)
const authHeaders = { Authorization: `Bearer ${token}` }

const wsClient = new SirannonClient({
  endpoints,
  discovery: 'coordinator',
  transport: 'websocket',
  readPreference: 'replica',
  readConcern: 'majority',
  headers: authHeaders,
  webSocketProtocols: [authProtocol],
})

const wsDb = wsClient.database(DATABASE_ID)

export async function subscribeControlPlane(callback: (event: CDCEvent) => void): Promise<RemoteSubscription[]> {
  return Promise.all([
    wsDb.on('customers').subscribe(callback),
    wsDb.on('entitlements').subscribe(callback),
    wsDb.on('usage_events').subscribe(callback),
    wsDb.on('billing_events').subscribe(callback),
    wsDb.on('audit_log').subscribe(callback),
  ])
}
